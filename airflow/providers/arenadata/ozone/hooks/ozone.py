# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import json
import shlex
import subprocess
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.arenadata.ozone.utils.cli_runner import CliRunner
from airflow.providers.arenadata.ozone.utils.errors import (
    OzoneCliError,
)
from airflow.providers.arenadata.ozone.utils.helpers import EnvSecretHelper
from airflow.providers.arenadata.ozone.utils.security import (
    KerberosConfig,
    SSLConfig,
)
from airflow.utils.log.secrets_masker import redact

RETRY_ATTEMPTS = 3
FAST_TIMEOUT_SECONDS = 5 * 60
SLOW_TIMEOUT_SECONDS = 60 * 60


@dataclass(frozen=True)
class OzoneConnSnapshot:
    """Lightweight connection snapshot to reduce repeated connection reads."""

    host: str
    port: int
    extra: dict[str, object]


class OzoneCliHook(BaseHook):
    """Base hook for Ozone CLI commands with retry and auth handling."""

    conn_name_attr = "ozone_conn_id"
    default_conn_name = "ozone_default"
    conn_type = "ozone"
    hook_name = "Ozone"

    def __init__(
        self,
        ozone_conn_id: str = default_conn_name,
        *,
        retry_attempts: int = RETRY_ATTEMPTS,
    ):
        super().__init__()
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self._kerberos_ticket_ready = False

    @cached_property
    def connection(self) -> object:
        """Resolve Airflow connection lazily."""
        return self.get_connection(self.ozone_conn_id)

    @cached_property
    def connection_snapshot(self) -> OzoneConnSnapshot:
        """Validate and cache required host/port/extra connection fields."""
        conn = self.connection
        extra = EnvSecretHelper.get_connection_extra(conn)
        raw_host = getattr(conn, "host", None)
        raw_port = getattr(conn, "port", None)
        if not raw_host or not str(raw_host).strip():
            raise AirflowException(
                f"Connection '{self.ozone_conn_id}' must define host for Ozone CLI operations."
            )
        if raw_port in (None, ""):
            raise AirflowException(
                f"Connection '{self.ozone_conn_id}' must define port for Ozone CLI operations."
            )
        host = str(raw_host).strip()
        port = int(raw_port)
        return OzoneConnSnapshot(host=host, port=port, extra=extra)

    @cached_property
    def _cached_ssl_env(self) -> dict[str, str] | None:
        """SSL/TLS configuration from connection Extra (lazy)."""
        try:
            return SSLConfig.load_from_connection(
                self.connection,
                conn_id=self.ozone_conn_id,
                logger=self.log,
                enabled_flag_keys=("ozone_security_enabled", "ozone.security.enabled"),
            )
        except AirflowException as err:
            self.log.debug("Could not load SSL configuration (connection may not exist): %s", str(err))
            return None

    @cached_property
    def _cached_kerberos_env(self) -> dict[str, str] | None:
        """Kerberos configuration from connection Extra (lazy)."""
        return KerberosConfig.load_ozone_env(
            extra=self.connection_snapshot.extra,
            conn_id=self.ozone_conn_id,
            logger=self.log,
        )

    @cached_property
    def _cached_client_conf_dir(self) -> str | None:
        """Generate minimal Ozone client config dir from connection extra when needed."""
        return KerberosConfig.build_client_conf_dir(
            host=self.connection_snapshot.host,
            port=self.connection_snapshot.port,
            extra=self.connection_snapshot.extra,
            conn_id=self.ozone_conn_id,
            logger=self.log,
        )

    @cached_property
    def _cached_effective_config_dir(self) -> str | None:
        """Config dir used for CLI --config and subprocess environment."""
        return KerberosConfig.effective_config_dir(self._cached_kerberos_env, self._cached_client_conf_dir)

    def _prepared_cli_env(self) -> dict[str, str]:
        """Build subprocess environment for Ozone CLI calls."""
        return KerberosConfig.build_ozone_cli_env(
            host=self.connection_snapshot.host,
            port=self.connection_snapshot.port,
            conn_id=self.ozone_conn_id,
            logger=self.log,
            effective_config_dir=self._cached_effective_config_dir,
            ssl_env=self._cached_ssl_env,
            kerberos_env=self._cached_kerberos_env,
        )

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, object]:
        """Describe Ozone connection extras in Airflow UI."""
        return {
            "hidden_fields": ["schema"],
            "relabeling": {"host": "Ozone OM Host", "port": "Ozone OM Port"},
            "placeholders": {
                "host": "ozone-om",
                "port": "9862",
                "extra": (
                    '{"ozone_security_enabled": "true", "ozone_scm_address": "ozone-scm:9860", '
                    '"hadoop_security_authentication": "kerberos", "kerberos_principal": "user@REALM", '
                    '"kerberos_keytab": "secret://kv/ozone/keytab"}'
                ),
            },
        }

    def test_connection(self) -> tuple[bool, str]:
        """Run a minimal CLI command to verify Ozone auth and connectivity."""
        try:
            result = self.run_cli(
                ["ozone", "sh", "volume", "list", "/"],
                timeout=FAST_TIMEOUT_SECONDS,
                retry_attempts=0,
                check=False,
                log_output=False,
                return_result=True,
            )
        except OzoneCliError as err:
            return False, f"Ozone CLI connection test failed: {err}"
        if result.returncode == 0:
            return True, "Ozone CLI connection test succeeded."
        error_text = CliRunner.pick_process_output(result) or "Unknown CLI error"
        return False, f"Ozone CLI connection test failed: {error_text}"

    def _prepare_cli_command(self, cmd: list[str]) -> list[str]:
        """Add ``--config`` for Kerberos-enabled commands when available."""
        if not KerberosConfig.is_enabled(self._cached_kerberos_env):
            return cmd

        if "--config" in cmd:
            return cmd

        config_dir = self._cached_effective_config_dir
        if not config_dir:
            self.log.warning(
                "Kerberos enabled but OZONE_CONF_DIR/HADOOP_CONF_DIR not set. "
                "Ozone CLI may not find security configuration files."
            )
            return cmd

        config_files_exist = KerberosConfig.check_config_files_exist(config_dir, logger=self.log)
        if not config_files_exist:
            self.log.warning(
                "Kerberos enabled but configuration files (core-site.xml, ozone-site.xml) "
                "not found in %s. Adding --config flag anyway as it's critical for Kerberos "
                "authentication. Ozone CLI will look for configs in this directory.",
                config_dir,
            )
        else:
            self.log.debug(
                "Configuration files found in %s, adding --config flag for Kerberos authentication",
                config_dir,
            )

        if cmd[:1] == ["ozone"]:
            new_cmd = [cmd[0], "--config", config_dir] + cmd[1:]
            self.log.debug(
                "Added --config flag to Ozone CLI command for Kerberos authentication: %s",
                redact(shlex.join(new_cmd)),
            )
            return new_cmd

        return cmd

    def run_cli(
        self,
        cmd: list[str],
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
        retry_attempts: int | None = None,
        input_text: str | None = None,
        check: bool = True,
        log_output: bool = True,
        return_result: bool = False,
    ) -> str | subprocess.CompletedProcess[str]:
        """Execute Ozone CLI command with error handling and automatic retries on retryable failures."""
        self.log.info("Executing Ozone CLI command (connection: %s)", self.ozone_conn_id)
        self._kerberos_ticket_ready = KerberosConfig.ensure_ticket(
            extra=self.connection_snapshot.extra,
            conn_id=self.ozone_conn_id,
            logger=self.log,
            kerberos_ticket_ready=self._kerberos_ticket_ready,
        )
        prepared_cmd = self._prepare_cli_command(cmd)
        effective_retry_attempts = self.retry_attempts if retry_attempts is None else retry_attempts
        result = CliRunner.run_ozone(
            prepared_cmd,
            env_overrides=self._prepared_cli_env(),
            timeout=timeout,
            input_text=input_text,
            check=check,
            log_output=log_output,
            retry_attempts=effective_retry_attempts,
        )
        if return_result:
            return result
        return CliRunner.pick_process_output(result)


class OzoneFsHook(OzoneCliHook):
    """
    Interact with Ozone via CLI (o3fs:// or ofs:// schemes).

    Uses 'ozone fs' or 'hadoop fs' commands.
    """

    hook_name = "Ozone FS"

    def mkdir(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> None:
        """Create a directory tree in Ozone FS."""
        self.log.debug("Creating directory in Ozone FS: %s", path)
        self.run_cli(["ozone", "fs", "-mkdir", "-p", path], timeout=timeout)

    def copy_from_local(
        self,
        local_path: str,
        remote_path: str,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Upload a local file to an Ozone URI."""
        self.log.debug(
            "Uploading file from local filesystem to Ozone (local: %s -> remote: %s)",
            local_path,
            remote_path,
        )
        local_file = Path(local_path)
        if not local_file.exists():
            self.log.error("Local file does not exist: %s", local_path)
            raise AirflowException(f"Local file does not exist: {local_path}")

        file_size = local_file.stat().st_size
        self.log.debug("Local file exists, size: %d bytes", file_size)
        self.run_cli(
            ["ozone", "fs", "-put", "-f", str(local_file), remote_path],
            timeout=timeout,
        )

    def exists(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> bool:
        """Check path existence with retryable-error handling."""
        self.log.debug("Checking existence of path in Ozone FS: %s", path)
        try:
            self.run_cli(
                ["ozone", "fs", "-test", "-e", path],
                timeout=timeout,
                check=True,
                log_output=True,
            )
            return True
        except OzoneCliError as err:
            error_text = (err.stderr or "").lower()
            if any(marker in error_text for marker in ("not found", "does not exist", "no such file")):
                return False
            raise

    def list_paths(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> list[str]:
        """List keys/paths under the provided Ozone URI."""
        self.log.debug("Listing paths in Ozone FS: %s", path)
        output = self.run_cli(["ozone", "fs", "-ls", "-C", path], timeout=timeout)

        paths = output.splitlines() if output else []
        self.log.info("Found %d path(s) in %s", len(paths), path)
        if paths:
            self.log.debug("Paths found: %s", paths[:10])
            if len(paths) > 10:
                self.log.debug("... and %d more paths", len(paths) - 10)
        return paths

    def set_key_property(
        self,
        path: str,
        replication_factor: int | None = None,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> None:
        """Set replication factor for a key."""
        if replication_factor:
            self.log.debug("Setting replication factor to %d for path: %s", replication_factor, path)
            self.run_cli(
                ["ozone", "fs", "-setrep", str(replication_factor), path],
                timeout=timeout,
            )
        else:
            self.log.debug("No key properties were specified to be set for path: %s", path)


class OzoneResource(str, Enum):
    """Supported Ozone admin resource types."""

    VOLUME = "volume"
    BUCKET = "bucket"


class OzoneAdminHook(OzoneCliHook):
    """Interact with Ozone Admin CLI (ozone sh)."""

    default_conn_name = "ozone_admin_default"
    hook_name = "Ozone Admin"

    def _delete_resource(
        self,
        *,
        resource_type: str,
        resource_path: str,
        cmd_base: list[str],
        not_found_keywords: list[str],
        recursive: bool = False,
        force: bool = False,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Delete volume/bucket with idempotent not-found handling."""
        self.log.info(
            "Deleting Ozone %s: %s (recursive=%s, force=%s)",
            resource_type,
            resource_path,
            recursive,
            force,
        )

        cmd = cmd_base.copy()
        if recursive:
            cmd.append("-r")
        cmd.append(resource_path)
        input_text = "yes\n" if (recursive and force) else None

        try:
            self.run_cli(
                cmd,
                timeout=timeout,
                input_text=input_text,
                check=True,
                log_output=True,
                return_result=True,
            )

            self.log.info("Successfully deleted %s: %s", resource_type, resource_path)
            return
        except OzoneCliError as err:
            stderr = (err.stderr or "").strip()
            err_l = stderr.lower()
            if any(k in stderr for k in not_found_keywords) or "does not exist" in err_l:
                self.log.info(
                    "%s %s does not exist, treating as success.", resource_type.capitalize(), resource_path
                )
                return
            masked_error = redact(stderr or "Unknown error")
            self.log.error("Failed to delete %s %s: %s", resource_type, resource_path, masked_error)
            raise AirflowException(
                f"Failed to delete {resource_type} {resource_path}: {masked_error}"
            ) from err

    def _create_resource(
        self,
        *,
        resource_type: str,
        resource_path: str,
        quota: str | None,
        already_exists_marker: str,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Create volume/bucket and treat ALREADY_EXISTS as success."""
        cmd = ["ozone", "sh", resource_type, "create", resource_path]
        if quota:
            cmd.extend(["--quota", quota])

        try:
            self.run_cli(
                cmd,
                timeout=timeout,
                check=True,
                log_output=True,
                return_result=True,
            )

            self.log.info(
                "Successfully created %s: %s (quota: %s)",
                resource_type,
                resource_path,
                quota or "none",
            )
            return
        except OzoneCliError as err:
            error_message = (err.stderr or "").strip() or "No error message provided"
            if already_exists_marker in error_message:
                self.log.info(
                    "%s %s already exists, treating as success.", resource_type.capitalize(), resource_path
                )
                return
            raise AirflowException(
                f"Ozone command failed (return code: {err.returncode}): {redact(error_message)}"
            )

    def create_volume(
        self,
        volume_name: str,
        quota: str | None = None,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Create volume and treat ALREADY_EXISTS as success."""
        self._create_resource(
            resource_type=OzoneResource.VOLUME.value,
            resource_path=f"/{volume_name}",
            quota=quota,
            already_exists_marker="VOLUME_ALREADY_EXISTS",
            timeout=timeout,
        )

    def create_bucket(
        self,
        volume_name: str,
        bucket_name: str,
        quota: str | None = None,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Create bucket and treat ALREADY_EXISTS as success."""
        self._create_resource(
            resource_type=OzoneResource.BUCKET.value,
            resource_path=f"/{volume_name}/{bucket_name}",
            quota=quota,
            already_exists_marker="BUCKET_ALREADY_EXISTS",
            timeout=timeout,
        )

    def delete_volume(
        self,
        volume_name: str,
        recursive: bool = False,
        force: bool = False,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Delete a volume, optionally with recursive confirmation."""
        self._delete_resource(
            resource_type=OzoneResource.VOLUME.value,
            resource_path=f"/{volume_name}",
            cmd_base=["ozone", "sh", "volume", "delete"],
            not_found_keywords=["VOLUME_NOT_FOUND"],
            recursive=recursive,
            force=force,
            timeout=timeout,
        )

    def delete_bucket(
        self,
        volume_name: str,
        bucket_name: str,
        recursive: bool = False,
        force: bool = False,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Delete a bucket, optionally with recursive confirmation."""
        self._delete_resource(
            resource_type=OzoneResource.BUCKET.value,
            resource_path=f"/{volume_name}/{bucket_name}",
            cmd_base=["ozone", "sh", "bucket", "delete"],
            not_found_keywords=["BUCKET_NOT_FOUND"],
            recursive=recursive,
            force=force,
            timeout=timeout,
        )

    def get_container_report(self, *, timeout: int = SLOW_TIMEOUT_SECONDS) -> dict:
        """Fetch and parse the JSON container report from SCM."""
        self.log.info("Fetching container report from Ozone SCM")
        output = self.run_cli(
            ["ozone", "admin", "container", "report", "--json"],
            timeout=timeout,
        )
        try:
            report_data = json.loads(output)
            if not isinstance(report_data, (dict, list)):
                raise ValueError(
                    f"Container report is not a valid JSON object or array, got type: {type(report_data)}"
                )
            return report_data
        except json.JSONDecodeError as e:
            masked_output = redact(output)
            self.log.debug("Raw output: %s", masked_output)
            raise AirflowException(f"Failed to parse container report JSON: {str(e)}") from e
        except ValueError as e:
            masked_output = redact(output)
            self.log.debug("Raw output: %s", masked_output)
            raise AirflowException(f"Invalid container report format: {str(e)}") from e
