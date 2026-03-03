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
import logging
import shlex
import subprocess
import time
from enum import Enum
from pathlib import Path

from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.arenadata.ozone.utils.common import run_subprocess
from airflow.providers.arenadata.ozone.utils.ozone_env import OzoneEnv
from airflow.utils.log.secrets_masker import redact

log = logging.getLogger(__name__)
DEFAULT_CLI_TIMEOUT_SECONDS = 300


class OzoneCliError(AirflowException):
    """Non-retryable Ozone CLI error."""

    def __init__(
        self,
        message: str,
        *,
        command: list[str] | None = None,
        stderr: str | None = None,
        returncode: int | None = None,
    ) -> None:
        super().__init__(message)
        self.command = command
        self.stderr = stderr
        self.returncode = returncode


class OzoneCliTransientError(OzoneCliError):
    """Retryable/transient Ozone CLI error."""


def is_transient_cli_failure(return_code: int | None, stderr: str) -> bool:
    """
    Classify transient Ozone CLI failures by stderr markers.

    We only retry errors that are likely to resolve on their own (network hiccups, timeouts, temporary
    unavailability). Most semantic failures (NOT_FOUND, ALREADY_EXISTS, permission errors) should not
    be retried here.
    """
    s = (stderr or "").lower()

    non_transient_markers = (
        "already_exists",
        "not_found",
        "does not exist",
        "accessdenied",
        "access denied",
        "permission denied",
        "invalid",
        "illegalargumentexception",
        "authentication failed",
        "unauthorized",
    )
    if any(m in s for m in non_transient_markers):
        return False

    transient_markers = (
        "timed out",
        "timeout",
        "connection refused",
        "connection reset",
        "connection closed",
        "no route to host",
        "temporarily unavailable",
        "service unavailable",
        "unknownhostexception",
        "could not resolve",
        "sockettimeoutexception",
        "connectexception",
        "eofexception",
        "broken pipe",
        "too many requests",
        "throttl",
        "503",
    )
    if any(m in s for m in transient_markers):
        return True

    return False


class OzoneCliHook(BaseHook):
    """
    Base hook for Apache Ozone interactions using CLI wrappers.

    Encapsulates the logic for running `ozone` commands, handling errors,
    and automatically retrying on transient failures.

    Supports SSL/TLS configuration via connection Extra:
    - ozone_security_enabled: Enable SSL/TLS for Ozone CLI
    - ozone_om_https_port: HTTPS port for Ozone Manager
    - ozone.scm.https.port: HTTPS port for SCM
    - ozone_ssl_keystore_location: Path to keystore file
    - ozone_ssl_keystore_password: Keystore password
    - ozone_ssl_truststore_location: Path to truststore file
    - ozone_ssl_truststore_password: Truststore password

    Supports Kerberos authentication via connection Extra:
    - hadoop_security_authentication: Set to "kerberos" to enable
    - kerberos_principal: Kerberos principal (e.g., "user@REALM.COM")
    - kerberos_keytab: Path to keytab file (supports secret:// paths)
    - kerberos_realm: Kerberos realm (e.g., "EXAMPLE.COM")
    - krb5_conf: Optional path to krb5.conf file
    """

    conn_name_attr = "ozone_conn_id"
    default_conn_name = "ozone_default"
    conn_type = "ozone"
    hook_name = "Ozone"

    def __init__(self, ozone_conn_id: str = default_conn_name):
        super().__init__()
        self.ozone_conn_id = ozone_conn_id

        self._env = OzoneEnv(
            conn_id=self.ozone_conn_id,
            get_connection=self.get_connection,
            logger=self.log,
        )

        self.log.debug("OzoneCliHook initialized (conn_id=%s)", self.ozone_conn_id)

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
            result = self.run_cli_check(["ozone", "sh", "volume", "list", "/"], timeout=30)
        except Exception as err:
            return False, f"Ozone CLI connection test failed: {err}"
        if result.returncode == 0:
            return True, "Ozone CLI connection test succeeded."
        error_text = (result.stderr or result.stdout or "Unknown CLI error").strip()
        return False, f"Ozone CLI connection test failed: {error_text}"

    def _check_config_files_exist(self, config_dir: str) -> bool:
        """
        Check if required Ozone configuration files exist in the directory.

        Returns True if both core-site.xml and ozone-site.xml exist, False otherwise.
        """
        if not config_dir:
            self.log.debug("Config directory is None or empty")
            return False

        config_path = Path(config_dir)
        if not config_path.is_dir():
            self.log.debug("Config directory does not exist: %s", config_dir)
            return False

        core_site = config_path / "core-site.xml"
        ozone_site = config_path / "ozone-site.xml"

        core_exists = core_site.is_file()
        ozone_exists = ozone_site.is_file()

        if not core_exists:
            self.log.debug("core-site.xml not found at: %s", core_site)
        if not ozone_exists:
            self.log.debug("ozone-site.xml not found at: %s", ozone_site)

        if core_exists and ozone_exists:
            self.log.debug("Both configuration files found in %s", config_dir)

        return core_exists and ozone_exists

    def _prepare_cli_command(self, cmd: list[str]) -> list[str]:
        """
        Prepare Ozone CLI command with --config flag if Kerberos is enabled.

        Using the --config flag explicitly tells Ozone Shell where to find security
        configuration files, which is critical for Kerberos authentication to work properly.
        """
        if not self._env.kerberos_enabled():
            return cmd

        if "--config" in cmd:
            return cmd

        config_dir = self._env.effective_config_dir
        if not config_dir:
            self.log.warning(
                "Kerberos enabled but OZONE_CONF_DIR/HADOOP_CONF_DIR not set. "
                "Ozone CLI may not find security configuration files."
            )
            return cmd

        config_files_exist = self._check_config_files_exist(config_dir)
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

    def _run_command(
        self,
        prepared_cmd: list[str],
        env: dict[str, str],
        timeout: int = DEFAULT_CLI_TIMEOUT_SECONDS,
        check: bool = True,
        input_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """
        Run Ozone CLI command locally.

        Returns subprocess.CompletedProcess.
        """
        try:
            result = run_subprocess(
                prepared_cmd,
                env=env,
                check=check,
                timeout=timeout,
                input_text=input_text,
            )
            return result
        except FileNotFoundError as e:
            raise OzoneCliError(
                "Ozone CLI not found. Please ensure Ozone is installed and available in PATH."
            ) from e

    def _log_cli_output(self, stdout: str, stderr: str, *, stdout_msg: str, stderr_msg: str) -> None:
        """Log stdout/stderr when present using provided message templates."""
        if stdout.strip():
            self.log.debug(stdout_msg, redact(stdout.strip()))
        if stderr.strip():
            self.log.debug(stderr_msg, redact(stderr.strip()))

    def run_cli_check(
        self,
        cmd: list[str],
        timeout: int = DEFAULT_CLI_TIMEOUT_SECONDS,
        input_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """
        Execute Ozone CLI command without retries.

        Use this method for probes such as existence checks where retry backoff is not needed.
        """
        prepared_cmd = self._prepare_cli_command(cmd)
        env = self._env.build_env()
        return self._run_command(
            prepared_cmd,
            env,
            timeout=timeout,
            check=False,
            input_text=input_text,
        )

    @retry(
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        before_sleep=before_sleep_log(log, logging.WARNING),
        retry=retry_if_exception_type(OzoneCliTransientError),
        reraise=True,
    )
    def run_cli(self, cmd: list[str]) -> str:
        """Execute Ozone CLI command with error handling and automatic retries on transient failures."""
        command_str = shlex.join(cmd)
        masked_command = redact(command_str)

        self.log.info("Executing Ozone CLI command (connection: %s): %s", self.ozone_conn_id, masked_command)
        masked_cmd = [redact(str(arg)) for arg in cmd]
        self.log.debug("Full command arguments: %s", masked_cmd)

        start_time = time.monotonic()
        try:
            prepared_cmd = self._prepare_cli_command(cmd)
            env = self._env.build_env()

            result = self._run_command(
                prepared_cmd,
                env,
                timeout=DEFAULT_CLI_TIMEOUT_SECONDS,
                check=True,
            )
            execution_time = time.monotonic() - start_time

            self.log.info("Ozone CLI command completed successfully in %.2f seconds", execution_time)
            self._log_cli_output(
                result.stdout or "",
                result.stderr or "",
                stdout_msg="Command stdout: %s",
                stderr_msg="Command stderr: %s",
            )

            return (result.stdout or "").strip()
        except subprocess.CalledProcessError as e:
            execution_time = time.monotonic() - start_time
            error_message = e.stderr.strip() if e.stderr else "No error message provided"
            return_code = e.returncode

            masked_error = redact(error_message)
            masked_failed_command = redact(command_str)

            self.log.error(
                "Ozone CLI command failed after %.2f seconds (return code: %d, connection: %s)",
                execution_time,
                return_code,
                self.ozone_conn_id,
            )
            self.log.error("Failed command: %s", masked_failed_command)
            self.log.error("Error output: %s", masked_error)

            if e.stdout:
                self.log.debug("Command stdout before failure: %s", redact(e.stdout.strip()))

            if is_transient_cli_failure(return_code, error_message):
                raise OzoneCliTransientError(
                    f"Ozone command failed (transient, return code: {return_code}): {masked_error}",
                    command=prepared_cmd,
                    stderr=error_message,
                    returncode=return_code,
                )

            raise OzoneCliError(
                f"Ozone command failed (return code: {return_code}): {masked_error}",
                command=prepared_cmd,
                stderr=error_message,
                returncode=return_code,
            )


class OzoneFsHook(OzoneCliHook):
    """
    Interact with Ozone via CLI (o3fs:// or ofs:// schemes).

    Uses 'ozone fs' or 'hadoop fs' commands.
    """

    hook_name = "Ozone FS"

    def mkdir(self, path: str) -> None:
        """Create a directory tree in Ozone FS."""
        self.log.info("Creating directory in Ozone FS: %s", path)
        self.run_cli(["ozone", "fs", "-mkdir", "-p", path])
        self.log.info("Successfully created directory: %s", path)

    def copy_from_local(self, local_path: str, remote_path: str) -> None:
        """Upload a local file to an Ozone URI."""
        self.log.info(
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
        self.run_cli(["ozone", "fs", "-put", "-f", str(local_file), remote_path])
        self.log.info("Successfully uploaded file to Ozone: %s", remote_path)

    def exists(self, path: str) -> bool:
        """Check path existence with transient-error handling."""
        self.log.debug("Checking existence of path in Ozone FS: %s", path)
        cmd = ["ozone", "fs", "-test", "-e", path]
        result = self.run_cli_check(cmd, timeout=30)
        if result.returncode == 0:
            self.log.debug("Path exists in Ozone: %s", path)
            return True

        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        combined = stderr or stdout
        if is_transient_cli_failure(result.returncode, combined):
            raise OzoneCliTransientError(f"Transient error while checking path existence: {path}")

        self.log.debug("Path does not exist in Ozone: %s", path)
        return False

    def list_paths(self, path: str) -> list[str]:
        """List keys/paths under the provided Ozone URI."""
        self.log.info("Listing paths in Ozone FS: %s", path)
        output = self.run_cli(["ozone", "fs", "-ls", "-C", path])

        paths = output.splitlines() if output else []
        self.log.info("Found %d path(s) in %s", len(paths), path)
        if paths:
            self.log.debug("Paths found: %s", paths[:10])
            if len(paths) > 10:
                self.log.debug("... and %d more paths", len(paths) - 10)
        return paths

    def set_key_property(self, path: str, replication_factor: int | None = None) -> None:
        """
        Set replication factor for a key.

        This helper is used by data lifecycle and administrative workflows.
        """
        if replication_factor:
            self.log.info("Setting replication factor to %d for path: %s", replication_factor, path)
            self.run_cli(["ozone", "fs", "-setrep", str(replication_factor), path])
            self.log.info("Successfully set replication factor to %d for path: %s", replication_factor, path)
        else:
            self.log.warning("No key properties were specified to be set for path: %s", path)


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
            start = time.monotonic()
            result = self.run_cli_check(
                cmd,
                timeout=DEFAULT_CLI_TIMEOUT_SECONDS,
                input_text=input_text,
            )
            elapsed = time.monotonic() - start

            if result.returncode == 0:
                self.log.info(
                    "Successfully deleted %s: %s in %.2f seconds", resource_type, resource_path, elapsed
                )
                self._log_cli_output(
                    result.stdout or "",
                    result.stderr or "",
                    stdout_msg="Command output: %s",
                    stderr_msg="Command stderr: %s",
                )
                return

            err = (result.stderr or "").strip()
            err_l = err.lower()
            if any(k in err for k in not_found_keywords) or "does not exist" in err_l:
                self.log.info(
                    "%s %s does not exist, treating as success.", resource_type.capitalize(), resource_path
                )
                return

            masked_error = redact(err or "Unknown error")
            self.log.error("Failed to delete %s %s: %s", resource_type, resource_path, masked_error)
            raise AirflowException(f"Failed to delete {resource_type} {resource_path}: {masked_error}")
        except subprocess.TimeoutExpired as err:
            self.log.error(
                "Timeout while deleting %s %s after %d seconds",
                resource_type,
                resource_path,
                DEFAULT_CLI_TIMEOUT_SECONDS,
            )
            raise AirflowException(
                f"Timeout while deleting {resource_type} {resource_path}. The operation took longer than 5 minutes."
            ) from err

    def _create_resource(
        self,
        *,
        resource_type: str,
        resource_path: str,
        quota: str | None,
        already_exists_marker: str,
    ) -> None:
        """Create volume/bucket and treat ALREADY_EXISTS as success."""
        cmd = ["ozone", "sh", resource_type, "create", resource_path]
        if quota:
            cmd.extend(["--quota", quota])

        try:
            start = time.monotonic()
            result = self.run_cli_check(cmd, timeout=DEFAULT_CLI_TIMEOUT_SECONDS)
            elapsed = time.monotonic() - start

            if result.returncode == 0:
                self.log.info(
                    "Successfully created %s: %s (quota: %s) in %.2f seconds",
                    resource_type,
                    resource_path,
                    quota or "none",
                    elapsed,
                )
                self._log_cli_output(
                    result.stdout or "",
                    result.stderr or "",
                    stdout_msg="Command output: %s",
                    stderr_msg="Command stderr: %s",
                )
                return

            err = (result.stderr or "").strip() or "No error message provided"
            if already_exists_marker in err:
                self.log.info(
                    "%s %s already exists, treating as success.", resource_type.capitalize(), resource_path
                )
                return
            raise AirflowException(f"Ozone command failed (return code: {result.returncode}): {redact(err)}")
        except subprocess.TimeoutExpired as err:
            self.log.error(
                "Timeout while creating %s %s after %d seconds",
                resource_type,
                resource_path,
                DEFAULT_CLI_TIMEOUT_SECONDS,
            )
            raise AirflowException(
                f"Timeout while creating {resource_type} {resource_path}. "
                "The operation took longer than 5 minutes."
            ) from err

    def create_volume(self, volume_name: str, quota: str | None = None) -> None:
        """Create volume and treat ALREADY_EXISTS as success."""
        self._create_resource(
            resource_type=OzoneResource.VOLUME.value,
            resource_path=f"/{volume_name}",
            quota=quota,
            already_exists_marker="VOLUME_ALREADY_EXISTS",
        )

    def create_bucket(self, volume_name: str, bucket_name: str, quota: str | None = None) -> None:
        """Create bucket and treat ALREADY_EXISTS as success."""
        self._create_resource(
            resource_type=OzoneResource.BUCKET.value,
            resource_path=f"/{volume_name}/{bucket_name}",
            quota=quota,
            already_exists_marker="BUCKET_ALREADY_EXISTS",
        )

    def delete_volume(self, volume_name: str, recursive: bool = False, force: bool = False) -> None:
        """Delete a volume, optionally with recursive confirmation."""
        self._delete_resource(
            resource_type=OzoneResource.VOLUME.value,
            resource_path=f"/{volume_name}",
            cmd_base=["ozone", "sh", "volume", "delete"],
            not_found_keywords=["VOLUME_NOT_FOUND"],
            recursive=recursive,
            force=force,
        )

    def delete_bucket(
        self,
        volume_name: str,
        bucket_name: str,
        recursive: bool = False,
        force: bool = False,
    ) -> None:
        """Delete a bucket, optionally with recursive confirmation."""
        self._delete_resource(
            resource_type=OzoneResource.BUCKET.value,
            resource_path=f"/{volume_name}/{bucket_name}",
            cmd_base=["ozone", "sh", "bucket", "delete"],
            not_found_keywords=["BUCKET_NOT_FOUND"],
            recursive=recursive,
            force=force,
        )

    def get_container_report(self) -> dict:
        """Fetch and parse the JSON container report from SCM."""
        self.log.info("Fetching container report from Ozone SCM")
        output = self.run_cli(["ozone", "admin", "container", "report", "--json"])
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
