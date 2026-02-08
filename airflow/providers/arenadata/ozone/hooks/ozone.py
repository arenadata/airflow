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

import logging
import os
import shutil
import subprocess
import tempfile
import time
import xml.sax.saxutils as saxutils

from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.arenadata.ozone.utils.ozone_utils import (
    apply_kerberos_env_vars,
    apply_ssl_env_vars,
    get_kerberos_env_vars,
    get_ssl_env_vars,
)
from airflow.utils.log.secrets_masker import redact

# Get the logger for tenacity to use
log = logging.getLogger(__name__)


class OzoneCliError(AirflowException):
    """Non-retryable Ozone CLI error."""


class OzoneCliTransientError(OzoneCliError):
    """Retryable/transient Ozone CLI error."""


def _is_transient_cli_failure(return_code: int | None, stderr: str) -> bool:
    """
    Best-effort classification of transient Ozone CLI failures.

    We only retry errors that are likely to resolve on their own (network hiccups, timeouts, temporary
    unavailability). Most semantic failures (NOT_FOUND, ALREADY_EXISTS, permission errors) should not
    be retried here.
    """
    s = (stderr or "").lower()

    # Explicitly non-transient / semantic errors
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

    # Conservative default: do not retry unknown failures here.
    return False


class OzoneHook(BaseHook):
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
        if not ozone_conn_id or not str(ozone_conn_id).strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        self.ozone_conn_id = str(ozone_conn_id).strip()
        self._ssl_env: dict[str, str] | None = None
        self._kerberos_env: dict[str, str] | None = None
        self._ssl_loaded = False
        self._kerberos_loaded = False
        self._client_conf_dir: str | None = None

        # Avoid noisy logs during DAG parsing; log at DEBUG if needed.
        self.log.debug("OzoneHook initialized (conn_id=%s)", self.ozone_conn_id)

    def _load_ssl_config(self):
        """Load SSL/TLS configuration from connection Extra."""
        if self._ssl_loaded:
            return
        try:
            conn = self.get_connection(self.ozone_conn_id)
            extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}

            ssl_env_vars = get_ssl_env_vars(extra, conn_id=self.ozone_conn_id)
            if ssl_env_vars:
                self._ssl_env = apply_ssl_env_vars(ssl_env_vars)
                self.log.debug("SSL/TLS configuration loaded from connection: %s", list(ssl_env_vars.keys()))
                if (
                    extra.get("ozone_security_enabled") == "true"
                    or extra.get("ozone.security.enabled") == "true"
                ):
                    self.log.debug("SSL/TLS enabled for Ozone Native CLI")
            else:
                self.log.debug("No SSL/TLS configuration found in connection Extra")
        except Exception as e:
            # Connection might not exist yet, that's OK
            self.log.debug("Could not load SSL configuration (connection may not exist): %s", str(e))
        finally:
            self._ssl_loaded = True

    def _load_kerberos_config(self):
        """Load Kerberos configuration from connection Extra."""
        if self._kerberos_loaded:
            return
        try:
            conn = self.get_connection(self.ozone_conn_id)
            extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}

            kerberos_env_vars = get_kerberos_env_vars(extra, conn_id=self.ozone_conn_id)
            if kerberos_env_vars:
                self._kerberos_env = apply_kerberos_env_vars(kerberos_env_vars)
                self.log.debug(
                    "Kerberos configuration loaded from connection: %s", list(kerberos_env_vars.keys())
                )
                # Only log Kerberos as enabled if explicitly configured (not just SSL)
                if extra.get("hadoop_security_authentication") == "kerberos":
                    self.log.debug("Kerberos authentication enabled for Ozone Native CLI")
            else:
                self.log.debug("No Kerberos configuration found in connection Extra")
        except Exception as e:
            # Connection might not exist yet, that's OK
            self.log.debug("Could not load Kerberos configuration (connection may not exist): %s", str(e))
        finally:
            self._kerberos_loaded = True

    def _ensure_security_config_loaded(self) -> None:
        """Load SSL/Kerberos config lazily (first command execution)."""
        self._load_ssl_config()
        self._load_kerberos_config()

    def _get_or_create_client_conf_dir(self) -> str | None:
        """
        If connection Extra has Ozone client addresses, build minimal ozone-site.xml and return temp dir.

        Extra keys (2–3 parameters only):
        - ``ozone_scm_address``: SCM client address (e.g. "scm" or "scm:9860"). Required to avoid
          CLI hanging; OM is taken from connection Host/Port.
        - ``ozone_recon_address``: optional Recon address (e.g. "recon:9891").

        Returns the temp directory path for OZONE_CONF_DIR, or None if ozone_scm_address not set.
        """
        if self._client_conf_dir is not None:
            return self._client_conf_dir
        try:
            conn = self.get_connection(self.ozone_conn_id)
            extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}
        except Exception as e:
            self.log.debug("Could not get connection for client conf: %s", e)
            return None

        ozone_scm_address = extra.get("ozone_scm_address")
        if not ozone_scm_address:
            return None

        tmpdir = tempfile.mkdtemp(prefix="airflow_ozone_conf_")
        try:
            om_addr = f"{conn.host or 'localhost'}:{conn.port or 9862}"
            scm_addr = (ozone_scm_address or "scm").strip()
            recon_addr = (extra.get("ozone_recon_address") or "").strip()
            om_esc = saxutils.escape(om_addr)
            scm_esc = saxutils.escape(scm_addr)
            recon_esc = saxutils.escape(recon_addr) if recon_addr else ""
            lines = [
                '<?xml version="1.0"?>',
                "<configuration>",
                f"  <property><name>ozone.om.address</name><value>{om_esc}</value></property>",
                f"  <property><name>ozone.scm.client.address</name><value>{scm_esc}</value></property>",
                f"  <property><name>ozone.scm.block.client.address</name><value>{scm_esc}</value></property>",
            ]
            if recon_esc:
                lines.append(
                    f"  <property><name>ozone.recon.address</name><value>{recon_esc}</value></property>"
                )
            lines.append("</configuration>")
            with open(os.path.join(tmpdir, "ozone-site.xml"), "w") as f:
                f.write("\n".join(lines))
            self.log.debug(
                "Generated minimal ozone-site.xml (om=%s, scm=%s)",
                om_addr,
                scm_addr,
            )
            with open(os.path.join(tmpdir, "core-site.xml"), "w") as f:
                f.write('<?xml version="1.0"?>\n<configuration>\n</configuration>\n')
            self._client_conf_dir = tmpdir
            return self._client_conf_dir
        except Exception as e:
            self.log.warning("Failed to create Ozone client conf from Extra: %s", e)
            try:
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass
            return None

    def _build_env(self) -> dict[str, str]:
        env = os.environ.copy()
        try:
            conn = self.get_connection(self.ozone_conn_id)
            env["OZONE_OM_ADDRESS"] = f"{conn.host}:{conn.port}"
        except Exception as e:
            self.log.debug("Could not set OZONE_OM_ADDRESS from connection: %s", e)
        conf_dir = self._get_or_create_client_conf_dir()
        if conf_dir:
            env["OZONE_CONF_DIR"] = conf_dir
        if self._ssl_env:
            env.update(self._ssl_env)
        if self._kerberos_env:
            env.update(self._kerberos_env)
        return env

    def _run_command(
        self,
        prepared_cmd: list[str],
        env: dict[str, str],
        timeout: int = 300,
        check: bool = True,
        input_text: str | None = None,
    ) -> tuple[str, str, int]:
        r"""
        Run Ozone CLI command locally.

        :param prepared_cmd: Full command list (e.g. ["ozone", "sh", "volume", "create", "/vol1"])
        :param env: Environment variables for the command
        :param timeout: Timeout in seconds
        :param check: If True, raise on non-zero return code
        :param input_text: Optional stdin (e.g. "yes\n" for recursive delete)
        :return: (stdout, stderr, returncode)
        """
        try:
            result = subprocess.run(
                prepared_cmd,
                capture_output=True,
                text=True,
                check=check,
                env=env,
                timeout=timeout,
                input=input_text,
            )
            return (result.stdout or ""), (result.stderr or ""), result.returncode
        except FileNotFoundError as e:
            raise OzoneCliError(
                "Ozone CLI not found. Please ensure Ozone is installed and available in PATH."
            ) from e

    def _get_config_dir(self) -> str | None:
        """
        Get Ozone configuration directory from environment variables.

        :return: Path to configuration directory if available, None otherwise
        """
        if self._kerberos_env:
            conf_dir = self._kerberos_env.get("OZONE_CONF_DIR") or self._kerberos_env.get("HADOOP_CONF_DIR")
            if conf_dir:
                return conf_dir
        return None

    def _check_config_files_exist(self, config_dir: str) -> bool:
        """
        Check if required Ozone configuration files exist in the directory.

        :param config_dir: Path to configuration directory
        :return: True if both core-site.xml and ozone-site.xml exist, False otherwise
        """
        if not config_dir:
            self.log.debug("Config directory is None or empty")
            return False

        if not os.path.isdir(config_dir):
            self.log.debug("Config directory does not exist: %s", config_dir)
            return False

        core_site = os.path.join(config_dir, "core-site.xml")
        ozone_site = os.path.join(config_dir, "ozone-site.xml")

        core_exists = os.path.isfile(core_site)
        ozone_exists = os.path.isfile(ozone_site)

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
        configuration files, which is critical for Kerberos authentication to work properly
        (notably as a workaround for known config-discovery issues such as HDDS-4602).

        Even if config files are not found yet, we still add --config flag when Kerberos
        is enabled, as the directory might be populated later or files might be in a different
        location that Ozone CLI can discover.

        :param cmd: Original command list (e.g., ["ozone", "sh", "volume", "create", "/vol1"])
        :return: Command list with --config flag added if Kerberos is enabled
        """
        # Kerberos/SSL config is loaded lazily.
        self._ensure_security_config_loaded()

        # Only add --config if Kerberos is enabled
        if not self._kerberos_env:
            return cmd

        # Idempotency: do not add --config twice
        if "--config" in cmd:
            return cmd

        # Get configuration directory from environment
        config_dir = self._get_config_dir()
        if not config_dir:
            self.log.warning(
                "Kerberos enabled but OZONE_CONF_DIR/HADOOP_CONF_DIR not set. "
                "Ozone CLI may not find security configuration files."
            )
            return cmd

        # Check if configuration files exist (for logging purposes)
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

        # Always add --config flag when Kerberos is enabled, even if files don't exist yet
        # This ensures Ozone CLI knows where to look for security configuration
        # Insert --config flag after "ozone" command
        # Format: ["ozone", "--config", "/path/to/conf", "sh", "volume", "create", ...]
        if len(cmd) > 0 and cmd[0] == "ozone":
            new_cmd = [cmd[0], "--config", config_dir] + cmd[1:]
            self.log.debug(
                "Added --config flag to Ozone CLI command for Kerberos authentication: %s",
                redact(" ".join(new_cmd)),
            )
            return new_cmd

        return cmd

    def run_cli_check(self, cmd: list[str], timeout: int = 300) -> subprocess.CompletedProcess[str]:
        """
        Execute Ozone CLI command without retries.

        Intended for fast checks (e.g. existence probes) where we do not want tenacity sleeps.
        """
        self._ensure_security_config_loaded()
        prepared_cmd = self._prepare_cli_command(cmd)
        env = self._build_env()
        stdout, stderr, returncode = self._run_command(prepared_cmd, env, timeout=timeout, check=False)
        return subprocess.CompletedProcess(prepared_cmd, returncode, stdout, stderr)

    @retry(
        # Wait exponentially between retries, starting at 2s, up to a max of 60s.
        # This prevents hammering the service.
        wait=wait_exponential(multiplier=2, min=2, max=60),
        # Stop retrying after 3 attempts. The first call is attempt 1.
        stop=stop_after_attempt(3),
        # Log a warning before each retry attempt. This is crucial for debugging.
        before_sleep=before_sleep_log(log, logging.WARNING),
        retry=retry_if_exception_type(OzoneCliTransientError),
        reraise=True,  # Re-raise the exception after the last retry attempt fails.
    )
    def run_cli(self, cmd: list[str]) -> str:
        """
        Execute Ozone CLI command with error handling and automatic retries on failure.

        :param cmd: List of command arguments (e.g., ["ozone", "fs", "-ls", "ofs://vol1/bucket1"])
        :return: Command output as string
        """
        command_str = " ".join(cmd)
        # Mask sensitive data in command before logging
        masked_command = redact(command_str)
        self.log.info("Executing Ozone CLI command (connection: %s): %s", self.ozone_conn_id, masked_command)
        # Log masked command arguments for debugging
        masked_cmd = [redact(str(arg)) for arg in cmd]
        self.log.debug("Full command arguments: %s", masked_cmd)

        start_time = time.time()
        try:
            self._ensure_security_config_loaded()
            prepared_cmd = self._prepare_cli_command(cmd)
            env = self._build_env()
            stdout, stderr, _ = self._run_command(prepared_cmd, env, timeout=300, check=True)
            execution_time = time.time() - start_time

            self.log.info("Ozone CLI command completed successfully in %.2f seconds", execution_time)

            if stdout:
                self.log.debug("Command stdout: %s", redact(stdout.strip()))
            if stderr:
                self.log.debug("Command stderr: %s", redact(stderr.strip()))

            return stdout.strip()
        except subprocess.CalledProcessError as e:
            execution_time = time.time() - start_time
            error_message = e.stderr.strip() if e.stderr else "No error message provided"
            return_code = e.returncode

            # Mask sensitive data before logging
            masked_error = redact(error_message)
            masked_command = redact(command_str)

            self.log.error(
                "Ozone CLI command failed after %.2f seconds (return code: %d, connection: %s)",
                execution_time,
                return_code,
                self.ozone_conn_id,
            )
            self.log.error("Failed command: %s", masked_command)
            self.log.error("Error output: %s", masked_error)
            if e.stdout:
                masked_stdout = redact(e.stdout.strip())
                self.log.debug("Command stdout before failure: %s", masked_stdout)

            if _is_transient_cli_failure(return_code, error_message):
                raise OzoneCliTransientError(
                    f"Ozone command failed (transient, return code: {return_code}): {masked_error}"
                )

            raise OzoneCliError(f"Ozone command failed (return code: {return_code}): {masked_error}")
        except OzoneCliError:
            raise
        except FileNotFoundError as e:
            self.log.error(
                "Ozone CLI command not found. Ensure Ozone binaries are installed and in PATH. Command attempted: %s",
                command_str,
            )
            raise OzoneCliError(
                "Ozone CLI not found. Please ensure Ozone is installed and available in PATH. "
                f"Original error: {str(e)}"
            ) from e
