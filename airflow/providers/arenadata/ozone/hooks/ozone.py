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
import subprocess
import time

from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.arenadata.ozone.utils.ozone_env import OzoneEnv
from airflow.utils.log.secrets_masker import redact

log = logging.getLogger(__name__)


class OzoneCliError(AirflowException):
    """Non-retryable Ozone CLI error."""


class OzoneCliTransientError(OzoneCliError):
    """Retryable/transient Ozone CLI error."""


def is_transient_cli_failure(return_code: int | None, stderr: str) -> bool:
    """
    Best-effort classification of transient Ozone CLI failures.

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

        self._env = OzoneEnv(
            conn_id=self.ozone_conn_id,
            get_connection=self.get_connection,
            logger=self.log,
        )

        self.log.debug("OzoneHook initialized (conn_id=%s)", self.ozone_conn_id)

    def _ensure_security_config_loaded(self) -> None:
        """Load SSL/Kerberos config lazily (first command execution)."""
        self._env.ensure_loaded()

    def _get_or_create_client_conf_dir(self) -> str | None:
        return self._env.get_or_create_client_conf_dir()

    def _build_env(self) -> dict[str, str]:
        return self._env.build_env()

    def _get_config_dir(self) -> str | None:
        return self._env.get_config_dir()

    def _check_config_files_exist(self, config_dir: str) -> bool:
        """
        Check if required Ozone configuration files exist in the directory.

        Returns True if both core-site.xml and ozone-site.xml exist, False otherwise.
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
        configuration files, which is critical for Kerberos authentication to work properly.
        """
        self._ensure_security_config_loaded()

        if not self._env.kerberos_enabled():
            return cmd

        if "--config" in cmd:
            return cmd

        config_dir = self._get_config_dir()
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

        if len(cmd) > 0 and cmd[0] == "ozone":
            new_cmd = [cmd[0], "--config", config_dir] + cmd[1:]
            self.log.debug(
                "Added --config flag to Ozone CLI command for Kerberos authentication: %s",
                redact(" ".join(new_cmd)),
            )
            return new_cmd

        return cmd

    def _run_command(
        self,
        prepared_cmd: list[str],
        env: dict[str, str],
        timeout: int = 300,
        check: bool = True,
        input_text: str | None = None,
    ) -> tuple[str, str, int]:
        """
        Run Ozone CLI command locally.

        Returns (stdout, stderr, returncode).
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
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        before_sleep=before_sleep_log(log, logging.WARNING),
        retry=retry_if_exception_type(OzoneCliTransientError),
        reraise=True,
    )
    def run_cli(self, cmd: list[str]) -> str:
        """Execute Ozone CLI command with error handling and automatic retries on transient failures."""
        command_str = " ".join(cmd)
        masked_command = redact(command_str)

        self.log.info("Executing Ozone CLI command (connection: %s): %s", self.ozone_conn_id, masked_command)
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
