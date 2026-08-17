#
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
"""DuckDB hook."""

from __future__ import annotations

import os
import re
import shlex
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

from airflow.providers.arenadata.duckdb.utils.errors import (
    DuckDbCliError,
    DuckDbCliErrors,
    DuckDbConfigurationError,
)
from airflow.providers.arenadata.duckdb.utils.sql_script import (
    bind_sql_parameters,
    write_sql_script,
)
from airflow.providers.arenadata.duckdb.version_compat import (
    DEFAULT_SENSITIVE_FIELDS,
    BaseHook,
    mask_secret,
    redact,
)

DEFAULT_TIMEOUT = 300
DEFAULT_CLI_PATH = "/usr/bin/duckdb"
TEST_CONNECTION_TIMEOUT = 10
DEFAULT_LOG_OUTPUT_LIMIT = 2000
TERMINATE_GRACE_PERIOD_SECONDS = 5
# Backoff delays (seconds) before lock-retry attempts 2..N.
LOCK_RETRY_BACKOFF_SECONDS = (1, 2, 4, 8, 16)

# Hard ban after shlex.split; normalized tokens (see _normalize_cli_token)
# -s is an alias of -c.underscore forms -no-stdin → no_stdin
BANNED_CLI_PARAM_TOKENS = frozenset(
    {
        "c",
        "s",  # alias of -c
        "f",
        "cmd",
        "init",
        "json",
        "csv",
        "readonly",
        "bail",
        "no_stdin",
    }
)

# sensitive CLI flag names, including DEFAULT_SENSITIVE_FIELDS
SENSITIVE_CLI_FLAG_NAMES = frozenset(DEFAULT_SENSITIVE_FIELDS) | frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "access_key",
        "access_token",
        "api_key",
        "apikey",
        "private_key",
        "credential",
        "credentials",
        "secret_key",
        "aws_secret_access_key",
    }
)

ALLOWED_OUTPUT_FORMATS = frozenset({None, "json", "csv"})

_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:[\\/]")
_URI_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")

# start_new_session=True: child gets its own session/process group so killpg can
# terminate an ADO wrapper and its duckdb child.Trade-off, on worker SIGKILL
# (OOM / docker kill) on_kill is not called and an orphaned duckdb may keep the
# file lock until reaped


class DuckDbHook(BaseHook):  # pylint: disable=abstract-method
    """
    Hook to interact with DuckDB via CLI (subprocess).

    Reads connection parameters from an Airflow Connection:
      - host     : path to the .duckdb file, or ``:memory:`` for in-memory database
      - extra    : JSON object with optional keys:
          - ``duckdb_binary`` (str)  – path to the duckdb binary (default: ``"/usr/bin/duckdb"``)
          - ``timeout``         (int)  – subprocess timeout in seconds (default: 300)
          - ``readonly``        (bool) – open the database in read-only mode (default: false)
          - ``cli_params``    (str | list[str])  – additional CLI parameters passed to the
            DuckDB binary (shell string or JSON array of string tokens)
          - ``lock_retry_attempts`` (int) – opt-in lock retries; number of CLI launches
            when > 0 (default: 0 = off). Resolution: ctor override > extra > 0.

    When ``lock_retry_attempts`` > 0, wall-clock time can approach
    ``attempts × timeout + sum(backoff)`` (backoff ``1,2,4,8,16`` seconds).

    :param duckdb_conn_id: Airflow connection ID to use.
    :param lock_retry_attempts: Override for connection extra / default 0.
    :param log_output_limit: Max chars of stdout/stderr on INFO / in exception text.
    """

    conn_name_attr = "duckdb_conn_id"
    default_conn_name = "duckdb_default"
    conn_type = "duckdb"
    hook_name = "DuckDB"

    def __init__(
        self,
        duckdb_conn_id: str = default_conn_name,
        *,
        lock_retry_attempts: int | None = None,
        log_output_limit: int | None = None,
    ) -> None:
        super().__init__()
        self.duckdb_conn_id = duckdb_conn_id
        self._lock_retry_attempts_override = lock_retry_attempts
        self.log_output_limit = (
            DEFAULT_LOG_OUTPUT_LIMIT if log_output_limit is None else int(log_output_limit)
        )
        self._process: subprocess.Popen[str] | None = None
        self._killed = False

    def _get_conn_params(self) -> dict[str, Any]:
        """
        Read connection parameters, ban/mask ``cli_params``, and preflight the binary.

        Database-path preflight runs in :meth:`_resolve_params` after optional override.
        """
        conn = self.get_connection(self.duckdb_conn_id)
        extra = conn.extra_dejson or {}
        raw_cli_params = extra.get("cli_params", "")
        cli_param_tokens = self._parse_cli_params(raw_cli_params)
        self._mask_sensitive_cli_params(cli_param_tokens)
        self._assert_cli_params_allowed(cli_param_tokens)

        if self._lock_retry_attempts_override is not None:
            lock_retry_attempts = self._coerce_lock_retry_attempts(self._lock_retry_attempts_override)
        else:
            lock_retry_attempts = self._coerce_lock_retry_attempts(extra.get("lock_retry_attempts", 0))

        params: dict[str, Any] = {
            "db_path": conn.host or ":memory:",
            "cli_path": str(extra.get("duckdb_binary", DEFAULT_CLI_PATH)),
            "timeout": self._coerce_timeout(extra.get("timeout", DEFAULT_TIMEOUT)),
            "readonly": self._coerce_bool(extra.get("readonly", False)),
            "cli_params": raw_cli_params,
            "cli_param_tokens": cli_param_tokens,
            "lock_retry_attempts": lock_retry_attempts,
        }
        self._preflight_binary(params["cli_path"])
        return params

    def _resolve_params(
        self,
        *,
        database: str | None = None,
        validate_db_path: bool = True,
    ) -> dict[str, Any]:
        """Resolve params with optional database override and soft db-path preflight."""
        params = self._get_conn_params()
        if database:
            params["db_path"] = database
        if validate_db_path:
            self._preflight_db_path(params["db_path"], readonly=params["readonly"])
        return params

    @staticmethod
    def _coerce_bool(value: Any) -> bool:
        """Interpret a JSON/string extra value as a boolean (e.g. the string ``"false"``)."""
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("true", "1", "yes", "on")

    @staticmethod
    def _coerce_timeout(value: Any) -> int:
        """Parse the ``timeout`` extra into an int, raising a clear error on bad input."""
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise DuckDbConfigurationError(
                f"Invalid 'timeout' in DuckDB connection extra: {value!r}"
            ) from exc

    @staticmethod
    def _coerce_lock_retry_attempts(value: Any) -> int:
        """Parse ``lock_retry_attempts`` (ctor or extra); must be a non-negative int."""
        try:
            attempts = int(value)
        except (TypeError, ValueError) as exc:
            raise DuckDbConfigurationError(
                f"Invalid 'lock_retry_attempts' in DuckDB connection: {value!r}"
            ) from exc
        if attempts < 0:
            raise DuckDbConfigurationError(
                f"Invalid 'lock_retry_attempts' in DuckDB connection: {value!r} (must be >= 0)"
            )
        return attempts

    @staticmethod
    def _parse_cli_params(raw: Any) -> list[str]:
        """Parse Extra ``cli_params``: shell string or JSON list/tuple of strings."""
        if isinstance(raw, str):
            if not raw.strip():
                return []
            try:
                return shlex.split(raw)
            except ValueError as exc:
                raise DuckDbConfigurationError(
                    f"Invalid cli_params in DuckDB connection extra: {exc}"
                ) from exc
        if isinstance(raw, (list, tuple)):
            tokens: list[str] = []
            for item in raw:
                if not isinstance(item, str):
                    raise DuckDbConfigurationError(
                        "Invalid cli_params in DuckDB connection extra: "
                        f"list elements must be strings, got {type(item).__name__}"
                    )
                tokens.append(item)
            return tokens
        raise DuckDbConfigurationError(
            "Invalid cli_params in DuckDB connection extra: "
            f"expected a string or a list of strings, got {type(raw).__name__}"
        )

    @staticmethod
    def _normalize_cli_token(token: str) -> str:
        """
        Normalize a CLI flag token for ban/sensitive matching.

        Strips leading dashes, lowercases, and maps hyphens to underscores so
        ``--access-key`` matches ``access_key`` and ``-no-stdin`` matches ``no_stdin``.
        """
        flag = token.split("=", 1)[0]
        return flag.lstrip("-").lower().replace("-", "_")

    def _assert_cli_params_allowed(self, tokens: list[str]) -> None:
        """Hard-ban tokens that break the provider's SQL / format contract."""
        for token in tokens:
            if not token.startswith("-"):
                continue
            name = self._normalize_cli_token(token)
            if name in BANNED_CLI_PARAM_TOKENS:
                display = name.replace("_", "-")
                raise DuckDbConfigurationError(
                    f"cli_params contains banned flag '-{display}'. "
                    "SQL, -f/-c, -json/-csv, -readonly, -bail and -no-stdin are managed by the "
                    "provider; use DuckDbOperator(output_format=...) for json/csv output."
                )

    def _mask_sensitive_cli_params(self, tokens: list[str]) -> None:
        """Register sensitive ``cli_params`` values with the secrets masker."""
        for flag_name, value in self._iter_cli_flag_values(tokens):
            if flag_name in SENSITIVE_CLI_FLAG_NAMES and value:
                # mask_secret(value) without name registers always (no sensitive_fields gate)
                mask_secret(value)

    @classmethod
    def _iter_cli_flag_values(cls, tokens: list[str]) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        i = 0
        while i < len(tokens):
            token = tokens[i]
            if token.startswith("-") and "=" in token:
                flag, _, value = token.partition("=")
                pairs.append((cls._normalize_cli_token(flag), value))
                i += 1
                continue
            if token.startswith("-") and i + 1 < len(tokens) and not tokens[i + 1].startswith("-"):
                pairs.append((cls._normalize_cli_token(token), tokens[i + 1]))
                i += 2
                continue
            i += 1
        return pairs

    def _preflight_binary(self, cli_path: str) -> None:
        path = Path(cli_path)
        if path.is_file() and os.access(path, os.X_OK):
            return
        raise DuckDbConfigurationError(
            f"DuckDB binary not found or not executable: {cli_path}. "
            f"Set extra.duckdb_binary on connection '{self.duckdb_conn_id}'."
        )

    def _preflight_db_path(self, db_path: str, *, readonly: bool) -> None:
        """
        Soft preflight for local file paths only.

        Writable parent is required only for write mode. Readonly connections
        only check that the database file exists and is readable (RO mounts).
        """
        if db_path.startswith(":memory:"):
            return
        if self._is_remote_or_virtual_path(db_path):
            return

        path = Path(db_path)
        parent = path.parent
        if not parent.exists():
            raise DuckDbConfigurationError(
                f"DuckDB database parent directory does not exist: {parent} "
                f"(duckdb_conn_id={self.duckdb_conn_id}, db_path={db_path})"
            )
        if readonly:
            if not path.is_file():
                raise DuckDbConfigurationError(
                    f"DuckDB database file does not exist for readonly connection: {db_path} "
                    f"(duckdb_conn_id={self.duckdb_conn_id})"
                )
            if not os.access(path, os.R_OK):
                raise DuckDbConfigurationError(
                    f"DuckDB database file is not readable: {db_path} (duckdb_conn_id={self.duckdb_conn_id})"
                )
            return
        if not os.access(parent, os.W_OK):
            raise DuckDbConfigurationError(
                f"DuckDB database parent directory is not writable: {parent} "
                f"(duckdb_conn_id={self.duckdb_conn_id}, db_path={db_path})"
            )

    @staticmethod
    def _is_remote_or_virtual_path(db_path: str) -> bool:
        """Return True for URI-like schemes; Windows drive letters stay local."""
        if _WINDOWS_DRIVE_RE.match(db_path):
            return False
        return bool(_URI_SCHEME_RE.match(db_path))

    def _build_command(
        self,
        params: dict[str, Any],
        *,
        output_format: str | None = None,
        sql: str | None = None,
        sql_file: str | None = None,
    ) -> list[str]:
        """
        Build the DuckDB CLI command list.

        :param params: connection parameters from ``_get_conn_params()``.
        :param output_format: ``"json"`` or ``"csv"``; no flag added when ``None``.
        :param sql: inline SQL string to execute with ``-c``.
        :param sql_file: path to a ``.sql`` file to execute with ``-f``.
        """
        if output_format not in ALLOWED_OUTPUT_FORMATS:
            raise DuckDbConfigurationError(
                f"Invalid output_format={output_format!r}. Allowed values: None, 'json', 'csv'."
            )

        cmd: list[str] = [params["cli_path"], params["db_path"]]

        if params["readonly"]:
            cmd.append("-readonly")

        cmd.append("-no-stdin")
        cmd.append("-bail")

        tokens = params.get("cli_param_tokens")
        if tokens is None:
            tokens = self._parse_cli_params(params.get("cli_params", ""))
        if tokens:
            cmd.extend(tokens)

        if output_format == "json":
            cmd.append("-json")
        elif output_format == "csv":
            cmd.append("-csv")
            cmd.append("-noheader")

        if sql is not None:
            cmd += ["-c", sql]
        elif sql_file is not None:
            cmd += ["-f", sql_file]
        else:
            raise ValueError("Either 'sql' or 'sql_file' must be provided.")

        return cmd

    def run_cli(
        self,
        sql: str,
        *,
        output_format: str | None = "json",
        database: str | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> str:
        """
        Execute SQL via DuckDB CLI.

        SQL is written to a temporary file and executed with ``-f`` to avoid
        Linux ``MAX_ARG_STRLEN`` limits on inline ``-c`` arguments.

        Order: connection/preflight/secrets → bind → CLI

        :param sql: SQL statement to execute (after Jinja templating).
        :param output_format: output format: ``"json"`` (default), ``"csv"``, or ``None``
            (CLI table). Default is JSON so XCom and sensors can parse a single statement.
            :meth:`run_file` defaults to ``None`` instead: ``-json`` plus first-array salvage
            would keep only the first result of a multi-statement script.
        :param database: optional path to ``.duckdb`` file; overrides Connection ``host``
            when non-empty.
        :param parameters: optional ``%(name)s`` placeholders to bind before execution.
        :return: raw stdout from the CLI, stripped of leading/trailing whitespace.
        :raises DuckDbConfigurationError: on configuration, bind, or preflight errors
            (including a missing or non-executable binary).
        :raises DuckDbCliError: on CLI non-zero exit, timeout, or ``Popen`` ``OSError``
            after preflight (binary vanished between preflight and launch).
        """
        params = self._resolve_params(database=database)
        try:
            rendered_sql = bind_sql_parameters(sql, parameters)
        except (TypeError, ValueError) as exc:
            raise DuckDbConfigurationError(str(exc)) from exc

        # temp SQL file stays outside the lock-retry loop (one file for all attempts)
        with write_sql_script(rendered_sql) as sql_file:
            cmd = self._build_command(
                params,
                output_format=output_format,
                sql_file=str(sql_file),
            )
            return self._run(
                cmd,
                timeout=params["timeout"],
                db_path=params["db_path"],
                lock_retry_attempts=params["lock_retry_attempts"],
            )

    def run_file(
        self,
        sql_file: str,
        *,
        output_format: str | None = None,
        database: str | None = None,
    ) -> str:
        """
        Execute a ``.sql`` file via DuckDB CLI.

        The file is executed as-is; ``%(name)s`` parameter binding is not applied.
        Use :meth:`run_cli` for parameterized inline SQL.

        :param sql_file: absolute path to the SQL file.
        :param output_format: output format: ``"json"``, ``"csv"``, or ``None`` (default).
            Default is ``None`` (CLI table) so a multi-statement file is not truncated by
            ``-json`` / first-array salvage. Pass ``"json"`` for a single-statement file.
        :param database: optional path to ``.duckdb`` file; overrides Connection ``host``
            when non-empty.
        :return: raw stdout from the CLI, stripped of leading/trailing whitespace.
        :raises DuckDbConfigurationError: when the SQL file does not exist, or on
            connection / preflight errors (including a missing or non-executable binary).
        :raises DuckDbCliError: on CLI non-zero exit, timeout, or ``Popen`` ``OSError``
            after preflight.
        """
        params = self._resolve_params(database=database)
        sql_path = Path(sql_file)
        if not sql_path.is_file():
            raise DuckDbConfigurationError(f"SQL file does not exist: {sql_file}")
        cmd = self._build_command(params, output_format=output_format, sql_file=sql_file)
        return self._run(
            cmd,
            timeout=params["timeout"],
            db_path=params["db_path"],
            lock_retry_attempts=params["lock_retry_attempts"],
        )

    def test_connection(self) -> tuple[bool, str]:
        """
        Verify DuckDB binary via shared params path (ban/secrets/binary preflight).

        Uses ``:memory:`` for the probe query; lock-retry is not applied.
        """
        try:
            params = self._resolve_params(validate_db_path=False)
            test_params = {**params, "db_path": ":memory:", "readonly": False}
            cmd = self._build_command(test_params, sql="SELECT 1", output_format=None)
            self._run(
                cmd,
                timeout=TEST_CONNECTION_TIMEOUT,
                db_path=":memory:",
                lock_retry_attempts=0,
            )
        except DuckDbConfigurationError as exc:
            return False, str(exc)
        except DuckDbCliError as exc:
            return False, str(exc)
        except Exception as exc:
            return False, f"DuckDB connection test failed: {exc}"

        return True, f"DuckDB binary is accessible: {params['cli_path']}"

    def on_kill(self) -> None:
        """Mark the hook as killed and terminate the active DuckDB process group."""
        self._killed = True
        if self._process is not None:
            self._terminate_process(self._process)

    def _run(
        self,
        cmd: list[str],
        *,
        timeout: int,
        db_path: str,
        lock_retry_attempts: int = 0,
    ) -> str:
        """
        Run *cmd*, optionally retrying on file-lock failures.

        ``lock_retry_attempts`` > 0 is the number of CLI launches; ``0`` means a
        single launch (opt-in off). Lock conflicts always raise a clear error;
        non-lock errors and timeouts are never retried.
        """
        max_launches = lock_retry_attempts if lock_retry_attempts > 0 else 1

        for attempt in range(1, max_launches + 1):
            if self._killed:
                raise DuckDbCliError(
                    "DuckDB process terminated because the task was killed",
                    command=cmd,
                    retryable=False,
                )

            if attempt > 1:
                delay = LOCK_RETRY_BACKOFF_SECONDS[min(attempt - 2, len(LOCK_RETRY_BACKOFF_SECONDS) - 1)]
                self.log.warning(
                    "DuckDB lock retry: sleeping %ss before attempt %s/%s (duckdb_conn_id=%s, db_path=%s)",
                    delay,
                    attempt,
                    max_launches,
                    self.duckdb_conn_id,
                    db_path,
                )
                time.sleep(delay)

            try:
                return self._run_once(cmd, timeout=timeout, db_path=db_path)
            except DuckDbCliError as exc:
                if "task was killed" in str(exc):
                    raise
                if not DuckDbCliErrors.is_lock_failure(exc.stderr):
                    raise
                if attempt < max_launches:
                    # Raw lock stderr on WARNING - do not redact
                    self.log.warning(
                        "DuckDB lock conflict on attempt %s/%s (duckdb_conn_id=%s, db_path=%s): %s",
                        attempt,
                        max_launches,
                        self.duckdb_conn_id,
                        db_path,
                        exc.stderr or "",
                    )
                    continue
                raise DuckDbCliError.for_lock_conflict(
                    duckdb_conn_id=self.duckdb_conn_id,
                    db_path=db_path,
                    command=cmd,
                    stderr=exc.stderr,
                    returncode=exc.returncode,
                ) from exc

        raise RuntimeError("DuckDB lock-retry loop exited without result")  # pragma: no cover

    def _run_once(self, cmd: list[str], *, timeout: int, db_path: str) -> str:
        """Run *cmd* via Popen/communicate once and return stripped stdout."""
        if self._killed:
            raise DuckDbCliError(
                "DuckDB process terminated because the task was killed",
                command=cmd,
                retryable=False,
            )

        masked_command = redact(shlex.join(cmd))
        self.log.debug("Executing DuckDB command: %s", masked_command)

        start = time.monotonic()
        try:
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
        except OSError as exc:
            raise DuckDbCliError(
                f"Cannot launch DuckDB binary '{cmd[0]}': {exc}",
                command=cmd,
                stderr=str(exc),
                retryable=False,
            ) from exc

        self._process = process
        if self._killed:
            self._terminate_process(process)
            stdout, stderr = process.communicate()
            self._process = None
            raise DuckDbCliError(
                "DuckDB process terminated because the task was killed",
                command=cmd,
                stderr=(stderr or "").strip(),
                returncode=process.returncode,
                retryable=False,
            )

        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            self._terminate_process(process)
            stdout, stderr = process.communicate()
            self._process = None
            duration = time.monotonic() - start
            stdout_s = (stdout or "").strip()
            stderr_s = (stderr or "").strip()
            self._log_streams_on_failure(stdout_s, stderr_s)
            if self._killed:
                raise DuckDbCliError(
                    "DuckDB process terminated because the task was killed",
                    command=cmd,
                    stderr=stderr_s,
                    returncode=process.returncode,
                    retryable=False,
                ) from exc
            self.log.info(
                "DuckDB timed out (duckdb_conn_id=%s, db_path=%s, returncode=%s, duration=%.2fs)",
                self.duckdb_conn_id,
                db_path,
                process.returncode,
                duration,
            )
            raise DuckDbCliError(
                f"DuckDB CLI timed out after {timeout}s: {masked_command}",
                command=cmd,
                stderr=stderr_s or stdout_s,
                returncode=process.returncode,
                retryable=False,
            ) from exc

        self._process = None
        duration = time.monotonic() - start
        stdout_s = (stdout or "").strip()
        stderr_s = (stderr or "").strip()
        returncode = process.returncode if process.returncode is not None else 1

        if self._killed:
            self._log_streams_on_failure(stdout_s, stderr_s)
            raise DuckDbCliError(
                "DuckDB process terminated because the task was killed",
                command=cmd,
                stderr=stderr_s,
                returncode=returncode,
                retryable=False,
            )

        if returncode != 0:
            self._log_streams_on_failure(stdout_s, stderr_s)
            self.log.info(
                "DuckDB failed (duckdb_conn_id=%s, db_path=%s, returncode=%s, duration=%.2fs)",
                self.duckdb_conn_id,
                db_path,
                returncode,
                duration,
            )
            error_text = stderr_s or "Unknown error"
            raise DuckDbCliError(
                f"DuckDB CLI failed (exit code {returncode}): {self._truncate_for_exception(error_text)}",
                command=cmd,
                stderr=stderr_s,
                returncode=returncode,
                retryable=False,
            )

        self.log.info(
            "DuckDB finished (duckdb_conn_id=%s, db_path=%s, returncode=%s, duration=%.2fs)",
            self.duckdb_conn_id,
            db_path,
            returncode,
            duration,
        )
        if stdout_s:
            self.log.info("DuckDB stdout: %s", self._truncate_for_info(stdout_s))
            self.log.debug("DuckDB stdout (full): %s", stdout_s)
        if stderr_s:
            self.log.info("DuckDB stderr: %s", self._truncate_for_info(stderr_s))
            self.log.debug("DuckDB stderr (full): %s", stderr_s)

        return stdout_s

    def _log_streams_on_failure(self, stdout: str, stderr: str) -> None:
        if stdout:
            self.log.info("DuckDB stdout: %s", self._truncate_for_info(stdout))
            self.log.debug("DuckDB stdout (full): %s", stdout)
        if stderr:
            self.log.error("DuckDB stderr: %s", self._truncate_for_info(stderr))
            self.log.debug("DuckDB stderr (full): %s", stderr)

    def _truncate_for_info(self, text: str) -> str:
        limit = self.log_output_limit
        if len(text) <= limit:
            return text
        return f"{text[:limit]}... truncated ({len(text)} chars), full output at DEBUG"

    def _truncate_for_exception(self, text: str) -> str:
        """Keep the tail of error text (DuckDB puts the error at the end)."""
        limit = self.log_output_limit
        if len(text) <= limit:
            return text
        return f"...[truncated head, {len(text)} chars total]\n{text[-limit:]}"

    def _terminate_process(self, process: subprocess.Popen[str]) -> None:
        """
        Stop process group (POSIX) or process (fallback), then reap.

        Sequence: SIGTERM → wait(grace) → SIGKILL if still alive.
        Caller must ``communicate()`` afterwards to drain pipes.
        """
        if process.poll() is not None:
            return

        if os.name == "posix":
            self._terminate_process_group(process)
        else:
            self._terminate_process_fallback(process)

    def _terminate_process_group(self, process: subprocess.Popen[str]) -> None:
        try:
            pgid = os.getpgid(process.pid)
        except (ProcessLookupError, OSError):
            return

        try:
            os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, OSError):
            return

        try:
            process.wait(timeout=TERMINATE_GRACE_PERIOD_SECONDS)
            return
        except subprocess.TimeoutExpired:
            pass

        try:
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, OSError):
            pass

        try:
            process.wait(timeout=TERMINATE_GRACE_PERIOD_SECONDS)
        except subprocess.TimeoutExpired:
            self.log.warning("DuckDB process group did not exit after SIGKILL (pid=%s)", process.pid)

    def _terminate_process_fallback(self, process: subprocess.Popen[str]) -> None:
        process.terminate()
        try:
            process.wait(timeout=TERMINATE_GRACE_PERIOD_SECONDS)
            return
        except subprocess.TimeoutExpired:
            pass
        process.kill()
        try:
            process.wait(timeout=TERMINATE_GRACE_PERIOD_SECONDS)
        except subprocess.TimeoutExpired:
            self.log.warning("DuckDB process did not exit after kill (pid=%s)", process.pid)

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field labels and placeholders for the Airflow connection UI."""
        return {
            "hidden_fields": ["port", "schema", "login", "password"],
            "relabeling": {
                "host": "Database file path",
            },
            "placeholders": {
                "host": ":memory:  or  /absolute/path/to/file.duckdb",
                "extra": (
                    "{\n"
                    '  "duckdb_binary": "/usr/bin/duckdb",\n'
                    '  "timeout": 300,\n'
                    '  "readonly": false,\n'
                    '  "cli_params": "",\n'
                    '  "lock_retry_attempts": 0\n'
                    "}"
                ),
            },
        }
