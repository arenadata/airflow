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

import shlex
import subprocess
from typing import Any

from airflow.providers.arenadata.duckdb.utils.sql_script import (
    bind_sql_parameters,
    write_sql_script,
)
from airflow.providers.arenadata.duckdb.version_compat import AirflowException, BaseHook

DEFAULT_TIMEOUT = 300
DEFAULT_CLI_PATH = "/usr/bin/duckdb"
TEST_CONNECTION_TIMEOUT = 10


class DuckDbHook(BaseHook):  # pylint: disable=abstract-method
    """
    Hook to interact with DuckDB via CLI (subprocess).

    Reads connection parameters from an Airflow Connection:
      - host     : path to the .duckdb file, or ``:memory:`` for in-memory database
      - extra    : JSON object with optional keys:
          - ``duckdb_binary`` (str)  – path to the duckdb binary (default: ``"/usr/bin/duckdb"``)
          - ``timeout``         (int)  – subprocess timeout in seconds (default: 300)
          - ``readonly``        (bool) – open the database in read-only mode (default: false)
          - ``cli_params``    (str)  – additional CLI parameters passed to the DuckDB binary

    :param duckdb_conn_id: Airflow connection ID to use.
    """

    conn_name_attr = "duckdb_conn_id"
    default_conn_name = "duckdb_default"
    conn_type = "duckdb"
    hook_name = "DuckDB"

    def __init__(self, duckdb_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.duckdb_conn_id = duckdb_conn_id

    def _get_conn_params(self) -> dict[str, Any]:
        """Read and return connection parameters from the Airflow Connection."""
        conn = self.get_connection(self.duckdb_conn_id)
        extra = conn.extra_dejson or {}
        return {
            "db_path": conn.host or ":memory:",
            "cli_path": str(extra.get("duckdb_binary", DEFAULT_CLI_PATH)),
            "timeout": self._coerce_timeout(extra.get("timeout", DEFAULT_TIMEOUT)),
            "readonly": self._coerce_bool(extra.get("readonly", False)),
            "cli_params": str(extra.get("cli_params", "")),
        }

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
            raise AirflowException(f"Invalid 'timeout' in DuckDB connection extra: {value!r}") from exc

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
        cmd: list[str] = [params["cli_path"], params["db_path"]]

        if params["readonly"]:
            cmd.append("-readonly")

        cmd.append("-no-stdin")
        cmd.append("-bail")

        if params["cli_params"]:
            cmd.extend(shlex.split(params["cli_params"]))

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

        :param sql: SQL statement to execute (after Jinja templating).
        :param output_format: output format: ``"json"`` (default), ``"csv"``, or ``None``
            (default DuckDB table format).
        :param database: optional path to ``.duckdb`` file; overrides Connection ``host``
            when non-empty.
        :param parameters: optional ``%(name)s`` placeholders to bind before execution.
        :return: raw stdout from the CLI, stripped of leading/trailing whitespace.
        :raises AirflowException: on CLI error, timeout, or missing binary.
        """
        try:
            rendered_sql = bind_sql_parameters(sql, parameters)
        except (TypeError, ValueError) as exc:
            raise AirflowException(str(exc)) from exc

        params = self._get_conn_params()
        if database:
            params["db_path"] = database

        with write_sql_script(rendered_sql) as sql_file:
            cmd = self._build_command(
                params,
                output_format=output_format,
                sql_file=str(sql_file),
            )
            return self._run(cmd, timeout=params["timeout"])

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
        :param output_format: output format: ``"json"``, ``"csv"``, or ``None``.
        :param database: optional path to ``.duckdb`` file; overrides Connection ``host``
            when non-empty.
        :return: raw stdout from the CLI, stripped of leading/trailing whitespace.
        :raises AirflowException: on CLI error, timeout, or missing binary.
        """
        params = self._get_conn_params()
        if database:
            params["db_path"] = database
        cmd = self._build_command(params, output_format=output_format, sql_file=sql_file)
        return self._run(cmd, timeout=params["timeout"])

    def test_connection(self) -> tuple[bool, str]:
        """Verify DuckDB binary is executable (database path is not validated)."""
        try:
            params = self._get_conn_params()
            test_params = {**params, "db_path": ":memory:", "readonly": False}
            cmd = self._build_command(test_params, sql="SELECT 1", output_format=None)
        except Exception as exc:
            return False, f"DuckDB connection test failed: {exc}"

        self.log.debug("Testing DuckDB connection: %s", shlex.join(cmd))
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=TEST_CONNECTION_TIMEOUT,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return False, f"Cannot launch DuckDB binary '{params['cli_path']}': {exc}"

        if result.returncode == 0:
            return True, f"DuckDB binary is accessible: {params['cli_path']}"

        error = (result.stderr or result.stdout or "").strip() or "Unknown error"
        return False, f"DuckDB error (exit {result.returncode}): {error}"

    def _run(self, cmd: list[str], *, timeout: int) -> str:
        """Run *cmd* as a subprocess and return stripped stdout."""
        command = shlex.join(cmd)
        self.log.debug("Executing DuckDB command: %s", command)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=True,
            )
        except subprocess.TimeoutExpired as exc:
            raise AirflowException(f"DuckDB CLI timed out after {timeout}s: {command}") from exc
        except subprocess.CalledProcessError as exc:
            stdout = (exc.stdout or "").strip()
            stderr = (exc.stderr or "").strip()
            if stdout:
                self.log.info("DuckDB stdout: %s", stdout)
            if stderr:
                self.log.error("DuckDB stderr: %s", stderr)
            raise AirflowException(
                f"DuckDB CLI failed (exit code {exc.returncode}): {stderr or 'Unknown error'}"
            ) from exc
        except OSError as exc:
            raise AirflowException(f"Cannot launch DuckDB binary '{cmd[0]}': {exc}") from exc

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()

        if stdout:
            self.log.info("DuckDB stdout: %s", stdout)
        if stderr:
            self.log.warning("DuckDB stderr: %s", stderr)

        return stdout

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
                    '  "cli_params": ""\n'
                    "}"
                ),
            },
        }
