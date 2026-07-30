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
"""Tests for DuckDB hook."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.arenadata.duckdb.hooks.duckdb import (
    DEFAULT_CLI_PATH,
    DEFAULT_TIMEOUT,
    TEST_CONNECTION_TIMEOUT,
    DuckDbHook,
)
from airflow.providers.arenadata.duckdb.version_compat import AirflowException

MOCK_SUBPROCESS_RUN = "airflow.providers.arenadata.duckdb.hooks.duckdb.subprocess.run"

BASE_PARAMS = {
    "db_path": "/tmp/test.duckdb",
    "cli_path": "/usr/bin/duckdb",
    "timeout": 300,
    "readonly": False,
    "cli_params": "",
}


@pytest.fixture
def duckdb_hook() -> DuckDbHook:
    """Return a DuckDbHook with a mocked Airflow Connection."""
    hook = DuckDbHook(duckdb_conn_id="duckdb_test")
    conn = MagicMock()
    conn.host = "/tmp/test.duckdb"
    conn.extra_dejson = {
        "duckdb_binary": "/usr/bin/duckdb",
        "timeout": 300,
        "readonly": False,
        "cli_params": "",
    }
    hook.get_connection = lambda _: conn  # type: ignore[method-assign]
    return hook


class TestDuckDbHookConnection:
    """Test DuckDB hook connection parameter resolution."""

    def test_get_conn_params_defaults(self, duckdb_hook: DuckDbHook) -> None:
        """Empty host falls back to :memory: and extras use defaults."""
        conn = MagicMock()
        conn.host = ""
        conn.extra_dejson = {}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        params = duckdb_hook._get_conn_params()

        assert params == {
            "db_path": ":memory:",
            "cli_path": DEFAULT_CLI_PATH,
            "timeout": DEFAULT_TIMEOUT,
            "readonly": False,
            "cli_params": "",
        }

    def test_get_conn_params_from_extra(self, duckdb_hook: DuckDbHook) -> None:
        """Connection extras are mapped to hook runtime parameters."""
        params = duckdb_hook._get_conn_params()

        assert params["db_path"] == "/tmp/test.duckdb"
        assert params["cli_path"] == "/usr/bin/duckdb"
        assert params["timeout"] == 300
        assert params["readonly"] is False
        assert params["cli_params"] == ""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (True, True),
            (False, False),
            ("true", True),
            ("1", True),
            ("yes", True),
            ("false", False),
            ("0", False),
        ],
    )
    def test_coerce_bool(self, value: object, expected: bool) -> None:
        """Boolean extras accept common string representations."""
        assert DuckDbHook._coerce_bool(value) is expected

    def test_coerce_timeout_invalid(self) -> None:
        """Invalid timeout extra raises a clear AirflowException."""
        with pytest.raises(AirflowException, match="Invalid 'timeout'"):
            DuckDbHook._coerce_timeout("abc")

    def test_get_ui_field_behaviour(self) -> None:
        """Connection UI customisation exposes expected fields."""
        result = DuckDbHook.get_ui_field_behaviour()

        assert "hidden_fields" in result
        assert "relabeling" in result
        assert "placeholders" in result
        assert result["hidden_fields"] == ["port", "schema", "login", "password"]
        assert result["relabeling"]["host"] == "Database file path"
        assert ":memory:" in result["placeholders"]["host"]


class TestDuckDbBuildCommand:
    """Test DuckDB CLI command assembly."""

    def test_build_command_inline_sql(self, duckdb_hook: DuckDbHook) -> None:
        """Inline SQL is passed via -c."""
        cmd = duckdb_hook._build_command(BASE_PARAMS, output_format="json", sql="SELECT 1")

        assert cmd == [
            "/usr/bin/duckdb",
            "/tmp/test.duckdb",
            "-no-stdin",
            "-bail",
            "-json",
            "-c",
            "SELECT 1",
        ]

    def test_build_command_sql_file(self, duckdb_hook: DuckDbHook) -> None:
        """SQL file path is passed via -f."""
        cmd = duckdb_hook._build_command(
            BASE_PARAMS,
            sql_file="/tmp/query.sql",
        )

        assert cmd == [
            "/usr/bin/duckdb",
            "/tmp/test.duckdb",
            "-no-stdin",
            "-bail",
            "-f",
            "/tmp/query.sql",
        ]

    def test_build_command_readonly_and_cli_params(self, duckdb_hook: DuckDbHook) -> None:
        """Readonly mode and extra CLI params are appended to the command."""
        params = {
            **BASE_PARAMS,
            "readonly": True,
            "cli_params": "--threads 4",
        }
        cmd = duckdb_hook._build_command(params, sql="SELECT 1")

        assert cmd[:4] == ["/usr/bin/duckdb", "/tmp/test.duckdb", "-readonly", "-no-stdin"]
        assert "--threads" in cmd
        assert "4" in cmd
        assert cmd[-2:] == ["-c", "SELECT 1"]

    def test_build_command_csv_format(self, duckdb_hook: DuckDbHook) -> None:
        """CSV output adds -csv and -noheader flags."""
        cmd = duckdb_hook._build_command(BASE_PARAMS, output_format="csv", sql="SELECT 1")

        assert "-csv" in cmd
        assert "-noheader" in cmd
        assert "-json" not in cmd

    def test_build_command_requires_sql_or_file(self, duckdb_hook: DuckDbHook) -> None:
        """Command builder requires either sql or sql_file."""
        with pytest.raises(ValueError, match="Either 'sql' or 'sql_file' must be provided"):
            duckdb_hook._build_command(BASE_PARAMS)


class TestDuckDbHookRun:
    """Test DuckDB hook CLI execution via subprocess."""

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_success(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Successful CLI execution returns stripped stdout."""
        mock_run.return_value = MagicMock(returncode=0, stdout='[{"c":1}]\n', stderr="")

        result = duckdb_hook.run_cli("SELECT 1")

        assert result == '[{"c":1}]'
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["check"] is True
        assert call_kwargs["timeout"] == 300
        cmd = mock_run.call_args.args[0]
        assert cmd[0] == "/usr/bin/duckdb"
        assert cmd[1] == "/tmp/test.duckdb"
        assert "-f" in cmd
        assert "-c" not in cmd
        assert "SELECT 1" not in cmd

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_large_sql_uses_file_not_argv(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Regression: Linux MAX_ARG_STRLEN — SQL must not go through -c."""
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
        huge_sql = "SELECT '" + ("x" * 200_000) + "'"

        duckdb_hook.run_cli(huge_sql)

        cmd = mock_run.call_args.args[0]
        assert "-f" in cmd
        assert "-c" not in cmd

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_with_parameters(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Parameters are bound into the temp SQL file before execution."""
        captured: dict[str, str] = {}

        def fake_run(cmd: list[str], **kwargs: object) -> MagicMock:
            sql_file = Path(cmd[cmd.index("-f") + 1])
            captured["content"] = sql_file.read_text(encoding="utf-8")
            return MagicMock(returncode=0, stdout="ok", stderr="")

        mock_run.side_effect = fake_run

        duckdb_hook.run_cli("SELECT %(id)s", parameters={"id": 42})

        assert "42" in captured["content"]
        sql_file = Path(mock_run.call_args.args[0][mock_run.call_args.args[0].index("-f") + 1])
        assert not sql_file.exists()

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_missing_parameter_raises(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Missing parameter placeholders raise AirflowException."""
        with pytest.raises(AirflowException, match="Missing SQL parameter"):
            duckdb_hook.run_cli("SELECT %(x)s", parameters={})
        mock_run.assert_not_called()

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_logs_stdout_on_success(
        self, mock_run: MagicMock, duckdb_hook: DuckDbHook, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Successful CLI stdout is written to task logs."""
        mock_run.return_value = MagicMock(returncode=0, stdout='[{"c":1}]', stderr="")

        with caplog.at_level("INFO"):
            duckdb_hook.run_cli("SELECT 1")

        assert "DuckDB stdout:" in caplog.text
        assert '[{"c":1}]' in caplog.text

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_stderr_on_success_is_warning(
        self, mock_run: MagicMock, duckdb_hook: DuckDbHook, caplog: pytest.LogCaptureFixture
    ) -> None:
        """stderr on a successful run is logged at WARNING, not ERROR."""
        mock_run.return_value = MagicMock(returncode=0, stdout='[{"c":1}]', stderr="some note")
        with caplog.at_level("WARNING"):
            duckdb_hook.run_cli("SELECT 1")
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("DuckDB stderr:" in r.message for r in warnings)
        assert not any(r.levelname == "ERROR" for r in caplog.records)

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_timeout(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """CLI timeout is converted to AirflowException."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=["duckdb"], timeout=300)

        with pytest.raises(AirflowException, match="timed out after 300s"):
            duckdb_hook.run_cli("SELECT 1")

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_error(
        self, mock_run: MagicMock, duckdb_hook: DuckDbHook, caplog: pytest.LogCaptureFixture
    ) -> None:
        """CLI failure raises AirflowException and logs stderr."""
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["duckdb"],
            output="",
            stderr="syntax error",
        )

        with caplog.at_level("ERROR"):
            with pytest.raises(AirflowException, match="syntax error"):
                duckdb_hook.run_cli("BAD SQL")

        assert "DuckDB stderr:" in caplog.text
        assert "syntax error" in caplog.text

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_error_unknown_when_empty_stderr(
        self, mock_run: MagicMock, duckdb_hook: DuckDbHook
    ) -> None:
        """CLI failure without stderr uses Unknown error placeholder."""
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["duckdb"],
            output="",
            stderr="",
        )

        with pytest.raises(AirflowException, match="Unknown error"):
            duckdb_hook.run_cli("BAD SQL")

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_missing_binary(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Missing DuckDB binary raises a clear AirflowException."""
        mock_run.side_effect = FileNotFoundError("No such file")

        with pytest.raises(AirflowException, match="Cannot launch DuckDB binary"):
            duckdb_hook.run_cli("SELECT 1")

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_cli_permission_error(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Non-executable binary raises AirflowException, not raw PermissionError."""
        mock_run.side_effect = PermissionError(13, "Permission denied")

        with pytest.raises(AirflowException, match="Cannot launch DuckDB binary"):
            duckdb_hook.run_cli("SELECT 1")

    @patch(MOCK_SUBPROCESS_RUN)
    def test_run_file_success(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """run_file executes SQL via -f flag."""
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")

        result = duckdb_hook.run_file("/tmp/query.sql")

        assert result == "ok"
        cmd = mock_run.call_args.args[0]
        assert cmd[-2:] == ["-f", "/tmp/query.sql"]

    @patch(MOCK_SUBPROCESS_RUN)
    def test_database_override(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Operator-level database path overrides Connection host."""
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")

        duckdb_hook.run_cli("SELECT 1", database="/override.duckdb")

        cmd = mock_run.call_args.args[0]
        assert cmd[1] == "/override.duckdb"

    @patch(MOCK_SUBPROCESS_RUN)
    def test_database_empty_string_uses_connection(
        self, mock_run: MagicMock, duckdb_hook: DuckDbHook
    ) -> None:
        """Empty database override keeps Connection host (Jinja empty-string case)."""
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")

        duckdb_hook.run_cli("SELECT 1", database="")

        cmd = mock_run.call_args.args[0]
        assert cmd[1] == "/tmp/test.duckdb"


class TestDuckDbHookTestConnection:
    """Test DuckDB hook test_connection helper."""

    @patch(MOCK_SUBPROCESS_RUN)
    def test_test_connection_success(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """test_connection succeeds when binary is executable."""
        mock_run.return_value = MagicMock(returncode=0, stdout="1", stderr="")

        ok, message = duckdb_hook.test_connection()

        assert ok is True
        assert "accessible" in message
        assert "/usr/bin/duckdb" in message
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["check"] is False
        assert call_kwargs["timeout"] == TEST_CONNECTION_TIMEOUT
        cmd = mock_run.call_args.args[0]
        assert cmd[1] == ":memory:"
        assert "-c" in cmd
        assert "SELECT 1" in cmd

    @patch(MOCK_SUBPROCESS_RUN)
    def test_test_connection_binary_not_found(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """test_connection reports missing binary without raising."""
        mock_run.side_effect = FileNotFoundError("No such file")

        ok, message = duckdb_hook.test_connection()

        assert ok is False
        assert "Cannot launch DuckDB binary" in message
        assert "/usr/bin/duckdb" in message

    @patch(MOCK_SUBPROCESS_RUN)
    def test_test_connection_cli_error(self, mock_run: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """test_connection reports non-zero CLI exit code."""
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="permission denied")

        ok, message = duckdb_hook.test_connection()

        assert ok is False
        assert "exit 1" in message
        assert "permission denied" in message
