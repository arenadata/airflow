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
    BANNED_CLI_PARAM_TOKENS,
    DEFAULT_CLI_PATH,
    DEFAULT_LOG_OUTPUT_LIMIT,
    DEFAULT_TIMEOUT,
    LOCK_RETRY_BACKOFF_SECONDS,
    TEST_CONNECTION_TIMEOUT,
    DuckDbHook,
)
from airflow.providers.arenadata.duckdb.utils.errors import DuckDbCliError, DuckDbConfigurationError
from airflow.providers.arenadata.duckdb.version_compat import redact
from airflow.sdk._shared.secrets_masker import reset_secrets_masker

LOCK_STDERR = (
    'Could not set lock on file "/tmp/test.duckdb": Conflicting lock is held'
)

MOCK_POPEN = "airflow.providers.arenadata.duckdb.hooks.duckdb.subprocess.Popen"

BASE_PARAMS = {
    "db_path": "/tmp/test.duckdb",
    "cli_path": "/usr/bin/duckdb",
    "timeout": 300,
    "readonly": False,
    "cli_params": "",
    "cli_param_tokens": [],
}


def _mock_process(
    *,
    stdout: str = '[{"c":1}]\n',
    stderr: str = "",
    returncode: int = 0,
    communicate_side_effect: object | None = None,
) -> MagicMock:
    proc = MagicMock()
    proc.pid = 12345
    proc.returncode = returncode
    proc.poll.return_value = returncode
    if communicate_side_effect is not None:
        proc.communicate.side_effect = communicate_side_effect
    else:
        proc.communicate.return_value = (stdout, stderr)
    return proc


@pytest.fixture
def duckdb_hook() -> DuckDbHook:
    """Return a DuckDbHook with a mocked Airflow Connection and skipped preflight."""
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
    hook._preflight_binary = MagicMock()  # type: ignore[method-assign]
    hook._preflight_db_path = MagicMock()  # type: ignore[method-assign]
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

        assert params["db_path"] == ":memory:"
        assert params["cli_path"] == DEFAULT_CLI_PATH
        assert params["timeout"] == DEFAULT_TIMEOUT
        assert params["readonly"] is False
        assert params["cli_params"] == ""
        assert params["cli_param_tokens"] == []
        assert params["lock_retry_attempts"] == 0
        duckdb_hook._preflight_binary.assert_called_once_with(DEFAULT_CLI_PATH)

    def test_get_conn_params_from_extra(self, duckdb_hook: DuckDbHook) -> None:
        """Connection extras are mapped to hook runtime parameters."""
        params = duckdb_hook._get_conn_params()

        assert params["db_path"] == "/tmp/test.duckdb"
        assert params["cli_path"] == "/usr/bin/duckdb"
        assert params["timeout"] == 300
        assert params["readonly"] is False
        assert params["cli_params"] == ""
        assert params["cli_param_tokens"] == []

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
        """Invalid timeout extra raises DuckDbConfigurationError."""
        with pytest.raises(DuckDbConfigurationError, match="Invalid 'timeout'"):
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
        assert "lock_retry_attempts" in result["placeholders"]["extra"]

    def test_ctor_log_output_limit_default(self) -> None:
        """Default log_output_limit comes from module constant."""
        hook = DuckDbHook()
        assert hook.log_output_limit == DEFAULT_LOG_OUTPUT_LIMIT


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
            "cli_param_tokens": ["--threads", "4"],
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


class TestDuckDbBanList:
    """Ban-list for cli_params."""

    @pytest.mark.parametrize("flag", sorted(BANNED_CLI_PARAM_TOKENS))
    def test_banned_flags_raise(self, duckdb_hook: DuckDbHook, flag: str) -> None:
        """Banned tokens raise DuckDbConfigurationError with output_format hint."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        # Pass hyphen form users type (no_stdin → -no-stdin)
        cli_flag = f"-{flag.replace('_', '-')}"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "cli_params": cli_flag}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        with pytest.raises(DuckDbConfigurationError, match="banned flag") as exc_info:
            duckdb_hook._get_conn_params()

        assert "output_format" in str(exc_info.value)

    def test_ban_s_alias_of_c(self, duckdb_hook: DuckDbHook) -> None:
        """-s is banned (alias of -c per DuckDB CLI docs)."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "cli_params": "-s 'SELECT 1'"}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        with pytest.raises(DuckDbConfigurationError, match="banned flag '-s'"):
            duckdb_hook._get_conn_params()

    def test_ban_no_stdin_hyphen_form(self, duckdb_hook: DuckDbHook) -> None:
        """DuckDB spelling -no-stdin is banned after hyphen→underscore normalize."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "cli_params": "-no-stdin"}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        with pytest.raises(DuckDbConfigurationError, match="banned flag '-no-stdin'"):
            duckdb_hook._get_conn_params()

    def test_allowed_cli_params_pass(self, duckdb_hook: DuckDbHook) -> None:
        """Non-banned params like --threads are allowed."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "cli_params": "--threads 4"}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        params = duckdb_hook._get_conn_params()

        assert params["cli_param_tokens"] == ["--threads", "4"]


class TestDuckDbPreflight:
    """Soft path/binary preflight."""

    def test_memory_named_skips_path_checks(self) -> None:
        """Named in-memory DBs use startswith(':memory:'), not equality."""
        hook = DuckDbHook(duckdb_conn_id="duckdb_test")
        hook._preflight_db_path(":memory:analytics", readonly=False)

    def test_scheme_prefix_skips_local_checks(self) -> None:
        """URI scheme paths are left to the CLI."""
        hook = DuckDbHook(duckdb_conn_id="duckdb_test")
        hook._preflight_db_path("s3://bucket/db.duckdb", readonly=True)
        hook._preflight_db_path("md:my_db", readonly=False)

    def test_windows_drive_is_local_not_scheme(self, tmp_path: Path) -> None:
        """Windows drive letters must not be treated as URI schemes."""
        hook = DuckDbHook(duckdb_conn_id="duckdb_test")
        assert hook._is_remote_or_virtual_path(r"C:\data\db.duckdb") is False
        assert hook._is_remote_or_virtual_path("C:/data/db.duckdb") is False
        assert hook._is_remote_or_virtual_path("s3://bucket/x") is True

        # local preflight on a real tmp parent (posix path); drive logic is covered above
        db = tmp_path / "local.duckdb"
        hook._preflight_db_path(str(db), readonly=False)

    def test_local_parent_missing_raises(self, tmp_path: Path) -> None:
        """Missing parent directory raises ConfigurationError."""
        hook = DuckDbHook(duckdb_conn_id="duckdb_test")
        missing = tmp_path / "no_such_dir" / "db.duckdb"
        with pytest.raises(DuckDbConfigurationError, match="parent directory does not exist"):
            hook._preflight_db_path(str(missing), readonly=False)

    def test_readonly_missing_file_raises(self, tmp_path: Path) -> None:
        """readonly=true requires an existing readable file."""
        hook = DuckDbHook(duckdb_conn_id="duckdb_test")
        missing = tmp_path / "missing.duckdb"
        with pytest.raises(DuckDbConfigurationError, match="does not exist for readonly"):
            hook._preflight_db_path(str(missing), readonly=True)

    def test_readonly_allows_non_writable_parent(self, tmp_path: Path) -> None:
        """readonly=true does not require a writable parent (RO mounts)."""
        hook = DuckDbHook(duckdb_conn_id="duckdb_test")
        db = tmp_path / "ro.duckdb"
        db.write_bytes(b"")
        tmp_path.chmod(0o555)
        try:
            hook._preflight_db_path(str(db), readonly=True)
        finally:
            tmp_path.chmod(0o755)

    def test_write_mode_requires_writable_parent(self, tmp_path: Path) -> None:
        """Write mode still requires a writable parent directory."""
        hook = DuckDbHook(duckdb_conn_id="duckdb_test")
        db = tmp_path / "w.duckdb"
        tmp_path.chmod(0o555)
        try:
            with pytest.raises(DuckDbConfigurationError, match="not writable"):
                hook._preflight_db_path(str(db), readonly=False)
        finally:
            tmp_path.chmod(0o755)

    def test_binary_missing_raises(self, tmp_path: Path) -> None:
        """Missing binary raises ConfigurationError with duckdb_binary hint."""
        hook = DuckDbHook(duckdb_conn_id="duckdb_test")
        with pytest.raises(DuckDbConfigurationError, match="extra.duckdb_binary") as exc_info:
            hook._preflight_binary(str(tmp_path / "no-duckdb"))
        assert "duckdb_test" in str(exc_info.value)

    def test_run_file_missing_sql_raises(self, duckdb_hook: DuckDbHook, tmp_path: Path) -> None:
        """run_file preflight: SQL file must exist."""
        with pytest.raises(DuckDbConfigurationError, match="SQL file does not exist"):
            duckdb_hook.run_file(str(tmp_path / "missing.sql"))


class TestDuckDbSecretsMasking:
    """mask_secret registration from cli_params."""

    @pytest.mark.enable_redact
    def test_mask_secret_registers_flag_value(self, duckdb_hook: DuckDbHook) -> None:
        """Sensitive flag values are registered via mask_secret(value) without name."""
        reset_secrets_masker()
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {
            "duckdb_binary": "/usr/bin/duckdb",
            "cli_params": "--password supersecretvalue",
        }
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        duckdb_hook._get_conn_params()

        assert redact("prefix supersecretvalue suffix") == "prefix *** suffix"
        reset_secrets_masker()

    @pytest.mark.enable_redact
    def test_mask_secret_equals_form(self, duckdb_hook: DuckDbHook) -> None:
        """--flag=value form is also registered."""
        reset_secrets_masker()
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {
            "duckdb_binary": "/usr/bin/duckdb",
            "cli_params": "--token=abcdef12345",
        }
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        duckdb_hook._get_conn_params()

        assert redact("token=abcdef12345") == "token=***"
        reset_secrets_masker()

    @pytest.mark.enable_redact
    def test_mask_secret_hyphenated_flag(self, duckdb_hook: DuckDbHook) -> None:
        """Hyphenated sensitive flags (--access-key) match underscore names."""
        reset_secrets_masker()
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {
            "duckdb_binary": "/usr/bin/duckdb",
            "cli_params": "--access-key SuperSecret12345",
        }
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        duckdb_hook._get_conn_params()

        assert redact("prefix SuperSecret12345 suffix") == "prefix *** suffix"
        reset_secrets_masker()

    @pytest.mark.enable_redact
    def test_mask_secret_hyphenated_equals_form(self, duckdb_hook: DuckDbHook) -> None:
        """--access-key=value form is registered after hyphen normalize."""
        reset_secrets_masker()
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {
            "duckdb_binary": "/usr/bin/duckdb",
            "cli_params": "--access-key=SuperSecret12345",
        }
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        duckdb_hook._get_conn_params()

        assert redact("prefix SuperSecret12345 suffix") == "prefix *** suffix"
        reset_secrets_masker()


class TestDuckDbHookRun:
    """Test DuckDB hook CLI execution via Popen."""

    @patch(MOCK_POPEN)
    def test_run_cli_success(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Successful CLI execution returns stripped stdout."""
        mock_popen.return_value = _mock_process()

        result = duckdb_hook.run_cli("SELECT 1")

        assert result == '[{"c":1}]'
        mock_popen.assert_called_once()
        call_kwargs = mock_popen.call_args.kwargs
        assert call_kwargs["stdin"] is subprocess.DEVNULL
        assert call_kwargs["start_new_session"] is True
        assert call_kwargs["text"] is True
        cmd = mock_popen.call_args.args[0]
        assert cmd[0] == "/usr/bin/duckdb"
        assert cmd[1] == "/tmp/test.duckdb"
        assert "-f" in cmd
        assert "-c" not in cmd
        assert "SELECT 1" not in cmd

    @patch(MOCK_POPEN)
    def test_run_cli_large_sql_uses_file_not_argv(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook
    ) -> None:
        """Regression: Linux MAX_ARG_STRLEN — SQL must not go through -c."""
        mock_popen.return_value = _mock_process(stdout="ok\n")
        huge_sql = "SELECT '" + ("x" * 200_000) + "'"

        duckdb_hook.run_cli(huge_sql)

        cmd = mock_popen.call_args.args[0]
        assert "-f" in cmd
        assert "-c" not in cmd

    @patch(MOCK_POPEN)
    def test_run_cli_with_parameters(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Parameters are bound into the temp SQL file before execution."""
        captured: dict[str, str] = {}

        def fake_popen(cmd: list[str], **kwargs: object) -> MagicMock:
            sql_file = Path(cmd[cmd.index("-f") + 1])
            captured["content"] = sql_file.read_text(encoding="utf-8")
            return _mock_process(stdout="ok\n")

        mock_popen.side_effect = fake_popen

        duckdb_hook.run_cli("SELECT %(id)s", parameters={"id": 42})

        assert "42" in captured["content"]
        sql_file = Path(mock_popen.call_args.args[0][mock_popen.call_args.args[0].index("-f") + 1])
        assert not sql_file.exists()

    @patch(MOCK_POPEN)
    def test_run_cli_missing_parameter_raises(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Missing parameter placeholders raise before CLI is invoked."""
        with pytest.raises(DuckDbConfigurationError, match="Missing SQL parameter"):
            duckdb_hook.run_cli("SELECT %(x)s", parameters={})
        mock_popen.assert_not_called()

    @patch(MOCK_POPEN)
    def test_run_cli_config_before_bind(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Configuration errors are raised before parameter binding."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "cli_params": "-c SELECT 1"}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        with pytest.raises(DuckDbConfigurationError, match="banned flag"):
            duckdb_hook.run_cli("SELECT %(x)s", parameters={})
        mock_popen.assert_not_called()

    @patch(MOCK_POPEN)
    def test_run_cli_logs_stdout_on_success(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Successful CLI stdout is written to task logs at INFO."""
        mock_popen.return_value = _mock_process()

        with caplog.at_level("INFO"):
            duckdb_hook.run_cli("SELECT 1")

        assert "DuckDB stdout:" in caplog.text
        assert '[{"c":1}]' in caplog.text
        assert "duckdb_conn_id=duckdb_test" in caplog.text

    @patch(MOCK_POPEN)
    def test_run_cli_stderr_on_success_is_info(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook, caplog: pytest.LogCaptureFixture
    ) -> None:
        """stderr on a successful run is logged at INFO."""
        mock_popen.return_value = _mock_process(stderr="some note")
        with caplog.at_level("INFO"):
            duckdb_hook.run_cli("SELECT 1")
        info_msgs = [r.message for r in caplog.records if r.levelname == "INFO"]
        assert any("DuckDB stderr:" in m for m in info_msgs)
        assert not any(r.levelname == "ERROR" for r in caplog.records)

    @patch(MOCK_POPEN)
    def test_run_cli_stdout_truncated_on_info(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook, caplog: pytest.LogCaptureFixture
    ) -> None:
        """INFO stdout is truncated; full text remains on DEBUG / return value."""
        hook = DuckDbHook(duckdb_conn_id="duckdb_test", log_output_limit=10)
        hook.get_connection = duckdb_hook.get_connection  # type: ignore[method-assign]
        hook._preflight_binary = MagicMock()  # type: ignore[method-assign]
        hook._preflight_db_path = MagicMock()  # type: ignore[method-assign]
        long_out = "abcdefghijklmnop"
        mock_popen.return_value = _mock_process(stdout=long_out + "\n")

        with caplog.at_level("DEBUG"):
            result = hook.run_cli("SELECT 1")

        assert result == long_out
        assert "truncated" in caplog.text
        assert any(long_out in r.message for r in caplog.records if r.levelname == "DEBUG")

    @patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.killpg")
    @patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.getpgid", return_value=999)
    @patch(MOCK_POPEN)
    def test_run_cli_timeout_posix_group_kill(
        self,
        mock_popen: MagicMock,
        mock_getpgid: MagicMock,
        mock_killpg: MagicMock,
        duckdb_hook: DuckDbHook,
    ) -> None:
        """Timeout uses SIGTERM→SIGKILL via killpg then communicate (POSIX)."""
        proc = _mock_process(
            communicate_side_effect=[
                subprocess.TimeoutExpired(cmd=["duckdb"], timeout=300),
                ("", "still running"),
            ]
        )
        proc.poll.side_effect = [None, None, 0]
        proc.wait.side_effect = [subprocess.TimeoutExpired(cmd=["duckdb"], timeout=5), 0]
        mock_popen.return_value = proc

        with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.name", "posix"):
            with pytest.raises(DuckDbCliError, match="timed out after 300s") as exc_info:
                duckdb_hook.run_cli("SELECT 1")

        assert exc_info.value.retryable is False
        assert mock_killpg.call_count >= 1
        assert proc.communicate.call_count == 2

    @patch(MOCK_POPEN)
    def test_run_cli_timeout_non_posix_fallback(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Non-POSIX timeout uses terminate → kill → communicate.

        Patch ``os.name`` only around terminate: a process-wide ``nt`` break
        pathlib on macOS/Linux (WindowsPath cannot be instantiated).
        """
        proc = _mock_process(
            communicate_side_effect=[
                subprocess.TimeoutExpired(cmd=["duckdb"], timeout=300),
                ("", ""),
            ]
        )
        proc.poll.side_effect = [None, None, 0]
        proc.wait.side_effect = [subprocess.TimeoutExpired(cmd=["duckdb"], timeout=5), 0]
        mock_popen.return_value = proc

        original_terminate = duckdb_hook._terminate_process

        def terminate_as_non_posix(process: subprocess.Popen[str]) -> None:
            with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.name", "nt"):
                original_terminate(process)

        with patch.object(duckdb_hook, "_terminate_process", side_effect=terminate_as_non_posix):
            with pytest.raises(DuckDbCliError, match="timed out after 300s"):
                duckdb_hook.run_cli("SELECT 1")

        proc.terminate.assert_called()
        proc.kill.assert_called()
        assert proc.communicate.call_count == 2

    @patch(MOCK_POPEN)
    def test_run_cli_error(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook, caplog: pytest.LogCaptureFixture
    ) -> None:
        """CLI failure raises DuckDbCliError and logs stderr at ERROR."""
        mock_popen.return_value = _mock_process(stdout="", stderr="syntax error", returncode=1)

        with caplog.at_level("ERROR"):
            with pytest.raises(DuckDbCliError, match="syntax error") as exc_info:
                duckdb_hook.run_cli("BAD SQL")

        assert exc_info.value.returncode == 1
        assert "DuckDB stderr:" in caplog.text
        assert "syntax error" in caplog.text

    @patch(MOCK_POPEN)
    def test_run_cli_error_unknown_when_empty_stderr(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook
    ) -> None:
        """CLI failure without stderr uses Unknown error placeholder."""
        mock_popen.return_value = _mock_process(stdout="", stderr="", returncode=1)

        with pytest.raises(DuckDbCliError, match="Unknown error"):
            duckdb_hook.run_cli("BAD SQL")

    @patch(MOCK_POPEN)
    def test_run_cli_missing_binary(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Missing DuckDB binary at launch raises DuckDbCliError."""
        mock_popen.side_effect = FileNotFoundError("No such file")

        with pytest.raises(DuckDbCliError, match="Cannot launch DuckDB binary"):
            duckdb_hook.run_cli("SELECT 1")

    @patch(MOCK_POPEN)
    def test_run_cli_permission_error(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Non-executable binary raises DuckDbCliError, not raw PermissionError."""
        mock_popen.side_effect = PermissionError(13, "Permission denied")

        with pytest.raises(DuckDbCliError, match="Cannot launch DuckDB binary"):
            duckdb_hook.run_cli("SELECT 1")

    @patch(MOCK_POPEN)
    def test_run_file_success(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook, tmp_path: Path) -> None:
        """run_file executes SQL via -f flag when the file exists."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT 1;", encoding="utf-8")
        mock_popen.return_value = _mock_process(stdout="ok\n")

        result = duckdb_hook.run_file(str(sql_file))

        assert result == "ok"
        cmd = mock_popen.call_args.args[0]
        assert cmd[-2:] == ["-f", str(sql_file)]

    @patch(MOCK_POPEN)
    def test_database_override(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Operator-level database path overrides Connection host."""
        mock_popen.return_value = _mock_process(stdout="ok\n")

        duckdb_hook.run_cli("SELECT 1", database="/override.duckdb")

        cmd = mock_popen.call_args.args[0]
        assert cmd[1] == "/override.duckdb"
        duckdb_hook._preflight_db_path.assert_called_with("/override.duckdb", readonly=False)

    @patch(MOCK_POPEN)
    def test_database_empty_string_uses_connection(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook
    ) -> None:
        """Empty database override keeps Connection host (Jinja empty-string case)."""
        mock_popen.return_value = _mock_process(stdout="ok\n")

        duckdb_hook.run_cli("SELECT 1", database="")

        cmd = mock_popen.call_args.args[0]
        assert cmd[1] == "/tmp/test.duckdb"


class TestDuckDbLockRetry:
    """Lock-retry loop; default attempts=0."""

    def test_ctor_override_wins_over_extra(self, duckdb_hook: DuckDbHook) -> None:
        """Ctor lock_retry_attempts overrides connection extra."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "lock_retry_attempts": 5}
        hook = DuckDbHook(duckdb_conn_id="duckdb_test", lock_retry_attempts=2)
        hook.get_connection = lambda _: conn  # type: ignore[method-assign]
        hook._preflight_binary = MagicMock()  # type: ignore[method-assign]

        assert hook._get_conn_params()["lock_retry_attempts"] == 2

    def test_extra_lock_retry_attempts(self, duckdb_hook: DuckDbHook) -> None:
        """Connection extra supplies lock_retry_attempts when ctor is None."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "lock_retry_attempts": 3}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        assert duckdb_hook._get_conn_params()["lock_retry_attempts"] == 3

    def test_coerce_lock_retry_attempts_invalid(self) -> None:
        """Negative / non-int lock_retry_attempts raise ConfigurationError."""
        with pytest.raises(DuckDbConfigurationError, match="lock_retry_attempts"):
            DuckDbHook._coerce_lock_retry_attempts(-1)
        with pytest.raises(DuckDbConfigurationError, match="lock_retry_attempts"):
            DuckDbHook._coerce_lock_retry_attempts("abc")

    @patch(MOCK_POPEN)
    def test_lock_attempts_zero_clear_message_no_retry(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook
    ) -> None:
        """Default attempts=0: one launch, clear lock error, no sleep/retry."""
        mock_popen.return_value = _mock_process(stdout="", stderr=LOCK_STDERR, returncode=1)

        with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.time.sleep") as mock_sleep:
            with pytest.raises(DuckDbCliError, match="database file is locked") as exc_info:
                duckdb_hook.run_cli("SELECT 1")

        assert exc_info.value.retryable is True
        assert exc_info.value.stderr == LOCK_STDERR.strip()
        assert "duckdb_conn_id=duckdb_test" in str(exc_info.value)
        assert "db_path=/tmp/test.duckdb" in str(exc_info.value)
        assert mock_popen.call_count == 1
        mock_sleep.assert_not_called()

    @patch(MOCK_POPEN)
    def test_lock_retries_then_clear_error(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook, caplog: pytest.LogCaptureFixture
    ) -> None:
        """attempts=3: three launches with backoff, then clear lock error."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "lock_retry_attempts": 3}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]
        mock_popen.return_value = _mock_process(stdout="", stderr=LOCK_STDERR, returncode=1)

        with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.time.sleep") as mock_sleep:
            with caplog.at_level("WARNING"):
                with pytest.raises(DuckDbCliError, match="database file is locked"):
                    duckdb_hook.run_cli("SELECT 1")

        assert mock_popen.call_count == 3
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0].args[0] == LOCK_RETRY_BACKOFF_SECONDS[0]
        assert mock_sleep.call_args_list[1].args[0] == LOCK_RETRY_BACKOFF_SECONDS[1]
        assert "lock conflict on attempt" in caplog.text
        assert LOCK_STDERR.strip() in caplog.text

    @patch(MOCK_POPEN)
    def test_lock_retry_succeeds_on_later_attempt(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook
    ) -> None:
        """Lock on first launch, success on second."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "lock_retry_attempts": 3}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]
        mock_popen.side_effect = [
            _mock_process(stdout="", stderr=LOCK_STDERR, returncode=1),
            _mock_process(stdout='[{"ok":1}]\n'),
        ]

        with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.time.sleep"):
            result = duckdb_hook.run_cli("SELECT 1")

        assert result == '[{"ok":1}]'
        assert mock_popen.call_count == 2

    @patch(MOCK_POPEN)
    def test_non_lock_error_not_retried(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """Syntax/catalog errors are not retried even when attempts > 0."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "lock_retry_attempts": 5}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]
        mock_popen.return_value = _mock_process(stdout="", stderr="Parser Error: syntax error", returncode=1)

        with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.time.sleep") as mock_sleep:
            with pytest.raises(DuckDbCliError, match="syntax error") as exc_info:
                duckdb_hook.run_cli("BAD")

        assert exc_info.value.retryable is False
        assert "database file is locked" not in str(exc_info.value)
        assert mock_popen.call_count == 1
        mock_sleep.assert_not_called()

    @patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.killpg")
    @patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.getpgid", return_value=999)
    @patch(MOCK_POPEN)
    def test_timeout_not_retried(
        self,
        mock_popen: MagicMock,
        mock_getpgid: MagicMock,
        mock_killpg: MagicMock,
        duckdb_hook: DuckDbHook,
    ) -> None:
        """Timeout is never retried."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "lock_retry_attempts": 3}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]
        proc = _mock_process(
            communicate_side_effect=[
                subprocess.TimeoutExpired(cmd=["duckdb"], timeout=300),
                ("", ""),
            ]
        )
        proc.poll.side_effect = [None, None, 0]
        proc.wait.side_effect = [subprocess.TimeoutExpired(cmd=["duckdb"], timeout=5), 0]
        mock_popen.return_value = proc

        with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.name", "posix"):
            with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.time.sleep") as mock_sleep:
                with pytest.raises(DuckDbCliError, match="timed out"):
                    duckdb_hook.run_cli("SELECT 1")

        assert mock_popen.call_count == 1
        mock_sleep.assert_not_called()

    @patch(MOCK_POPEN)
    def test_killed_stops_retry_loop(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """_killed before a retry iteration raises clear killed message."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "lock_retry_attempts": 3}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]
        mock_popen.return_value = _mock_process(stdout="", stderr=LOCK_STDERR, returncode=1)

        def sleep_and_kill(_delay: float) -> None:
            duckdb_hook._killed = True

        with patch(
            "airflow.providers.arenadata.duckdb.hooks.duckdb.time.sleep",
            side_effect=sleep_and_kill,
        ):
            with pytest.raises(DuckDbCliError, match="task was killed"):
                duckdb_hook.run_cli("SELECT 1")

        assert mock_popen.call_count == 1

    @patch(MOCK_POPEN)
    def test_temp_sql_file_reused_across_retries(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook
    ) -> None:
        """Temp SQL path is created once outside the retry loop."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "lock_retry_attempts": 2}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]
        mock_popen.return_value = _mock_process(stdout="", stderr=LOCK_STDERR, returncode=1)
        sql_files: list[str] = []

        def capture(cmd: list[str], **kwargs: object) -> MagicMock:
            sql_files.append(cmd[cmd.index("-f") + 1])
            return mock_popen.return_value

        mock_popen.side_effect = capture

        with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.time.sleep"):
            with pytest.raises(DuckDbCliError, match="database file is locked"):
                duckdb_hook.run_cli("SELECT 1")

        assert len(sql_files) == 2
        assert sql_files[0] == sql_files[1]


class TestDuckDbHookOnKill:
    """Tests for on_kill / _killed messaging."""

    @patch(MOCK_POPEN)
    def test_killed_before_run_raises_clear_message(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook
    ) -> None:
        """If on_kill ran before _run, raise clear message (not exit -15)."""
        duckdb_hook.on_kill()
        with pytest.raises(DuckDbCliError, match="task was killed"):
            duckdb_hook.run_cli("SELECT 1")
        mock_popen.assert_not_called()

    @patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.killpg")
    @patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.getpgid", return_value=999)
    @patch(MOCK_POPEN)
    def test_killed_after_popen_raises_clear_message(
        self,
        mock_popen: MagicMock,
        mock_getpgid: MagicMock,
        mock_killpg: MagicMock,
        duckdb_hook: DuckDbHook,
    ) -> None:
        """Re-check after Popen: if _killed, terminate and raise clear message."""
        proc = _mock_process(stdout="", stderr="")
        proc.poll.return_value = None
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        def set_killed(*args: object, **kwargs: object) -> MagicMock:
            duckdb_hook._killed = True
            return proc

        mock_popen.side_effect = set_killed

        with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.name", "posix"):
            with pytest.raises(DuckDbCliError, match="task was killed") as exc_info:
                duckdb_hook.run_cli("SELECT 1")

        assert "-15" not in str(exc_info.value)
        mock_killpg.assert_called()


class TestDuckDbHookTestConnection:
    """Test DuckDB hook test_connection helper (shared params path + _run)."""

    @patch(MOCK_POPEN)
    def test_test_connection_success(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """test_connection succeeds when binary is executable."""
        mock_popen.return_value = _mock_process(stdout="1\n")

        ok, message = duckdb_hook.test_connection()

        assert ok is True
        assert "accessible" in message
        assert "/usr/bin/duckdb" in message
        mock_popen.assert_called_once()
        call_kwargs = mock_popen.call_args.kwargs
        assert call_kwargs["stdin"] is subprocess.DEVNULL
        assert call_kwargs["start_new_session"] is True
        cmd = mock_popen.call_args.args[0]
        assert cmd[1] == ":memory:"
        assert "-c" in cmd
        assert "SELECT 1" in cmd
        # db-path preflight skipped for the probe; binary preflight still runs.
        duckdb_hook._preflight_db_path.assert_not_called()
        duckdb_hook._preflight_binary.assert_called()

    @patch(MOCK_POPEN)
    def test_test_connection_binary_not_found(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """test_connection reports missing binary without raising."""
        mock_popen.side_effect = FileNotFoundError("No such file")

        ok, message = duckdb_hook.test_connection()

        assert ok is False
        assert "Cannot launch DuckDB binary" in message
        assert "/usr/bin/duckdb" in message

    @patch(MOCK_POPEN)
    def test_test_connection_cli_error(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """test_connection reports non-zero CLI exit code."""
        mock_popen.return_value = _mock_process(stdout="", stderr="permission denied", returncode=1)

        ok, message = duckdb_hook.test_connection()

        assert ok is False
        assert "exit code 1" in message
        assert "permission denied" in message

    @patch(MOCK_POPEN)
    def test_test_connection_banned_cli_params(self, mock_popen: MagicMock, duckdb_hook: DuckDbHook) -> None:
        """test_connection uses the same ban path as run_cli."""
        conn = MagicMock()
        conn.host = "/tmp/test.duckdb"
        conn.extra_dejson = {"duckdb_binary": "/usr/bin/duckdb", "cli_params": "-init /tmp/x"}
        duckdb_hook.get_connection = lambda _: conn  # type: ignore[method-assign]

        ok, message = duckdb_hook.test_connection()

        assert ok is False
        assert "banned flag" in message
        mock_popen.assert_not_called()

    @patch(MOCK_POPEN)
    def test_test_connection_preflight_binary_failure(
        self, mock_popen: MagicMock, duckdb_hook: DuckDbHook
    ) -> None:
        """Binary preflight failure is returned as (False, message)."""
        duckdb_hook._preflight_binary = MagicMock(  # type: ignore[method-assign]
            side_effect=DuckDbConfigurationError("DuckDB binary not found or not executable: /bad")
        )

        ok, message = duckdb_hook.test_connection()

        assert ok is False
        assert "not found or not executable" in message
        mock_popen.assert_not_called()

    @patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.killpg")
    @patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.getpgid", return_value=999)
    @patch(MOCK_POPEN)
    def test_test_connection_timeout_uses_kill_path(
        self,
        mock_popen: MagicMock,
        mock_getpgid: MagicMock,
        mock_killpg: MagicMock,
        duckdb_hook: DuckDbHook,
    ) -> None:
        """test_connection timeout uses the same kill-path as run_cli."""
        proc = _mock_process(
            communicate_side_effect=[
                subprocess.TimeoutExpired(cmd=["duckdb"], timeout=TEST_CONNECTION_TIMEOUT),
                ("", ""),
            ]
        )
        proc.poll.side_effect = [None, None, 0]
        proc.wait.side_effect = [subprocess.TimeoutExpired(cmd=["duckdb"], timeout=5), 0]
        mock_popen.return_value = proc

        with patch("airflow.providers.arenadata.duckdb.hooks.duckdb.os.name", "posix"):
            ok, message = duckdb_hook.test_connection()

        assert ok is False
        assert f"timed out after {TEST_CONNECTION_TIMEOUT}s" in message
        assert mock_killpg.call_count >= 1
        assert proc.communicate.call_count == 2
