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

import os
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from airflow.providers.arenadata.duckdb.hooks.duckdb import DuckDbHook
from airflow.providers.arenadata.duckdb.utils.errors import DuckDbCliError, DuckDbConfigurationError
from airflow.providers.arenadata.duckdb.utils.json_output import parse_json_output

DUCKDB_BINARY = "/files/bin/duckdb"
DuckDbConnFactory = Callable[..., tuple[str, Path]]


def _blocking_read_sql(fifo_path: Path) -> str:
    """SQL that blocks until someone writes to the FIFO (or the CLI is killed/timed out)."""
    # explicit columns avoid auto-detect finishing immediately on an empty FIFO open race
    return f"SELECT * FROM read_csv('{fifo_path}', header = false, columns = {{'x': 'VARCHAR'}});"


def _hold_db_lock(db_path: Path) -> subprocess.Popen[str]:
    """Open an interactive DuckDB CLI session that keeps a write lock on *db_path*."""
    writer = subprocess.Popen(
        [DUCKDB_BINARY, str(db_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if writer.stdin is None:
        writer.kill()
        raise RuntimeError("DuckDB writer stdin is not available")
    # force the process to open the database file before the hook races it
    writer.stdin.write("SELECT 1;\n")
    writer.stdin.flush()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if writer.poll() is not None:
            stderr = (writer.stderr.read() if writer.stderr else "") or ""
            raise RuntimeError(f"DuckDB writer exited early (code={writer.returncode}): {stderr}")
        # file exists and writer is alive - lock should be held after first statement
        if db_path.exists():
            time.sleep(0.2)
            return writer
        time.sleep(0.05)
    writer.kill()
    raise RuntimeError("Timed out waiting for DuckDB writer to open the database")


def _release_db_lock(writer: subprocess.Popen[str]) -> None:
    try:
        if writer.stdin and not writer.stdin.closed:
            writer.stdin.write(".quit\n")
            writer.stdin.flush()
            writer.stdin.close()
        writer.wait(timeout=15)
    except Exception:
        writer.kill()
        writer.wait(timeout=5)


@pytest.mark.integration("duckdb")
class TestDuckDbHookIntegration:
    def test_create_insert_select_roundtrip(self, duckdb_conn_id: str) -> None:
        hook = DuckDbHook(duckdb_conn_id=duckdb_conn_id)

        hook.run_cli(
            "CREATE TABLE t (id INTEGER, name VARCHAR);INSERT INTO t VALUES (1, 'alice');",
            output_format=None,
        )
        raw = hook.run_cli("SELECT id, name FROM t ORDER BY id;", output_format="json")
        rows = parse_json_output(raw)

        assert rows == [{"id": 1, "name": "alice"}]

    def test_lock_conflict_without_retry(self, duckdb_conn_factory: DuckDbConnFactory) -> None:
        """Writer holds the file lock; hook with default (no retry) raises a clear lock error."""
        conn_id, db_path = duckdb_conn_factory()
        DuckDbHook(duckdb_conn_id=conn_id).run_cli(
            "CREATE TABLE t (id INTEGER);",
            output_format=None,
        )

        writer = _hold_db_lock(db_path)
        try:
            with pytest.raises(DuckDbCliError, match="locked by another process") as exc_info:
                DuckDbHook(duckdb_conn_id=conn_id).run_cli("SELECT 1;", output_format=None)
            assert exc_info.value.retryable is True
        finally:
            _release_db_lock(writer)

    def test_lock_retry_succeeds_after_writer_releases(self, duckdb_conn_factory: DuckDbConnFactory) -> None:
        """lock_retry_attempts > 0 retries until the writer releases the file."""
        conn_id, db_path = duckdb_conn_factory(lock_retry_attempts=5, timeout=30)
        DuckDbHook(duckdb_conn_id=conn_id).run_cli(
            "CREATE TABLE t (id INTEGER);",
            output_format=None,
        )

        writer = _hold_db_lock(db_path)

        def _release_soon() -> None:
            time.sleep(1.5)
            _release_db_lock(writer)

        releaser = threading.Thread(target=_release_soon, daemon=True)
        releaser.start()
        try:
            raw = DuckDbHook(duckdb_conn_id=conn_id).run_cli(
                "SELECT 1 AS ok;",
                output_format="json",
            )
            assert parse_json_output(raw) == [{"ok": 1}]
        finally:
            releaser.join(timeout=20)
            if writer.poll() is None:
                _release_db_lock(writer)

    def test_cli_timeout(self, duckdb_conn_factory: DuckDbConnFactory, tmp_path: Path) -> None:
        """extra.timeout cuts off a blocked query with DuckDbCliError."""
        conn_id, _ = duckdb_conn_factory(timeout=2)
        DuckDbHook(duckdb_conn_id=conn_id).run_cli("SELECT 1;", output_format=None)

        fifo = tmp_path / "block_timeout.fifo"
        os.mkfifo(fifo)

        with pytest.raises(DuckDbCliError, match="timed out after 2s"):
            DuckDbHook(duckdb_conn_id=conn_id).run_cli(_blocking_read_sql(fifo), output_format=None)

    def test_on_kill_terminates_running_cli(
        self, duckdb_conn_factory: DuckDbConnFactory, tmp_path: Path
    ) -> None:
        """on_kill terminates an in-flight CLI process blocked on I/O."""
        conn_id, _ = duckdb_conn_factory(timeout=60)
        DuckDbHook(duckdb_conn_id=conn_id).run_cli("SELECT 1;", output_format=None)

        fifo = tmp_path / "block_kill.fifo"
        os.mkfifo(fifo)

        hook = DuckDbHook(duckdb_conn_id=conn_id)
        errors: list[BaseException] = []

        def _run() -> None:
            try:
                hook.run_cli(_blocking_read_sql(fifo), output_format=None)
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=_run)
        worker.start()
        deadline = time.monotonic() + 15
        while hook._process is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert hook._process is not None, "CLI process did not start in time"
        # Give the child a moment to block on the FIFO read
        time.sleep(0.3)

        hook.on_kill()
        worker.join(timeout=30)
        assert not worker.is_alive()
        assert errors
        assert isinstance(errors[0], DuckDbCliError)
        assert "killed" in str(errors[0]).lower() or errors[0].returncode not in (0, None)

    def test_readonly_select_and_write_rejected(self, duckdb_conn_factory: DuckDbConnFactory) -> None:
        """Create DB writable, then readonly connection can SELECT but not INSERT."""
        write_conn_id, db_path = duckdb_conn_factory()
        DuckDbHook(duckdb_conn_id=write_conn_id).run_cli(
            "CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);",
            output_format=None,
        )

        ro_conn_id, _ = duckdb_conn_factory(readonly=True)
        # point readonly connection at the same file (override host via factory host is unique -
        # use database= override on run_cli instead)
        ro_hook = DuckDbHook(duckdb_conn_id=ro_conn_id)
        rows = parse_json_output(
            ro_hook.run_cli("SELECT id FROM t;", output_format="json", database=str(db_path))
        )
        assert rows == [{"id": 1}]

        with pytest.raises(DuckDbCliError):
            ro_hook.run_cli("INSERT INTO t VALUES (2);", output_format=None, database=str(db_path))

    def test_readonly_missing_file_preflight(self, duckdb_conn_factory: DuckDbConnFactory) -> None:
        """Readonly on a missing .duckdb file fails in preflight (not at CLI)."""
        conn_id, db_path = duckdb_conn_factory(readonly=True)
        assert not db_path.exists()

        with pytest.raises(DuckDbConfigurationError, match="does not exist for readonly"):
            DuckDbHook(duckdb_conn_id=conn_id).run_cli("SELECT 1;", output_format=None)

    def test_bail_stops_after_error(self, duckdb_conn_id: str) -> None:
        """-bail: statement after an error is not executed."""
        hook = DuckDbHook(duckdb_conn_id=duckdb_conn_id)
        hook.run_cli("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);", output_format=None)

        with pytest.raises(DuckDbCliError):
            hook.run_cli(
                "INSERT INTO t VALUES (2); SELECT * FROM no_such_table; INSERT INTO t VALUES (3);",
                output_format=None,
            )

        rows = parse_json_output(hook.run_cli("SELECT id FROM t ORDER BY id;", output_format="json"))
        assert rows == [{"id": 1}, {"id": 2}]

    def test_json_and_csv_output(self, duckdb_conn_id: str) -> None:
        """Real CLI -json / -csv output is returned and JSON parses to rows."""
        hook = DuckDbHook(duckdb_conn_id=duckdb_conn_id)
        hook.run_cli(
            "CREATE TABLE t (id INTEGER, name VARCHAR); INSERT INTO t VALUES (1, 'alice');",
            output_format=None,
        )

        json_raw = hook.run_cli("SELECT id, name FROM t;", output_format="json")
        assert parse_json_output(json_raw) == [{"id": 1, "name": "alice"}]

        csv_raw = hook.run_cli("SELECT id, name FROM t;", output_format="csv")
        assert "alice" in csv_raw
        assert "1" in csv_raw

    def test_run_file_executes_sql_on_disk(self, duckdb_conn_id: str, tmp_path: Path) -> None:
        """run_file executes a real .sql file from disk."""
        sql_path = tmp_path / "script.sql"
        sql_path.write_text(
            "CREATE TABLE t (id INTEGER, name VARCHAR);\n"
            "INSERT INTO t VALUES (7, 'file');\n"
            "SELECT id, name FROM t;\n",
            encoding="utf-8",
        )

        raw = DuckDbHook(duckdb_conn_id=duckdb_conn_id).run_file(str(sql_path), output_format="json")
        assert parse_json_output(raw) == [{"id": 7, "name": "file"}]

    def test_bind_parameters_unicode_and_quotes(self, duckdb_conn_id: str) -> None:
        """%(name)s binding with unicode and embedded quotes on a live DB."""
        hook = DuckDbHook(duckdb_conn_id=duckdb_conn_id)
        hook.run_cli("CREATE TABLE t (name VARCHAR);", output_format=None)
        hook.run_cli(
            "INSERT INTO t VALUES (%(name)s);",
            output_format=None,
            parameters={"name": "auto café Brian O'Conner"},
        )

        rows = parse_json_output(hook.run_cli("SELECT name FROM t;", output_format="json"))
        assert rows == [{"name": "auto café Brian O'Conner"}]
