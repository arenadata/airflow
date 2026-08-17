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
"""Tests for DuckDB sensors."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.arenadata.duckdb.sensors.duckdb import DuckDbSqlSensor
from airflow.providers.arenadata.duckdb.utils.errors import DuckDbCliError, DuckDbOutputError
from airflow.providers.arenadata.duckdb.version_compat import AirflowException, AirflowFailException

MOCK_HOOK = "airflow.providers.arenadata.duckdb.sensors.duckdb.DuckDbHook"


class TestDuckDbSqlSensor:
    """Test DuckDbSqlSensor."""

    def test_template_fields(self) -> None:
        """Template fields cover SQL, parameters, database path, and connection id."""
        assert DuckDbSqlSensor.template_fields == (
            "sql",
            "parameters",
            "database",
            "duckdb_conn_id",
        )
        assert "lock_retry_attempts" not in DuckDbSqlSensor.template_fields
        assert "log_output_limit" not in DuckDbSqlSensor.template_fields
        assert DuckDbSqlSensor.template_ext == (".sql",)
        assert DuckDbSqlSensor.template_fields_renderers == {
            "sql": "sql",
            "parameters": "json",
        }

    @pytest.mark.parametrize(
        ("stdout", "expected"),
        [
            ('[{"c": 5}]', True),
            ('[{"c": 0}]', False),
            ('[{"c": null}]', False),
            ('[{"c": false}]', False),
            ('[{"c": ""}]', False),
        ],
    )
    @patch(MOCK_HOOK)
    def test_poke_truthy_falsy(self, mock_hook_cls: MagicMock, stdout: str, expected: bool) -> None:
        """Sensor evaluates the first cell using Python truthiness."""
        mock_hook_cls.return_value.run_cli.return_value = stdout
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        assert sensor.poke({}) is expected

    @patch(MOCK_HOOK)
    def test_poke_string_zero_is_truthy(self, mock_hook_cls: MagicMock) -> None:
        """String zero is truthy under Python semantics."""
        mock_hook_cls.return_value.run_cli.return_value = '[{"c": "0"}]'
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        assert sensor.poke({}) is True

    @patch(MOCK_HOOK)
    def test_poke_first_column_of_multicol_row(self, mock_hook_cls: MagicMock) -> None:
        """Only the first cell of the first row is evaluated."""
        mock_hook_cls.return_value.run_cli.return_value = '[{"a": 0, "b": 1}]'
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        assert sensor.poke({}) is False

    @patch(MOCK_HOOK)
    def test_poke_empty_first_row(self, mock_hook_cls: MagicMock) -> None:
        """Empty row dict is treated as no usable result."""
        mock_hook_cls.return_value.run_cli.return_value = "[{}]"
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        assert sensor.poke({}) is False

    @patch(MOCK_HOOK)
    def test_poke_empty_result(self, mock_hook_cls: MagicMock) -> None:
        """Empty result set keeps the sensor waiting."""
        mock_hook_cls.return_value.run_cli.return_value = "[]"
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        assert sensor.poke({}) is False

    @patch(MOCK_HOOK)
    def test_poke_empty_stdout_waits(self, mock_hook_cls: MagicMock) -> None:
        """Empty CLI stdout is treated as empty set (wait), not OutputError."""
        mock_hook_cls.return_value.run_cli.return_value = ""
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        assert sensor.poke({}) is False

    @patch(MOCK_HOOK)
    def test_poke_invalid_json_raises_output_error(self, mock_hook_cls: MagicMock) -> None:
        """Invalid JSON hard-fails with DuckDbOutputError (not wait)."""
        mock_hook_cls.return_value.run_cli.return_value = "not-json"
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        with pytest.raises(DuckDbOutputError, match="Failed to parse JSON"):
            sensor.poke({})

    @patch(MOCK_HOOK)
    def test_poke_non_list_json_raises_output_error(self, mock_hook_cls: MagicMock) -> None:
        """Non-list JSON payload hard-fails with DuckDbOutputError."""
        mock_hook_cls.return_value.run_cli.return_value = '{"c": 1}'
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        with pytest.raises(DuckDbOutputError, match="must be a list"):
            sensor.poke({})

    @patch(MOCK_HOOK)
    def test_poke_salvages_json_after_prefix(self, mock_hook_cls: MagicMock) -> None:
        """JSON array after log noise is salvaged and evaluated."""
        mock_hook_cls.return_value.run_cli.return_value = 'noise\n[{"c": 1}]'
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        assert sensor.poke({}) is True

    @patch(MOCK_HOOK)
    def test_poke_calls_hook_with_lock_retry_zero(self, mock_hook_cls: MagicMock) -> None:
        """Sensor always creates the hook with lock_retry_attempts=0."""
        mock_hook_cls.return_value.run_cli.return_value = '[{"c": 1}]'
        sensor = DuckDbSqlSensor(
            task_id="test_sensor",
            sql="SELECT count(*) AS c FROM t",
            duckdb_conn_id="duckdb_test",
            log_output_limit=500,
        )

        sensor.poke({})

        mock_hook_cls.assert_called_once_with(
            duckdb_conn_id="duckdb_test",
            lock_retry_attempts=0,
            log_output_limit=500,
        )
        mock_hook_cls.return_value.run_cli.assert_called_once_with(
            "SELECT count(*) AS c FROM t",
            output_format="json",
            database=None,
            parameters=None,
        )

    @patch(MOCK_HOOK)
    def test_poke_database_override_passed_to_hook(self, mock_hook_cls: MagicMock) -> None:
        """Explicit database path is forwarded to the hook."""
        mock_hook_cls.return_value.run_cli.return_value = '[{"c": 1}]'
        sensor = DuckDbSqlSensor(
            task_id="test_sensor",
            sql="SELECT 1",
            database="/tmp/override.duckdb",
        )
        sensor.poke({})
        assert mock_hook_cls.return_value.run_cli.call_args.kwargs["database"] == "/tmp/override.duckdb"

    @patch(MOCK_HOOK)
    def test_poke_database_defaults_to_none(self, mock_hook_cls: MagicMock) -> None:
        """Without database override the sensor targets the connection host."""
        mock_hook_cls.return_value.run_cli.return_value = '[{"c": 1}]'
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")
        sensor.poke({})
        assert mock_hook_cls.return_value.run_cli.call_args.kwargs["database"] is None

    @patch(MOCK_HOOK)
    def test_poke_hook_error_propagates(self, mock_hook_cls: MagicMock) -> None:
        """CLI execution errors from the hook fail the sensor poke."""
        mock_hook_cls.return_value.run_cli.side_effect = DuckDbCliError("CLI failed")
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        with pytest.raises(AirflowException, match="CLI failed"):
            sensor.poke({})

    @pytest.mark.parametrize("soft_fail", [False, True])
    @patch(MOCK_HOOK)
    def test_poke_fail_on_empty_raises_fail_exception(
        self,
        mock_hook_cls: MagicMock,
        soft_fail: bool,
    ) -> None:
        """fail_on_empty raises AirflowFailException from poke."""
        mock_hook_cls.return_value.run_cli.return_value = "[]"
        sensor = DuckDbSqlSensor(
            task_id="test_sensor",
            sql="SELECT 1",
            fail_on_empty=True,
            soft_fail=soft_fail,
        )

        with pytest.raises(AirflowFailException, match="query returned no rows"):
            sensor.poke({})

    @patch(MOCK_HOOK)
    def test_poke_forwards_parameters(self, mock_hook_cls: MagicMock) -> None:
        """parameters are forwarded to DuckDbHook.run_cli."""
        mock_hook_cls.return_value.run_cli.return_value = '[{"c": 1}]'
        sensor = DuckDbSqlSensor(
            task_id="test_sensor",
            sql="SELECT %(flag)s AS c",
            parameters={"flag": 1},
        )

        sensor.poke({})

        assert mock_hook_cls.return_value.run_cli.call_args.kwargs["parameters"] == {"flag": 1}

    def test_empty_sql_raises_value_error(self) -> None:
        """Empty SQL is rejected at construction time."""
        with pytest.raises(ValueError, match="sql cannot be empty"):
            DuckDbSqlSensor(task_id="test_sensor", sql="")

    def test_on_kill_safe_when_hook_is_none(self) -> None:
        """on_kill before poke must not raise."""
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")
        sensor.on_kill()

    @patch(MOCK_HOOK)
    def test_on_kill_delegates_to_hook(self, mock_hook_cls: MagicMock) -> None:
        """on_kill after poke terminates the hook process."""
        mock_hook_cls.return_value.run_cli.return_value = '[{"c": 1}]'
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")
        sensor.poke({})

        sensor.on_kill()

        mock_hook_cls.return_value.on_kill.assert_called_once()
