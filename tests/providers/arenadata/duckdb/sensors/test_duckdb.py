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

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.arenadata.duckdb.sensors.duckdb import DuckDbSqlSensor

MOCK_HOOK = "airflow.providers.arenadata.duckdb.sensors.duckdb.DuckDbHook"


class TestDuckDbSqlSensor:
    """Test DuckDbSqlSensor."""

    def test_template_fields(self) -> None:
        """Template fields cover SQL, database path, and connection id."""
        assert DuckDbSqlSensor.template_fields == ("sql", "database", "duckdb_conn_id")
        assert DuckDbSqlSensor.template_ext == (".sql",)

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
    def test_poke_invalid_json_returns_false(
        self, mock_hook_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Invalid JSON output is retried after logging a warning."""
        mock_hook_cls.return_value.run_cli.return_value = "not-json"
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        with caplog.at_level("WARNING", logger=DuckDbSqlSensor.__module__):
            assert sensor.poke({}) is False

        assert "failed to parse JSON output" in caplog.text

    @patch(MOCK_HOOK)
    def test_poke_non_list_json_returns_false(self, mock_hook_cls: MagicMock) -> None:
        """Non-list JSON payload is treated as empty result."""
        mock_hook_cls.return_value.run_cli.return_value = '{"c": 1}'
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        assert sensor.poke({}) is False

    @patch(MOCK_HOOK)
    def test_poke_calls_hook_with_json_format(self, mock_hook_cls: MagicMock) -> None:
        """Sensor always requests JSON output from the hook."""
        mock_hook_cls.return_value.run_cli.return_value = '[{"c": 1}]'
        sensor = DuckDbSqlSensor(
            task_id="test_sensor",
            sql="SELECT count(*) AS c FROM t",
            duckdb_conn_id="duckdb_test",
        )

        sensor.poke({})

        mock_hook_cls.assert_called_once_with(duckdb_conn_id="duckdb_test")
        mock_hook_cls.return_value.run_cli.assert_called_once_with(
            "SELECT count(*) AS c FROM t",
            output_format="json",
            database=None,
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
        mock_hook_cls.return_value.run_cli.side_effect = AirflowException("CLI failed")
        sensor = DuckDbSqlSensor(task_id="test_sensor", sql="SELECT 1")

        with pytest.raises(AirflowException, match="CLI failed"):
            sensor.poke({})

    @pytest.mark.parametrize(
        ("soft_fail", "expected_exception"),
        [
            (False, AirflowException),
            (True, AirflowSkipException),
        ],
    )
    @patch(MOCK_HOOK)
    def test_poke_fail_on_empty_soft_fail(
        self,
        mock_hook_cls: MagicMock,
        soft_fail: bool,
        expected_exception: type[AirflowException],
    ) -> None:
        """fail_on_empty respects soft_fail (same contract as SqlSensor)."""
        mock_hook_cls.return_value.run_cli.return_value = "[]"
        sensor = DuckDbSqlSensor(
            task_id="test_sensor",
            sql="SELECT 1",
            fail_on_empty=True,
            soft_fail=soft_fail,
        )

        with pytest.raises(expected_exception, match="query returned no rows"):
            sensor.poke({})

    def test_empty_sql_raises_value_error(self) -> None:
        """Empty SQL is rejected at construction time."""
        with pytest.raises(ValueError, match="sql cannot be empty"):
            DuckDbSqlSensor(task_id="test_sensor", sql="")
