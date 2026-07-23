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
"""Tests for DuckDB operators."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.arenadata.duckdb.operators.duckdb import DuckDbOperator

MOCK_HOOK = "airflow.providers.arenadata.duckdb.operators.duckdb.DuckDbHook"


class TestDuckDbOperator:
    """Test DuckDbOperator."""

    def test_template_fields(self) -> None:
        """Template fields cover SQL, database path, and connection id."""
        assert DuckDbOperator.template_fields == ("sql", "database", "duckdb_conn_id")
        assert DuckDbOperator.template_ext == (".sql",)

    @patch(MOCK_HOOK)
    def test_execute_calls_hook(self, mock_hook_cls: MagicMock) -> None:
        """execute() delegates SQL execution to DuckDbHook.run_cli."""
        mock_hook_cls.return_value.run_cli.return_value = '[{"c": 1}]'
        operator = DuckDbOperator(
            task_id="test_op",
            sql="SELECT 1",
            duckdb_conn_id="duckdb_test",
        )

        operator.execute({})

        mock_hook_cls.assert_called_once_with(duckdb_conn_id="duckdb_test")
        mock_hook_cls.return_value.run_cli.assert_called_once_with(
            "SELECT 1",
            output_format="json",
            database=None,
        )

    @patch(MOCK_HOOK)
    def test_execute_uses_run_cli_not_run_file(self, mock_hook_cls: MagicMock) -> None:
        """Operator executes inline SQL via run_cli."""
        mock_hook_cls.return_value.run_cli.return_value = "ok"
        operator = DuckDbOperator(task_id="test_op", sql="SELECT 1")

        operator.execute({})

        mock_hook_cls.return_value.run_cli.assert_called_once()
        mock_hook_cls.return_value.run_file.assert_not_called()

    @patch(MOCK_HOOK)
    def test_database_override_passed_to_hook(self, mock_hook_cls: MagicMock) -> None:
        """Explicit database path is forwarded to the hook."""
        mock_hook_cls.return_value.run_cli.return_value = "ok"
        operator = DuckDbOperator(
            task_id="test_op",
            sql="SELECT 1",
            database="/tmp/override.duckdb",
        )

        operator.execute({})

        assert mock_hook_cls.return_value.run_cli.call_args.kwargs["database"] == "/tmp/override.duckdb"

    @patch(MOCK_HOOK)
    def test_output_format_passed_to_hook(self, mock_hook_cls: MagicMock) -> None:
        """output_format is forwarded to the hook."""
        mock_hook_cls.return_value.run_cli.return_value = "1"
        operator = DuckDbOperator(
            task_id="test_op",
            sql="SELECT 1",
            output_format="csv",
        )

        operator.execute({})

        assert mock_hook_cls.return_value.run_cli.call_args.kwargs["output_format"] == "csv"

    @patch(MOCK_HOOK)
    def test_execute_returns_stdout_for_xcom(self, mock_hook_cls: MagicMock) -> None:
        """execute() returns raw CLI stdout for XCom."""
        mock_hook_cls.return_value.run_cli.return_value = '[{"c": 5}]'
        operator = DuckDbOperator(task_id="test_op", sql="SELECT count(*) AS c FROM t")

        result = operator.execute({})

        assert result == '[{"c": 5}]'

    def test_empty_sql_raises_value_error(self) -> None:
        """Empty SQL is rejected at construction time."""
        with pytest.raises(ValueError, match="sql cannot be empty"):
            DuckDbOperator(task_id="test_op", sql="")

    def test_render_template_fields_sql(self) -> None:
        """Jinja templates in SQL are rendered before execution."""
        operator = DuckDbOperator(
            task_id="test_op",
            sql="SELECT * FROM {{ params.table }}",
        )

        operator.render_template_fields({"params": {"table": "demo"}})

        assert operator.sql == "SELECT * FROM demo"
