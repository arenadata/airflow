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
"""DuckDB sensors."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.arenadata.duckdb.hooks.duckdb import DuckDbHook
from airflow.providers.arenadata.duckdb.version_compat import (
    AirflowException,
    AirflowSkipException,
    BaseSensorOperator,
)

if TYPE_CHECKING:
    from airflow.providers.arenadata.duckdb.version_compat import Context


class DuckDbSqlSensor(BaseSensorOperator):  # pylint: disable=too-few-public-methods
    """
    Wait until a DuckDB SQL query returns a truthy result.

    Evaluates the first cell of the first returned row. Continues waiting when
    the value is false, zero, null, empty string, or the result set is empty.

    :param sql: SQL statement or path to a ``.sql`` template file.
    :param duckdb_conn_id: Airflow connection ID for DuckDB.
    :param database: Optional path to ``.duckdb`` file; overrides Connection ``host``.
    :param parameters: optional ``%(name)s`` placeholders bound into *sql*
        after Jinja templating.
    :param fail_on_empty: If True, fail when the query returns no rows.
    """

    template_fields: Sequence[str] = ("sql", "parameters", "database", "duckdb_conn_id")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql", "parameters": "json"}
    ui_color = "#7c7287"

    def __init__(
        self,
        sql: str,
        duckdb_conn_id: str = DuckDbHook.default_conn_name,
        *,
        database: str | None = None,
        parameters: Mapping[str, Any] | None = None,
        fail_on_empty: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not sql:
            raise ValueError("sql cannot be empty")
        self.sql = sql
        self.duckdb_conn_id = duckdb_conn_id
        self.database = database
        self.parameters = dict(parameters) if parameters is not None else None
        self.fail_on_empty = fail_on_empty

    def poke(self, context: Context) -> bool:
        """Return True when the SQL query returns a truthy first cell."""
        hook = DuckDbHook(duckdb_conn_id=self.duckdb_conn_id)
        raw = hook.run_cli(
            self.sql,
            output_format="json",
            database=self.database,
            parameters=self.parameters,
        )

        try:
            rows = json.loads(raw) if raw else []
        except json.JSONDecodeError:
            self.log.warning("DuckDB sensor: failed to parse JSON output, will retry")
            return False

        if not isinstance(rows, list) or not rows:
            if self.fail_on_empty:
                message = "DuckDB sensor: query returned no rows"
                if self.soft_fail:
                    raise AirflowSkipException(message)
                raise AirflowException(message)
            return False

        first_row = rows[0]
        if not isinstance(first_row, dict) or not first_row:
            return False

        first_value = next(iter(first_row.values()))
        return bool(first_value)
