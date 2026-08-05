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

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.arenadata.duckdb.hooks.duckdb import DuckDbHook
from airflow.providers.arenadata.duckdb.utils.json_output import parse_json_output
from airflow.providers.arenadata.duckdb.version_compat import AirflowFailException, BaseSensorOperator

if TYPE_CHECKING:
    from airflow.providers.arenadata.duckdb.version_compat import Context


class DuckDbSqlSensor(BaseSensorOperator):  # pylint: disable=too-few-public-methods
    """
    Wait until a DuckDB SQL query returns a truthy result.

    Evaluates the first cell of the first returned row. Continues waiting when
    the value is false, zero, null, empty string, or the result set is empty
    (including empty CLI stdout).

    CLI and JSON contract errors fail the task. With ``fail_on_empty=True``, an
    empty result raises ``AirflowFailException``. Lock retries are not used
    in-poke: the hook is always created with ``lock_retry_attempts=0``
    (waiting is via ``poke_interval`` / reschedule).

    :param sql: SQL statement or path to a ``.sql`` template file.
    :param duckdb_conn_id: Airflow connection ID for DuckDB.
    :param database: Optional path to ``.duckdb`` file; overrides Connection ``host``.
    :param parameters: optional ``%(name)s`` placeholders bound into *sql*
        after Jinja templating.
    :param fail_on_empty: If True, fail (``AirflowFailException``) when the query
        returns no rows - no task retries for "no data yet".
    :param log_output_limit: Max chars of stdout/stderr on INFO logs
        (``None`` = hook module default). Not templated.
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
        log_output_limit: int | None = None,
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
        self.log_output_limit = log_output_limit
        self._hook: DuckDbHook | None = None

    def poke(self, context: Context) -> bool:
        """Return True when the SQL query returns a truthy first cell."""
        self._hook = DuckDbHook(
            duckdb_conn_id=self.duckdb_conn_id,
            lock_retry_attempts=0,
            log_output_limit=self.log_output_limit,
        )
        raw = self._hook.run_cli(
            self.sql,
            output_format="json",
            database=self.database,
            parameters=self.parameters,
        )

        # Empty stdout = empty result set - wait (or fail_on_empty).
        if not (raw or "").strip():
            return self._handle_empty_result()

        rows = parse_json_output(raw)
        if not rows:
            return self._handle_empty_result()

        first_row = rows[0]
        if not isinstance(first_row, dict) or not first_row:
            return False

        first_value = next(iter(first_row.values()))
        return bool(first_value)

    def _handle_empty_result(self) -> bool:
        if self.fail_on_empty:
            raise AirflowFailException("DuckDB sensor: query returned no rows")
        return False

    def on_kill(self) -> None:
        """Terminate the active DuckDB process if poke has started the hook."""
        if self._hook is not None:
            self._hook.on_kill()
