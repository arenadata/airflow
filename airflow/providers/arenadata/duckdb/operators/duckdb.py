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
"""DuckDB operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.arenadata.duckdb.hooks.duckdb import DuckDbHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DuckDbOperator(BaseOperator):  # pylint: disable=too-few-public-methods
    """
    Execute DuckDB SQL via ADO DuckDB CLI.

    Supports inline SQL, Jinja templating, and SQL files (via ``template_ext``).
    Returns raw CLI stdout (JSON string by default) for XCom.

    :param sql: SQL statement or path to a ``.sql`` template file.
    :param duckdb_conn_id: Airflow connection ID for DuckDB.
    :param database: Optional path to ``.duckdb`` file; overrides Connection ``host``.
    :param output_format: CLI output format: ``"json"`` (default), ``"csv"``, or ``None``.
    """

    template_fields: Sequence[str] = ("sql", "database", "duckdb_conn_id")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#fff4b8"

    def __init__(
        self,
        sql: str,
        duckdb_conn_id: str = DuckDbHook.default_conn_name,
        *,
        database: str | None = None,
        output_format: str | None = "json",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not sql:
            raise ValueError("sql cannot be empty")
        self.sql = sql
        self.duckdb_conn_id = duckdb_conn_id
        self.database = database
        self.output_format = output_format

    def execute(self, context: Context) -> str:
        """Run SQL and return raw stdout from DuckDB CLI."""
        hook = DuckDbHook(duckdb_conn_id=self.duckdb_conn_id)
        return hook.run_cli(
            self.sql,
            output_format=self.output_format,
            database=self.database,
        )
