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

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.arenadata.duckdb.hooks.duckdb import ALLOWED_OUTPUT_FORMATS, DuckDbHook
from airflow.providers.arenadata.duckdb.utils.errors import DuckDbConfigurationError
from airflow.providers.arenadata.duckdb.version_compat import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.arenadata.duckdb.version_compat import Context


class DuckDbOperator(BaseOperator):  # pylint: disable=too-few-public-methods
    """
    Execute DuckDB SQL via ADO DuckDB CLI.

    Supports inline SQL, Jinja templating, and ``.sql`` files (via ``template_ext``;
    see connections docs for DAG-bundle and missing-file behavior).
    Returns raw CLI stdout (JSON by default) for XCom.

    :param sql: SQL statement or path to a ``.sql`` template file.
    :param duckdb_conn_id: Airflow connection ID for DuckDB.
    :param database: Optional path to ``.duckdb`` file; overrides Connection ``host``.
    :param output_format: CLI output format: ``"json"`` (default), ``"csv"``, or ``None``.
    :param parameters: optional ``%(name)s`` placeholders bound into *sql*
        after Jinja templating.
    :param lock_retry_attempts: Opt-in CLI lock retries for the hook
        (``None`` = connection extra / default 0). Not templated.
    :param log_output_limit: Max chars of stdout/stderr on INFO logs
        (``None`` = hook module default). Not templated.
    """

    template_fields: Sequence[str] = ("sql", "parameters", "database", "duckdb_conn_id")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql", "parameters": "json"}
    ui_color = "#fff4b8"

    def __init__(
        self,
        sql: str,
        duckdb_conn_id: str = DuckDbHook.default_conn_name,
        *,
        database: str | None = None,
        output_format: str | None = "json",
        parameters: Mapping[str, Any] | None = None,
        lock_retry_attempts: int | None = None,
        log_output_limit: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not sql:
            raise ValueError("sql cannot be empty")
        if output_format not in ALLOWED_OUTPUT_FORMATS:
            raise DuckDbConfigurationError(
                f"Invalid output_format={output_format!r}. Allowed values: None, 'json', 'csv'."
            )
        self.sql = sql
        self.duckdb_conn_id = duckdb_conn_id
        self.database = database
        self.output_format = output_format
        self.parameters = dict(parameters) if parameters is not None else None
        self.lock_retry_attempts = lock_retry_attempts
        self.log_output_limit = log_output_limit
        self._hook: DuckDbHook | None = None

    def execute(self, context: Context) -> str:
        """Run SQL and return raw stdout from DuckDB CLI."""
        self._hook = DuckDbHook(
            duckdb_conn_id=self.duckdb_conn_id,
            lock_retry_attempts=self.lock_retry_attempts,
            log_output_limit=self.log_output_limit,
        )
        return self._hook.run_cli(
            self.sql,
            output_format=self.output_format,
            database=self.database,
            parameters=self.parameters,
        )

    def on_kill(self) -> None:
        """Terminate the active DuckDB process if execute has started the hook."""
        if self._hook is not None:
            self._hook.on_kill()
