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

from collections.abc import Callable
from pathlib import Path

import pytest

from airflow.providers.arenadata.duckdb.operators.duckdb import DuckDbOperator
from airflow.providers.arenadata.duckdb.utils.json_output import parse_json_output

DuckDbConnFactory = Callable[..., tuple[str, Path]]


@pytest.mark.integration("duckdb")
class TestDuckDbOperatorIntegration:
    def test_create_insert_select_roundtrip(self, duckdb_conn_factory: DuckDbConnFactory) -> None:
        """Operator execute({}) runs multi-statement SQL on a file DB (literals, no Jinja)."""
        conn_id, _ = duckdb_conn_factory()

        operator = DuckDbOperator(
            task_id="duckdb_roundtrip",
            sql=(
                "CREATE TABLE t (id INTEGER, name VARCHAR);"
                "INSERT INTO t VALUES (1, 'bob');"
                "SELECT id, name FROM t ORDER BY id;"
            ),
            duckdb_conn_id=conn_id,
            output_format="json",
        )
        raw = operator.execute({})
        assert parse_json_output(raw) == [{"id": 1, "name": "bob"}]
