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

from airflow.providers.arenadata.duckdb.hooks.duckdb import DuckDbHook
from airflow.providers.arenadata.duckdb.sensors.duckdb import DuckDbSqlSensor
from airflow.providers.arenadata.duckdb.utils.errors import DuckDbCliError

DuckDbConnFactory = Callable[..., tuple[str, Path]]


@pytest.mark.integration("duckdb")
class TestDuckDbSqlSensorIntegration:
    def test_poke_truthy_and_empty(self, duckdb_conn_factory: DuckDbConnFactory) -> None:
        """One poke: truthy row → True; empty result → False."""
        conn_id, _ = duckdb_conn_factory()
        DuckDbHook(duckdb_conn_id=conn_id).run_cli(
            "CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);",
            output_format=None,
        )

        sensor = DuckDbSqlSensor(
            task_id="duckdb_sensor",
            sql="SELECT COUNT(*) AS c FROM t WHERE id = 1;",
            duckdb_conn_id=conn_id,
            poke_interval=1,
            timeout=30,
        )
        assert sensor.poke({}) is True

        empty_sensor = DuckDbSqlSensor(
            task_id="duckdb_sensor_empty",
            sql="SELECT id FROM t WHERE id = -1;",
            duckdb_conn_id=conn_id,
            poke_interval=1,
            timeout=30,
        )
        assert empty_sensor.poke({}) is False

    def test_poke_cli_failure_raises(self, duckdb_conn_factory: DuckDbConnFactory) -> None:
        """CLI failure during poke propagates (does not return False)."""
        conn_id, _ = duckdb_conn_factory()
        DuckDbHook(duckdb_conn_id=conn_id).run_cli("SELECT 1;", output_format=None)

        sensor = DuckDbSqlSensor(
            task_id="duckdb_sensor_fail",
            sql="SELECT * FROM no_such_table;",
            duckdb_conn_id=conn_id,
            poke_interval=1,
            timeout=30,
        )
        with pytest.raises(DuckDbCliError):
            sensor.poke({})
