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
"""
Example DAG: DuckDB pipeline (inline SQL, Connection, sensor, XCom)

Before running, create an Airflow Connection:

Connection ID: duckdb_default
Connection Type: duckdb
Host (database file path): /tmp/example_duckdb.duckdb
Extra (JSON):
  {"duckdb_binary": "/usr/bin/duckdb"}

On macOS with Homebrew use ``/opt/homebrew/bin/duckdb`` instead of ``/usr/bin/duckdb``

Important:

- Do not use ``:memory:`` in multi-task DAGs: each task runs DuckDB CLI in a
  separate subprocess, so in-memory state does not persist between tasks.
- ``DuckDbSqlSensor`` waits for a truthy query result against an **existing**
  table or view. A query against a missing object fails the task; it does not
  keep waiting
- Do not run ``example_duckdb_basic`` and ``example_duckdb_sensors`` at the
  same time: they share ``duckdb_default.host``. DuckDB allows only one writer
  process per file. Use separate Connections for parallel runs
- In production the data producer is often an external process or another DAG;
  the sensor then blocks until data appears
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.arenadata.duckdb.operators.duckdb import DuckDbOperator
from airflow.providers.arenadata.duckdb.sensors.duckdb import DuckDbSqlSensor
from airflow.providers.arenadata.duckdb.version_compat import DAG

CONN_ID = "duckdb_default"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="example_duckdb_basic",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["example", "duckdb"],
) as dag:
    create_table = DuckDbOperator(
        task_id="create_table",
        sql="CREATE OR REPLACE TABLE demo(id INT, name VARCHAR);",
        duckdb_conn_id=CONN_ID,
    )

    insert_data = DuckDbOperator(
        task_id="insert_data",
        sql="INSERT INTO demo VALUES (1, 'alpha'), (2, 'beta');",
        duckdb_conn_id=CONN_ID,
    )

    wait_for_rows = DuckDbSqlSensor(
        task_id="wait_for_rows",
        sql="SELECT count(*) AS c FROM demo",
        duckdb_conn_id=CONN_ID,
        mode="reschedule",
        poke_interval=30,
        timeout=300,
    )

    select_count = DuckDbOperator(
        task_id="select_count",
        sql="SELECT count(*) AS c FROM demo",
        duckdb_conn_id=CONN_ID,
    )

    create_table >> insert_data >> wait_for_rows >> select_count
