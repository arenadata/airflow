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
Example DAG: DuckDB sensor patterns (inline SQL and SQL file)

Uses ``duckdb_default`` Connection; the database path comes from Connection
``host``. Optional ``database=`` on the sensor overrides ``host`` (same as
``DuckDbOperator``); this DAG does not set it

Connection ``duckdb_default``:

Connection Type: duckdb
Host: /tmp/example_duckdb.duckdb
Extra: {"duckdb_binary": "/usr/bin/duckdb"}  (macOS: /opt/homebrew/bin/duckdb)

Important:

- The sensor waits for **data** in an existing table, not for the table itself
  Queries against missing objects fail the task.
- Data is seeded in this DAG for a self-contained demo. In production the
  producer is usually an external process or another DAG; then the sensor
  blocks until data is ready
- Do not run together with ``example_duckdb_basic``: both use the same file
  and DuckDB allows only one writer at a time
- ``fail_on_empty=True`` fails when the query returns no rows;
  ``soft_fail=True`` raises ``AirflowSkipException`` instead (not shown here)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.arenadata.duckdb.operators.duckdb import DuckDbOperator
from airflow.providers.arenadata.duckdb.sensors.duckdb import DuckDbSqlSensor

CONN_ID = "duckdb_default"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="example_duckdb_sensors",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["example", "duckdb", "sensor"],
    params={
        "table": Param("events", type="string"),
        "min_id": Param(1, type="integer"),
    },
) as dag:
    seed_data = DuckDbOperator(
        task_id="seed_data",
        sql="""
            CREATE OR REPLACE TABLE {{ params.table }}(id INT);
            INSERT INTO {{ params.table }} VALUES (1);
        """,
        duckdb_conn_id=CONN_ID,
    )

    wait_inline = DuckDbSqlSensor(
        task_id="wait_inline",
        sql="SELECT count(*) AS ready FROM {{ params.table }}",
        duckdb_conn_id=CONN_ID,
        mode="reschedule",
        poke_interval=30,
        timeout=300,
    )

    wait_from_sql_file = DuckDbSqlSensor(
        task_id="wait_from_sql_file",
        sql="queries/wait_until_ready.sql",
        duckdb_conn_id=CONN_ID,
        mode="reschedule",
        poke_interval=30,
        timeout=300,
    )

    seed_data >> wait_inline >> wait_from_sql_file
