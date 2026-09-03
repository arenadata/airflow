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
Example DAG: DuckDbSqlSensor (inline SQL and SQL file)

This DAG does not insert data. It waits until a query is truthy.

Connection ``duckdb_default`` — same as ``example_duckdb_basic``.

Before trigger, the table must already exist (missing object fails, it does
not wait). Empty is fine:

    CREATE TABLE IF NOT EXISTS events(id INT);

Trigger the DAG: ``wait_inline`` reschedules while there are no rows
(timeout 300s). Unblock from another session (not during a poke — file lock):

    INSERT INTO events VALUES (1);

Do not run together with ``example_duckdb_basic`` on the same Connection host
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.arenadata.duckdb.sensors.duckdb import DuckDbSqlSensor
from airflow.providers.arenadata.duckdb.version_compat import DAG, Param

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

    wait_inline >> wait_from_sql_file
