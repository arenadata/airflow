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
Example DAG: DuckDB operator features (SQL file, Jinja, explicit database path)

Uses ``database="{{ params.db_path }}"`` on every task to override Connection
``host``. ``params.db_path`` must differ from ``duckdb_default.host`` so the
override demo is visible (e.g. host ``/tmp/example_duckdb.duckdb``, db_path
``/tmp/example_duckdb_sql_file.duckdb``)

Connection ``duckdb_default`` (same as example_duckdb_basic):

Connection Type: duckdb
Host: /tmp/example_duckdb.duckdb
Extra: {"duckdb_binary": "/usr/bin/duckdb"}  (macOS: /opt/homebrew/bin/duckdb)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.arenadata.duckdb.operators.duckdb import DuckDbOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="example_duckdb_sql_file",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["example", "duckdb", "operator"],
    params={
        "db_path": Param("/tmp/example_duckdb_sql_file.duckdb", type="string"),
        "table": Param("demo", type="string"),
        "label": Param("from_sql_file", type="string"),
    },
) as dag:
    create_table = DuckDbOperator(
        task_id="create_table",
        sql="""
            CREATE OR REPLACE TABLE {{ params.table }}(
                id INT, name VARCHAR
            );
        """,
        database="{{ params.db_path }}",
        duckdb_conn_id="duckdb_default",
    )

    load_from_file = DuckDbOperator(
        task_id="load_from_file",
        sql="queries/load_demo.sql",
        database="{{ params.db_path }}",
        duckdb_conn_id="duckdb_default",
    )

    export_csv = DuckDbOperator(
        task_id="export_csv",
        sql="SELECT * FROM {{ params.table }} ORDER BY id",
        database="{{ params.db_path }}",
        output_format="csv",  # CLI uses -noheader; stdout has no column names
        duckdb_conn_id="duckdb_default",
    )

    create_table >> load_from_file >> export_csv
