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
"""Example DAG demonstrating HBase Thrift2 batch operations.

This DAG shows two approaches for batch operations:
1. Using Hook methods directly via PythonOperator (lower-level)
2. Using specialized Operators (higher-level, recommended)
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.arenadata.hbase.hooks.hbase import HBaseHook
from airflow.providers.arenadata.hbase.operators.hbase import (
    HBaseBatchGetOperator,
    HBaseBatchPutOperator,
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBaseScanOperator,
)
from airflow.operators.python import PythonOperator

# Connection ID for Thrift2
HBASE_CONN_ID = "hbase_thrift2"
TABLE_NAME = "test_batch_table"


def delete_table_if_exists():
    """Delete table if it exists for idempotency."""
    hook = HBaseHook(hbase_conn_id=HBASE_CONN_ID)
    if hook.table_exists(TABLE_NAME):
        hook.delete_table(TABLE_NAME)
        print(f"Deleted existing table: {TABLE_NAME}")
    else:
        print(f"Table {TABLE_NAME} does not exist")


def create_table():
    """Create test table."""
    hook = HBaseHook(hbase_conn_id=HBASE_CONN_ID)
    families = {"cf1": {}, "cf2": {}}
    hook.create_table(TABLE_NAME, families)
    print(f"Created table: {TABLE_NAME}")


def batch_put_rows():
    """Batch insert rows using Thrift2."""
    hook = HBaseHook(hbase_conn_id=HBASE_CONN_ID)

    # Prepare 100 rows
    rows = []
    for i in range(100):
        rows.append({
            "row_key": f"row_{i:03d}",
            "cf1:col1": f"value1_{i}",
            "cf1:col2": f"value2_{i}",
            "cf2:col1": f"value3_{i}",
        })

    # Batch insert with batch_size=20
    hook.batch_put_rows(TABLE_NAME, rows, batch_size=20)
    print(f"Batch inserted {len(rows)} rows")


def batch_get_rows():
    """Batch get rows using Thrift2."""
    hook = HBaseHook(hbase_conn_id=HBASE_CONN_ID)

    # Get first 10 rows
    row_keys = [f"row_{i:03d}" for i in range(10)]
    results = hook.batch_get_rows(TABLE_NAME, row_keys, columns=["cf1:col1", "cf2:col1"])

    print(f"Batch retrieved {len(results)} rows:")
    for result in results:
        print(f"  {result}")


def batch_delete_rows():
    """Batch delete rows using Thrift2."""
    hook = HBaseHook(hbase_conn_id=HBASE_CONN_ID)

    # Delete rows 50-99
    row_keys = [f"row_{i:03d}" for i in range(50, 100)]
    hook.batch_delete_rows(TABLE_NAME, row_keys, batch_size=20)
    print(f"Batch deleted {len(row_keys)} rows")


def cleanup_table():
    """Delete table at the end."""
    hook = HBaseHook(hbase_conn_id=HBASE_CONN_ID)
    hook.delete_table(TABLE_NAME)
    print(f"Deleted table: {TABLE_NAME}")


with DAG(
    dag_id="example_hbase_batch",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "batch"],
    doc_md=__doc__,
) as dag:

    delete_if_exists = PythonOperator(
        task_id="delete_table_if_exists",
        python_callable=delete_table_if_exists,
    )

    create = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    batch_put = PythonOperator(
        task_id="batch_put_rows",
        python_callable=batch_put_rows,
    )

    batch_get = PythonOperator(
        task_id="batch_get_rows",
        python_callable=batch_get_rows,
    )

    batch_delete = PythonOperator(
        task_id="batch_delete_rows",
        python_callable=batch_delete_rows,
    )

    cleanup = PythonOperator(
        task_id="cleanup_table",
        python_callable=cleanup_table,
    )

    delete_if_exists >> create >> batch_put >> batch_get >> batch_delete >> cleanup

    # Approach 2: Using specialized operators (recommended)
    create_operator = HBaseCreateTableOperator(
        task_id="create_table_operator",
        table_name=TABLE_NAME,
        families={"cf1": {}, "cf2": {}},
        hbase_conn_id=HBASE_CONN_ID,
    )

    batch_put_operator = HBaseBatchPutOperator(
        task_id="batch_put_operator",
        table_name=TABLE_NAME,
        rows=[
            {
                "row_key": f"op_row_{i:03d}",
                "cf1:col1": f"op_value1_{i}",
                "cf1:col2": f"op_value2_{i}",
                "cf2:col1": f"op_value3_{i}",
            }
            for i in range(50)
        ],
        batch_size=10,
        max_workers=2,
        hbase_conn_id=HBASE_CONN_ID,
    )

    scan_operator = HBaseScanOperator(
        task_id="scan_operator",
        table_name=TABLE_NAME,
        columns=["cf1:col1", "cf2:col1"],
        limit=20,
        hbase_conn_id=HBASE_CONN_ID,
    )

    batch_get_operator = HBaseBatchGetOperator(
        task_id="batch_get_operator",
        table_name=TABLE_NAME,
        row_keys=[f"op_row_{i:03d}" for i in range(10)],
        columns=["cf1:col1", "cf1:col2"],
        hbase_conn_id=HBASE_CONN_ID,
    )

    delete_operator = HBaseDeleteTableOperator(
        task_id="delete_table_operator",
        table_name=TABLE_NAME,
        hbase_conn_id=HBASE_CONN_ID,
    )

    cleanup >> create_operator >> batch_put_operator >> scan_operator >> batch_get_operator >> delete_operator
