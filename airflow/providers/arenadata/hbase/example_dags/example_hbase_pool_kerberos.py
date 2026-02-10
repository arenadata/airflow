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
Example DAG demonstrating Thrift2 connection pool with Kerberos authentication.

This DAG shows how to use connection pooling with Kerberos for high-performance
parallel operations on HBase.

Prerequisites:
- Valid Kerberos ticket (kinit)
- HBase Thrift2 server configured for Kerberos
- Airflow connection 'hbase_kerberos' with:
  - Host: HBase Thrift2 server hostname
  - Port: 9090 (or your Thrift2 port)
  - Extra: {
      "auth_method": "GSSAPI",
      "kerberos_service_name": "hbase",
      "connection_pool": {
        "enabled": true,
        "size": 10
      }
    }
"""

from __future__ import annotations

import time
from datetime import datetime

from airflow import DAG
from airflow.providers.arenadata.hbase.hooks.hbase import HBaseThriftHook
from airflow.operators.python import PythonOperator

CONN_ID = "hbase_kerberos"
TABLE_NAME = "kerberos_pool_test"


def setup_table():
    """Create test table."""
    hook = HBaseThriftHook(hbase_conn_id=CONN_ID)

    if hook.table_exists(TABLE_NAME):
        hook.delete_table(TABLE_NAME)

    families = {"cf1": {}, "cf2": {}, "cf3": {}}
    hook.create_table(TABLE_NAME, families)
    print(f"Created table: {TABLE_NAME}")


def benchmark_parallel_insert():
    """Benchmark parallel insert with connection pool."""
    hook = HBaseThriftHook(hbase_conn_id=CONN_ID)

    # Generate 10,000 rows
    rows = []
    for i in range(10000):
        rows.append({
            "row_key": f"row_{i:06d}",
            "cf1:col1": f"value1_{i}",
            "cf1:col2": f"value2_{i}",
            "cf2:col1": f"value3_{i}",
            "cf2:col2": f"value4_{i}",
            "cf3:col1": f"value5_{i}",
        })

    start = time.time()
    # Use 4 workers from pool of 10
    hook.batch_put_rows(TABLE_NAME, rows, batch_size=200, max_workers=4)
    elapsed = time.time() - start

    print(f"Inserted {len(rows)} rows in {elapsed:.2f}s ({len(rows)/elapsed:.0f} rows/sec)")


def benchmark_large_dataset():
    """Benchmark large dataset with connection pool."""
    hook = HBaseThriftHook(hbase_conn_id=CONN_ID)

    # Generate 50,000 rows
    rows = []
    for i in range(50000):
        rows.append({
            "row_key": f"large_{i:06d}",
            "cf1:col1": f"value1_{i}",
            "cf1:col2": f"value2_{i}",
            "cf2:col1": f"value3_{i}",
        })

    start = time.time()
    # Use 6 workers from pool of 10
    hook.batch_put_rows(TABLE_NAME, rows, batch_size=250, max_workers=6)
    elapsed = time.time() - start

    print(f"Inserted {len(rows)} rows in {elapsed:.2f}s ({len(rows)/elapsed:.0f} rows/sec)")


def verify_data():
    """Verify inserted data."""
    hook = HBaseThriftHook(hbase_conn_id=CONN_ID)

    results = hook.scan_table(TABLE_NAME, row_start="row_000000", row_stop="row_000010")
    print(f"Sample from parallel insert: {len(results)} rows")

    results = hook.scan_table(TABLE_NAME, row_start="large_000000", row_stop="large_000010")
    print(f"Sample from large dataset: {len(results)} rows")


def cleanup_table():
    """Delete test table."""
    hook = HBaseThriftHook(hbase_conn_id=CONN_ID)
    hook.delete_table(TABLE_NAME)
    print(f"Deleted table: {TABLE_NAME}")


with DAG(
    dag_id="example_hbase_pool_kerberos",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "pool", "kerberos"],
    doc_md=__doc__,
) as dag:

    setup = PythonOperator(
        task_id="setup_table",
        python_callable=setup_table,
    )

    bench_parallel = PythonOperator(
        task_id="benchmark_parallel_insert",
        python_callable=benchmark_parallel_insert,
    )

    bench_large = PythonOperator(
        task_id="benchmark_large_dataset",
        python_callable=benchmark_large_dataset,
    )

    verify = PythonOperator(
        task_id="verify_data",
        python_callable=verify_data,
    )

    cleanup = PythonOperator(
        task_id="cleanup_table",
        python_callable=cleanup_table,
    )

    setup >> bench_parallel >> bench_large >> verify >> cleanup
