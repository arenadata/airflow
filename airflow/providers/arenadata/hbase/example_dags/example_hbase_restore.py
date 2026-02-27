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
HBase restore operations example.

This DAG demonstrates HBase restore functionality with data verification.
It uses the backup ID from the backup consumer DAG via XCom or manual specification.

Workflow:
1. Delete table (to simulate data loss)
2. Restore from backup using backup ID
3. Verify data was restored correctly

To get backup ID from previous DAG run:
- Use XCom: {{ ti.xcom_pull(dag_id='example_hbase_backup_consumer',
task_ids='create_full_backup') }}
- Or specify manually in backup_id parameter
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.arenadata.hbase.operators.hbase import (
    HBaseDeleteTableOperator,
    HBaseRestoreOperator,
)
from airflow.providers.arenadata.hbase.hooks.hbase import HBaseThriftHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "example_hbase_restore",
    default_args=default_args,
    description="HBase restore operations with verification",
    schedule_interval=None,
    catchup=False,
    tags=["example", "hbase", "restore"],
    params={
        "backup_id": Param(
            default="",
            type="string",
            description="Backup ID to restore (e.g., backup_1770197062556). Required.",
        ),
    },
)


def verify_restored_data(**context):
    """Verify that data was restored correctly."""

    hook = HBaseThriftHook(hbase_conn_id="hbase_thrift2")
    table_name = "test_table_backup"

    # Get restore output from previous task
    ti = context["ti"]
    restore_output = ti.xcom_pull(task_ids="restore_backup")
    if restore_output:
        print("\nRestore operation output:")
        print(restore_output)

        # Check if backup is not enabled
        if "Backup is not enabled" in restore_output:
            print("\n" + "=" * 60)
            print("HBASE BACKUP IS NOT ENABLED ON THE CLUSTER")
            print("=" * 60)
            print("\nTo enable HBase backup, add to hbase-site.xml:")
            print("")
            print("  <property>")
            print("    <name>hbase.backup.enable</name>")
            print("    <value>true</value>")
            print("  </property>")
            print("")
            print("And configure backup classes (see HBase documentation).")
            print("Then restart HBase cluster.")
            print("\nSee: http://hbase.apache.org/book.html#backuprestore")
            print("=" * 60)
            return

    # Check if table exists
    if not hook.table_exists(table_name):
        print(f"\nTable '{table_name}' does not exist after restore!")
        print("\nPossible reasons:")
        print("  1. Restore operation failed (check restore task logs above)")
        print("  2. Incorrect backup_id specified")
        print("  3. Backup doesn't contain this table")
        print("  4. Insufficient permissions")
        return

    print(f"✓ Table '{table_name}' exists")

    # Scan table to get row count
    rows = list(hook.scan_table(table_name, limit=1000))
    row_count = len(rows)

    print(f"Restored table '{table_name}' contains {row_count} rows")

    if row_count == 0:
        print("\nWARNING: Table exists but is empty after restore!")
        print("This could mean:")
        print("  1. The backup was empty")
        print("  2. The backup_id is incorrect")
        print("  3. The restore operation didn't complete successfully")
        return

    # Show sample data
    print("\nSample restored data (first 5 rows):")
    for i, (row_key, data) in enumerate(rows[:5]):
        print(f"  Row {i+1}: {row_key} -> {data}")

    print(f"\nRestore verification successful: {row_count} rows restored")


# Step 1: Delete table to simulate data loss (ignore if not exists)
delete_table = HBaseDeleteTableOperator(
    task_id="delete_table",
    table_name="test_table_backup",
    if_not_exists="ignore",
    hbase_conn_id="hbase_thrift2",
    dag=dag,
)

# Step 2: Restore from backup using backup_id from params
restore_backup = HBaseRestoreOperator(
    task_id="restore_backup",
    backup_path="hdfs:///hbase/backup",
    backup_id="{{ params.backup_id }}",
    tables=["test_table_backup"],
    overwrite=True,
    hbase_conn_id="hbase_thrift2",
    do_xcom_push=True,
    dag=dag,
)

# Step 3: Verify restored data
verify_data = PythonOperator(
    task_id="verify_restored_data",
    python_callable=verify_restored_data,
    dag=dag,
)

# Define task dependencies
delete_table >> restore_backup >> verify_data  # pylint: disable=pointless-statement
