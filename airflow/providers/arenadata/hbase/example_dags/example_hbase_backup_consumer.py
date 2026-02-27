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
HBase backup consumer DAG - automatically triggered by dataset.

This DAG demonstrates data-aware scheduling with incremental backups:
1. Automatically triggered when test_table_backup is updated
2. Creates backup set
3. Performs FULL backup on first run, then INCREMENTAL backups
4. Gets backup history

Prerequisites:
- HBase must be running in distributed mode with HDFS
- Create backup directory in HDFS:
  hdfs dfs -mkdir -p /hbase/backup && hdfs dfs -chmod 777 /hbase/backup
- Producer DAG must run first to generate data
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.arenadata.hbase.operators.hbase import (
    BackupSetAction,
    BackupType,
    HBaseBackupHistoryOperator,
    HBaseBackupSetOperator,
    HBaseCreateBackupOperator,
)
from airflow.providers.arenadata.hbase.datasets.hbase import hbase_table_dataset
from airflow.providers.arenadata.hbase.hooks.hbase_cli import HBaseCLIHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define dataset - same as in producer
backup_table_dataset = hbase_table_dataset(host="hbase", port=9090, table_name="test_table_backup")


def decide_backup_type(**_context) -> str:
    """
    Decide whether to create FULL or INCREMENTAL backup.

    Logic:
    - If no backups exist for test_table_backup -> FULL
    - If backups exist for test_table_backup -> INCREMENTAL

    Returns:
        Task ID to execute next
    """
    hook = HBaseCLIHook(
        hbase_conn_id="hbase_thrift2",
        java_home="/usr/lib/jvm/java-arenadata-openjdk-8",
        hbase_home="/usr/lib/hbase",
    )

    # Get ALL backup history (backup set filter doesn't work properly)
    try:
        history = hook.get_backup_history()
        print(f"Full backup history:\n{history}")

        # Check if any backups exist for test_table_backup
        if not history or not history.strip():
            print("No previous backups found. Creating FULL backup.")
            return "create_full_backup"

        # Check if test_table_backup is mentioned in history
        if "test_table_backup" in history:
            print("Previous backups found for test_table_backup. Creating INCREMENTAL backup.")
            return "create_incremental_backup"
        print("No backups found for test_table_backup. Creating FULL backup.")
        return "create_full_backup"
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error getting backup history: {e}")
        print("Defaulting to FULL backup.")
        return "create_full_backup"


with DAG(
    "example_hbase_backup_consumer",
    default_args=default_args,
    description="Automatic HBase backup triggered by data updates",
    schedule=[backup_table_dataset],  # Triggered by dataset updates
    catchup=False,
    tags=["example", "hbase", "backup", "consumer"],
) as dag:

    # Cleanup any stuck backup sessions before starting
    cleanup_stuck_sessions = BashOperator(
        task_id="cleanup_stuck_sessions",
        # || true to not fail if nothing to repair
        bash_command=("/usr/lib/hbase/bin/hbase backup repair || true"),
    )

    # Create backup set
    create_backup_set = HBaseBackupSetOperator(
        task_id="create_backup_set",
        action=BackupSetAction.ADD,
        backup_set_name="test_backup_set",
        tables=["test_table_backup"],
        hbase_conn_id="hbase_thrift2",
    )

    # Decide backup type based on history
    decide_backup = BranchPythonOperator(
        task_id="decide_backup_type",
        python_callable=decide_backup_type,
    )

    # Create FULL backup (first time)
    # Note: backup_path must be a valid HDFS path
    # Examples:
    #   - hdfs:///hbase/backup (uses default namenode)
    #   - hdfs://namenode:8020/hbase/backup (explicit namenode)
    create_full_backup = HBaseCreateBackupOperator(
        task_id="create_full_backup",
        backup_type=BackupType.FULL,
        backup_path="hdfs:///hbase/backup",  # HDFS URI
        backup_set_name="test_backup_set",
        workers=1,
        hbase_conn_id="hbase_thrift2",
        do_xcom_push=True,  # Push backup ID to XCom
    )

    # Create INCREMENTAL backup (subsequent runs)
    # Only saves changes since last backup (FULL or INCREMENTAL)
    create_incremental_backup = HBaseCreateBackupOperator(
        task_id="create_incremental_backup",
        backup_type=BackupType.INCREMENTAL,
        backup_path="hdfs:///hbase/backup",  # Same path as FULL
        backup_set_name="test_backup_set",
        workers=1,
        hbase_conn_id="hbase_thrift2",
        do_xcom_push=True,  # Push backup ID to XCom
    )

    # Get backup history (runs after either backup type)
    get_backup_history = HBaseBackupHistoryOperator(
        task_id="get_backup_history",
        backup_set_name="test_backup_set",
        hbase_conn_id="hbase_thrift2",
        trigger_rule="none_failed_min_one_success",
    )

    # Define task dependencies
    (cleanup_stuck_sessions >> create_backup_set >> decide_backup)  # pylint: disable=pointless-statement
    (decide_backup >> [create_full_backup, create_incremental_backup])  # pylint: disable=pointless-statement
    (  # pylint: disable=pointless-statement
        [create_full_backup, create_incremental_backup] >> get_backup_history
    )
