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

This DAG demonstrates data-aware scheduling:
1. Automatically triggered when test_table_backup is updated
2. Creates backup set
3. Performs full backup with predefined backup ID
4. Gets backup history

Prerequisites:
- HBase must be running in distributed mode with HDFS
- Create backup directory in HDFS: hdfs dfs -mkdir -p /hbase/backup && hdfs dfs -chmod 777 /hbase/backup
- Producer DAG must run first to generate data
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.hbase.operators.hbase import (
    BackupSetAction,
    BackupType,
    HBaseBackupHistoryOperator,
    HBaseBackupSetOperator,
    HBaseCreateBackupOperator,
)
from airflow.providers.hbase.datasets.hbase import hbase_table_dataset

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
backup_table_dataset = hbase_table_dataset(
    host="hbase",
    port=9090,
    table_name="test_table_backup"
)

# Predefined backup ID for use in restore DAG
# Format: backup_<timestamp> - you can use this ID in restore DAG
BACKUP_ID = "backup_{{ ds_nodash }}"  # e.g., backup_20240130

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
        bash_command="/usr/lib/hbase/bin/hbase backup repair || true",  # || true to not fail if nothing to repair
    )

    # Create backup set
    create_backup_set = HBaseBackupSetOperator(
        task_id="create_backup_set",
        action=BackupSetAction.ADD,
        backup_set_name="test_backup_set",
        tables=["test_table_backup"],
        hbase_conn_id="hbase_thrift2",
    )

    # List backup sets
    list_backup_sets = HBaseBackupSetOperator(
        task_id="list_backup_sets",
        action=BackupSetAction.LIST,
        hbase_conn_id="hbase_thrift2",
    )

    # Create full backup
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

    # Get backup history
    get_backup_history = HBaseBackupHistoryOperator(
        task_id="get_backup_history",
        backup_set_name="test_backup_set",
        hbase_conn_id="hbase_thrift2",
    )

    # Define task dependencies
    cleanup_stuck_sessions >> create_backup_set >> list_backup_sets >> create_full_backup >> get_backup_history
