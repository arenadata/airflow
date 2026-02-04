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
HBase backup producer DAG - generates data for backup.

This DAG demonstrates data generation that triggers backup via Datasets:
1. Creates table
2. Populates with test data
3. Produces dataset event to trigger backup DAG

Prerequisites:
- HBase must be running in distributed mode with HDFS
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.hbase.operators.hbase import (
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBasePutOperator,
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

# Define dataset for the table
backup_table_dataset = hbase_table_dataset(
    host="hbase",
    port=9090,
    table_name="test_table_backup"
)

with DAG(
    "example_hbase_backup_producer",
    default_args=default_args,
    description="Generate data for HBase backup",
    schedule="@daily",  # Runs daily
    catchup=False,
    tags=["example", "hbase", "backup", "producer"],
) as dag:

    # Delete table if exists for idempotency
    delete_table_cleanup = HBaseDeleteTableOperator(
        task_id="delete_table_cleanup",
        table_name="test_table_backup",
        hbase_conn_id="hbase_thrift2",
    )

    # Create test table for backup
    create_table = HBaseCreateTableOperator(
        task_id="create_table",
        table_name="test_table_backup",
        families={"cf1": {}, "cf2": {}},
        hbase_conn_id="hbase_thrift2",
        # No outlets here - only produce dataset after data is inserted
    )

    # Add test data
    put_data = HBasePutOperator(
        task_id="put_data",
        table_name="test_table_backup",
        row_key="test_row",
        data={"cf1:col1": "test_value"},
        hbase_conn_id="hbase_thrift2",
        outlets=[backup_table_dataset],  # Produce dataset only after data is ready
    )

    # Define task dependencies
    delete_table_cleanup >> create_table >> put_data
