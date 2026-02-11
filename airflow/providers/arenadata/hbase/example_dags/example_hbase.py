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
Example DAG showing HBase Thrift2 client usage.

Before running this DAG, create an Airflow Connection:

Connection ID: hbase_thrift2
Connection Type: Generic
Host: your-hbase-host
Port: 9090

For Kerberos, add to Extra:
{
  "auth_method": "GSSAPI",
  "kerberos_service_name": "hbase",
  "use_framed_transport": false,
  "kerberos_keytab": "/etc/security/keytabs/airflow.service.keytab"
}
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
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
    "example_hbase",
    default_args=default_args,
    description="Example HBase Thrift2 DAG",
    schedule_interval=None,
    catchup=False,
    tags=["example", "hbase"],
)


def create_table_task():
    """Create HBase table using Thrift2."""
    hook = HBaseThriftHook(hbase_conn_id="hbase_thrift2")
    
    # Delete table if exists
    if hook.table_exists("test_table_thrift2"):
        hook.delete_table("test_table_thrift2")
        print("Deleted existing table")

    # Create table
    hook.create_table(
        "test_table_thrift2",
        families={
            "cf1": {},
            "cf2": {},
        }
    )
    print("Created table: test_table_thrift2")


def put_data_task():
    """Put data into HBase table using Thrift2."""
    hook = HBaseThriftHook(hbase_conn_id="hbase_thrift2")
    
    # Put single row
    hook.put_row(
        "test_table_thrift2",
        "row1",
        {
            "cf1:col1": "value1",
            "cf1:col2": "value2",
            "cf2:col1": "value3",
        }
    )
    print("Put data for row1")

    # Put more rows
    for i in range(2, 6):
        hook.put_row(
            "test_table_thrift2",
            f"row{i}",
            {
                "cf1:col1": f"value{i}_1",
                "cf2:col1": f"value{i}_2",
            }
        )
    print("Put data for rows 2-5")


def get_data_task():
    """Get data from HBase table using Thrift2."""
    hook = HBaseThriftHook(hbase_conn_id="hbase_thrift2")
    
    # Get single row
    result = hook.get_row("test_table_thrift2", "row1")
    print(f"Got row1: {result}")

    # Get specific columns
    result = hook.get_row(
        "test_table_thrift2",
        "row1",
        columns=["cf1:col1", "cf2:col1"]
    )
    print(f"Got row1 (specific columns): {result}")


def scan_table_task():
    """Scan HBase table using Thrift2."""
    hook = HBaseThriftHook(hbase_conn_id="hbase_thrift2")
    
    # Scan all rows
    results = hook.scan_table("test_table_thrift2")
    print(f"Scanned {len(results)} rows")
    for result in results:
        print(f"  Row: {result['row']}, Columns: {len(result['columns'])}")

    # Scan with limit
    results = hook.scan_table("test_table_thrift2", limit=3)
    print(f"Scanned with limit=3: {len(results)} rows")


def delete_row_task():
    """Delete row from HBase table using Thrift2."""
    hook = HBaseThriftHook(hbase_conn_id="hbase_thrift2")
    
    # Delete specific columns
    hook.delete_row("test_table_thrift2", "row2", columns=["cf1:col1"])
    print("Deleted cf1:col1 from row2")

    # Delete entire row
    hook.delete_row("test_table_thrift2", "row3")
    print("Deleted row3")


def list_tables_task():
    """List all tables using Thrift2."""
    hook = HBaseThriftHook(hbase_conn_id="hbase_thrift2")
    
    tables = hook.list_tables()
    print(f"Tables: {tables}")


def cleanup_task():
    """Delete test table using Thrift2."""
    hook = HBaseThriftHook(hbase_conn_id="hbase_thrift2")
    
    if hook.table_exists("test_table_thrift2"):
        hook.delete_table("test_table_thrift2")
        print("Deleted table: test_table_thrift2")


# Define tasks
create_table = PythonOperator(
    task_id="create_table",
    python_callable=create_table_task,
    dag=dag,
)

put_data = PythonOperator(
    task_id="put_data",
    python_callable=put_data_task,
    dag=dag,
)

get_data = PythonOperator(
    task_id="get_data",
    python_callable=get_data_task,
    dag=dag,
)

scan_table = PythonOperator(
    task_id="scan_table",
    python_callable=scan_table_task,
    dag=dag,
)

delete_row = PythonOperator(
    task_id="delete_row",
    python_callable=delete_row_task,
    dag=dag,
)

list_tables = PythonOperator(
    task_id="list_tables",
    python_callable=list_tables_task,
    dag=dag,
)

cleanup = PythonOperator(
    task_id="cleanup",
    python_callable=cleanup_task,
    dag=dag,
)

# Set dependencies
create_table >> put_data >> get_data >> scan_table >> delete_row >> list_tables >> cleanup
