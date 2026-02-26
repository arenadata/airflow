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
Example DAG demonstrating HBase operations with Kerberos authentication.

This DAG shows how to:
1. Connect to HBase Thrift2 server with Kerberos (GSSAPI) authentication
2. Create a table
3. Insert data
4. Query data
5. Clean up

Prerequisites:
- Valid Kerberos ticket (kinit)
- HBase Thrift2 server configured for Kerberos
- Airflow connection 'hbase_kerberos' with:
  - Host: HBase Thrift2 server hostname
  - Port: 9090 (or your Thrift2 port)
  - Extra: {"auth_method": "GSSAPI", "kerberos_service_name": "hbase"}
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.arenadata.hbase.operators.hbase import (
    HBaseCreateTableOperator,
    HBasePutOperator,
    HBaseScanOperator,
    HBaseDeleteTableOperator,
)

with DAG(
    dag_id="example_hbase_kerberos",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "kerberos"],
    doc_md=__doc__,
) as dag:
    # Create table with column families
    create_table = HBaseCreateTableOperator(
        task_id="create_table",
        hbase_conn_id="hbase_kerberos",
        table_name="test_kerberos_table",
        families={"cf1": {}, "cf2": {}},
    )

    # Insert data into table
    put_data = HBasePutOperator(
        task_id="put_data",
        hbase_conn_id="hbase_kerberos",
        table_name="test_kerberos_table",
        row_key="row1",
        data={
            "cf1:name": "Alice",
            "cf1:age": "30",
            "cf2:city": "New York",
            "cf2:country": "USA",
        },
    )

    # Get data from table
    get_data = HBaseScanOperator(
        task_id="get_data",
        hbase_conn_id="hbase_kerberos",
        table_name="test_kerberos_table",
        row_start="row1",
        row_stop="row2",
    )

    # Delete table
    delete_table = HBaseDeleteTableOperator(
        task_id="delete_table",
        hbase_conn_id="hbase_kerberos",
        table_name="test_kerberos_table",
    )

    # Define task dependencies
    create_table >> put_data >> get_data >> delete_table  # pylint: disable=pointless-statement
