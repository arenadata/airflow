.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

Ozone to Hive (partition registration)
=====================================

``OzoneToHiveOperator`` registers an Ozone path as a Hive table partition (Hive CLI).

Operator
--------

* ``airflow.providers.arenadata.ozone.transfers.ozone_to_hive.OzoneToHiveOperator``

Dependency note
---------------

This operator requires installing the Hive provider:

* ``apache-airflow-providers-apache-hive``

The import is performed lazily so DAG parsing does not break if the Hive provider is not installed,
but the task will fail at runtime with a clear error message.

Example
-------

.. code-block:: python

   from airflow import DAG
   from airflow.providers.arenadata.ozone.transfers.ozone_to_hive import OzoneToHiveOperator

   with DAG("ozone_to_hive_example", schedule=None, start_date=None) as dag:
       register = OzoneToHiveOperator(
           task_id="register_partition",
           ozone_path="ofs://om/archive/processed/ds={{ ds }}",
           table_name="processed_events",
           partition_spec={"ds": "{{ ds }}"},
           hive_cli_conn_id="hive_cli_default",
       )
