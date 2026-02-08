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

HDFS to Ozone (DistCp)
=====================

``HdfsToOzoneOperator`` migrates data from HDFS to Ozone using ``hadoop distcp``.
It is intended for large-scale transfers and supports retries for transient failures.

Operator
--------

* ``airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone.HdfsToOzoneOperator``

Prerequisites
-------------

* ``hadoop`` binary available on the worker
* Correct Hadoop config for both source and destination (typically via ``HADOOP_CONF_DIR``)

Example
-------

.. code-block:: python

   from airflow import DAG
   from airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone import HdfsToOzoneOperator

   with DAG("hdfs_to_ozone_example", schedule=None, start_date=None) as dag:
       migrate = HdfsToOzoneOperator(
           task_id="migrate",
           source_path="hdfs:///user/data/legacy/",
           dest_path="ofs://om/vol1/bucket1/migrated/",
       )
