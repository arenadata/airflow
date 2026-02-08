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

Transfer operators (local + intra-cluster move)
===============================================

Those operators perform data movement using the Native CLI.

Operators
---------

* ``airflow.providers.arenadata.ozone.operators.ozone_transfer.LocalFilesystemToOzoneOperator``
* ``airflow.providers.arenadata.ozone.operators.ozone_transfer.OzoneToOzoneOperator``

Notes
-----

* ``OzoneToOzoneOperator`` is a metadata-only operation on the Ozone side (fast).
* Wildcards are supported in ``source_path`` (e.g. ``ofs://om/vol/bucket/*``) and treated as idempotent.

Example
-------

.. code-block:: python

   from airflow import DAG
   from airflow.providers.arenadata.ozone.operators.ozone_transfer import (
       LocalFilesystemToOzoneOperator,
       OzoneToOzoneOperator,
   )

   with DAG("ozone_transfer_example", schedule=None, start_date=None) as dag:
       upload = LocalFilesystemToOzoneOperator(
           task_id="upload_local_file",
           local_path="/tmp/input.csv",
           remote_path="ofs://om/landing/raw/input.csv",
           overwrite=True,
       )

       archive = OzoneToOzoneOperator(
           task_id="archive_file",
           source_path="ofs://om/landing/raw/input.csv",
           dest_path="ofs://om/archive/processed/input.csv",
       )

       upload >> archive
