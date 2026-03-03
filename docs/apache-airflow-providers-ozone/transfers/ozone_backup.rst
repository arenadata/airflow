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

Ozone snapshot backup
====================

``OzoneBackupOperator`` creates an Ozone snapshot of a bucket using the Native CLI hook
(``OzoneAdminHook.run_cli``).
The operation is idempotent: if Ozone returns ``FILE_ALREADY_EXISTS`` in CLI stderr,
or a human-readable ``snapshot already exists`` marker (case-insensitive),
the snapshot is treated as already created and the task succeeds.

Operator
--------

* ``airflow.providers.arenadata.ozone.transfers.ozone_backup.OzoneBackupOperator``

Example
-------

.. code-block:: python

   from airflow import DAG
   from airflow.providers.arenadata.ozone.transfers.ozone_backup import OzoneBackupOperator

   with DAG("ozone_backup_example", schedule=None, start_date=None) as dag:
       backup = OzoneBackupOperator(
           task_id="snapshot",
           volume="archive",
           bucket="processed",
           snapshot_name="snap-{{ ds_nodash }}",
       )
