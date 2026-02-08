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

Filesystem operators
====================

Filesystem operators use the Native CLI (``ozone fs``) to work with ``ofs://`` / ``o3fs://`` paths.

Operators
---------

* ``airflow.providers.arenadata.ozone.operators.ozone_fs.OzoneFsMkdirOperator`` (mkdir -p)
* ``airflow.providers.arenadata.ozone.operators.ozone_fs.OzoneFsPutOperator`` (write string to a file)
* ``airflow.providers.arenadata.ozone.operators.ozone_fs.OzoneDeleteKeyOperator`` (delete a key/path; supports wildcards)
* ``airflow.providers.arenadata.ozone.operators.ozone_fs.OzoneListOperator`` (list paths; returns list via XCom)

Notes
-----

Wildcard delete
^^^^^^^^^^^^^^^

``OzoneDeleteKeyOperator`` supports wildcard patterns like ``ofs://om/vol/bucket/path/*``.
If nothing matches, the operator succeeds (idempotent cleanup).

Example
-------

.. code-block:: python

   from airflow import DAG
   from airflow.providers.arenadata.ozone.operators.ozone_fs import (
       OzoneFsMkdirOperator,
       OzoneFsPutOperator,
       OzoneListOperator,
       OzoneDeleteKeyOperator,
   )

   with DAG("ozone_fs_example", schedule=None, start_date=None) as dag:
       mkdir = OzoneFsMkdirOperator(
           task_id="mkdir",
           path="ofs://om/landing/raw/data_dir",
       )

       put_file = OzoneFsPutOperator(
           task_id="put_file",
           content="hello ozone",
           remote_path="ofs://om/landing/raw/data_dir/file.txt",
       )

       list_files = OzoneListOperator(
           task_id="list_files",
           path="ofs://om/landing/raw/data_dir",
       )

       cleanup = OzoneDeleteKeyOperator(
           task_id="cleanup",
           path="ofs://om/landing/raw/data_dir/*",
       )

       mkdir >> put_file >> list_files >> cleanup
