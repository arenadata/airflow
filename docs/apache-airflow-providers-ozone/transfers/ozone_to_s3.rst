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

Ozone to S3 (parallel backup)
=============================

``OzoneToS3Operator`` copies objects from an Ozone S3 bucket to an external S3 bucket.
It uses parallel workers to speed up bulk transfers.

Operator
--------

* ``airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneToS3Operator``

Connections
-----------

* Source (Ozone S3 Gateway): ``ozone_s3`` connection id (default ``ozone_s3_default``) with ``endpoint_url`` pointing to S3G.
* Target (external S3): AWS connection id (default ``aws_default``).

Example
-------

.. code-block:: python

   from airflow import DAG
   from airflow.providers.arenadata.ozone.transfers.ozone_to_s3 import OzoneToS3Operator

   with DAG("ozone_to_s3_example", schedule=None, start_date=None) as dag:
       backup = OzoneToS3Operator(
           task_id="backup",
           ozone_bucket="ozone-bucket",
           ozone_prefix="data/",
           s3_bucket="dr-bucket",
           s3_prefix="ozone-backup/data/",
           max_workers=10,
       )
