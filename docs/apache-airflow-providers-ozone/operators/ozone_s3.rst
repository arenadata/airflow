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

S3 Gateway operators
====================

S3 Gateway operators interact with Ozone via the S3-compatible API, using the provider's own
``OzoneS3Hook`` (boto3-based) and an ``ozone_s3`` connection (for example ``ozone_s3_default``).

Operators
---------

* ``airflow.providers.arenadata.ozone.operators.ozone_s3.OzoneS3CreateBucketOperator``
* ``airflow.providers.arenadata.ozone.operators.ozone_s3.OzoneS3PutObjectOperator``

Connection notes
----------------

Configure the ``ozone_s3`` connection Extra with:

* ``endpoint_url``: e.g. ``http://s3g:9878`` or ``https://s3g:9879``
* ``verify``: ``false`` (development only) or a path to a CA bundle

Example
-------

.. code-block:: python

   from airflow import DAG
   from airflow.providers.arenadata.ozone.operators.ozone_s3 import (
       OzoneS3CreateBucketOperator,
       OzoneS3PutObjectOperator,
   )

   with DAG("ozone_s3_example", schedule=None, start_date=None) as dag:
       create_bucket = OzoneS3CreateBucketOperator(
           task_id="create_bucket",
           bucket_name="s3bucket",
           ozone_conn_id="ozone_s3_default",
       )

       put_object = OzoneS3PutObjectOperator(
           task_id="put_object",
           bucket_name="s3bucket",
           key="data/test.json",
           data='{"hello": "ozone-s3"}',
           ozone_conn_id="ozone_s3_default",
       )

       create_bucket >> put_object
