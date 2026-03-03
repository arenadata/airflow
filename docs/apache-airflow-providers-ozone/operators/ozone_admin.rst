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

Admin operators
===============

The admin operators manage Ozone **volumes**, **buckets** and **quotas** using the Native CLI (``ozone sh``).
They are implemented to be **idempotent**: "already exists" / "not found" is treated as success where applicable.

Operators
---------

* ``airflow.providers.arenadata.ozone.operators.ozone.OzoneCreateVolumeOperator``
* ``airflow.providers.arenadata.ozone.operators.ozone.OzoneCreateBucketOperator``
* ``airflow.providers.arenadata.ozone.operators.ozone.OzoneDeleteVolumeOperator``
* ``airflow.providers.arenadata.ozone.operators.ozone.OzoneDeleteBucketOperator``
* ``airflow.providers.arenadata.ozone.operators.ozone.OzoneSetQuotaOperator``

Important notes
---------------

Naming rules
^^^^^^^^^^^^

Ozone volume and bucket names must match the pattern ``[a-z0-9.-]+`` (lowercase letters, digits, dots and dashes).
Underscores are not allowed.

Quotas
^^^^^^

When a **volume has a space quota**, Ozone requires that **buckets inside that volume** also have space quotas.
If you set volume quota, pass ``quota=...`` when creating buckets as well.

Example
-------

.. code-block:: python

   from datetime import timedelta
   from airflow import DAG
   from airflow.providers.arenadata.ozone.operators.ozone import (
       OzoneCreateVolumeOperator,
       OzoneCreateBucketOperator,
       OzoneSetQuotaOperator,
   )

   with DAG("ozone_admin_example", schedule=None, start_date=None) as dag:
       create_volume = OzoneCreateVolumeOperator(
           task_id="create_volume",
           volume_name="landing",
           quota="10GB",
           execution_timeout=timedelta(minutes=2),
       )

       create_bucket = OzoneCreateBucketOperator(
           task_id="create_bucket",
           volume_name="landing",
           bucket_name="raw",
           quota="10GB",
       )

       set_bucket_quota = OzoneSetQuotaOperator(
           task_id="set_bucket_quota",
           volume="landing",
           bucket="raw",
           quota="10GB",
       )

       create_volume >> create_bucket >> set_bucket_quota
