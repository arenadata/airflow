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

Example DAG configuration
=========================

Ozone provider example DAGs are configurable via environment variables.
This follows the common Airflow provider pattern used by other providers
for test and demo scenarios.

How configuration is applied
----------------------------

Each example DAG defines a small local ``get_env_str()`` helper and reads
values at DAG parse time. That helper uses
``airflow.providers.arenadata.ozone.utils.helpers.TypeNormalizationHelper.normalize_optional_str()``
to normalize environment values.
If a variable is not set, each DAG uses a safe default value.

For local testing with helper scripts:

* ``files/run_ozone.sh``
* ``files/run_ozone_ssl.sh``
* ``files/run_ozone_kerberos_ssl.sh``
* ``files/run_af.sh``

these scripts preconfigure and pass environment variables into the Airflow
container so the examples can run without editing DAG sources.

Main variables
--------------

Common:

* ``OZONE_EXAMPLE_OM_HOST``

Basic example (``example_ozone_usage``):

* ``OZONE_EXAMPLE_USAGE_ADMIN_CONN_ID``
* ``OZONE_EXAMPLE_USAGE_S3_CONN_ID``
* ``OZONE_EXAMPLE_USAGE_VOLUME``
* ``OZONE_EXAMPLE_USAGE_BUCKET``
* ``OZONE_EXAMPLE_USAGE_DIR``
* ``OZONE_EXAMPLE_USAGE_FILE``
* ``OZONE_EXAMPLE_USAGE_S3_BUCKET``
* ``OZONE_EXAMPLE_USAGE_S3_KEY``

SSL example (``example_ozone_usage_ssl``):

* ``OZONE_EXAMPLE_SSL_ADMIN_CONN_ID``
* ``OZONE_EXAMPLE_SSL_S3_CONN_ID``
* ``OZONE_EXAMPLE_SSL_VOLUME``
* ``OZONE_EXAMPLE_SSL_BUCKET``
* ``OZONE_EXAMPLE_SSL_DIR``
* ``OZONE_EXAMPLE_SSL_FILE``
* ``OZONE_EXAMPLE_SSL_S3_BUCKET``
* ``OZONE_EXAMPLE_SSL_S3_KEY``

SSL + Kerberos example (``example_ozone_usage_ssl_kerberos``):

* ``OZONE_EXAMPLE_KRB_ADMIN_CONN_ID``
* ``OZONE_EXAMPLE_KRB_S3_CONN_ID``
* ``OZONE_EXAMPLE_KRB_VOLUME``
* ``OZONE_EXAMPLE_KRB_BUCKET``
* ``OZONE_EXAMPLE_KRB_DIR``
* ``OZONE_EXAMPLE_KRB_FILE``
* ``OZONE_EXAMPLE_KRB_S3_BUCKET``
* ``OZONE_EXAMPLE_KRB_S3_KEY``

Data pipeline example (``example_ozone_data_pipeline``):

* ``OZONE_EXAMPLE_PIPELINE_CONN_ID``
* ``OZONE_EXAMPLE_PIPELINE_HDFS_CONN_ID`` (optional)
* ``OZONE_EXAMPLE_PIPELINE_VOLUME``
* ``OZONE_EXAMPLE_PIPELINE_BUCKET``
* ``OZONE_EXAMPLE_PIPELINE_TRIGGER_FILE``
* ``OZONE_EXAMPLE_PIPELINE_QUOTA``
* ``OZONE_EXAMPLE_PIPELINE_SOURCE_PATH``
* ``OZONE_EXAMPLE_PIPELINE_DEST_SUBPATH``

Data lifecycle example (``example_ozone_data_lifecycle``):

* ``OZONE_EXAMPLE_LIFECYCLE_CONN_ID``
* ``OZONE_EXAMPLE_LIFECYCLE_HIVE_CONN_ID``
* ``OZONE_EXAMPLE_LIFECYCLE_LANDING_VOLUME``
* ``OZONE_EXAMPLE_LIFECYCLE_LANDING_BUCKET``
* ``OZONE_EXAMPLE_LIFECYCLE_ARCHIVE_VOLUME``
* ``OZONE_EXAMPLE_LIFECYCLE_ARCHIVE_BUCKET``
* ``OZONE_EXAMPLE_LIFECYCLE_HIVE_TABLE``

Multi-tenant example (``example_ozone_multi_tenant_management``):

* ``OZONE_EXAMPLE_MULTI_TENANT_CONN_ID``
* ``OZONE_EXAMPLE_MULTI_TENANT_PROJECT_VOLUME``
* ``OZONE_EXAMPLE_MULTI_TENANT_PROJECT_QUOTA``
* ``OZONE_EXAMPLE_MULTI_TENANT_LANDING_BUCKET``
* ``OZONE_EXAMPLE_MULTI_TENANT_PROCESSED_BUCKET``
* ``OZONE_EXAMPLE_MULTI_TENANT_BUCKET_QUOTA``

Cross-region replication example (``example_ozone_cross_region_replication``):

* ``OZONE_EXAMPLE_REPLICATION_SOURCE_CLUSTER``
* ``OZONE_EXAMPLE_REPLICATION_TARGET_CLUSTER``
* ``OZONE_EXAMPLE_REPLICATION_SOURCE_BASE``
* ``OZONE_EXAMPLE_REPLICATION_TARGET_BASE``
* ``OZONE_EXAMPLE_REPLICATION_HDFS_CONN_ID`` (optional)
* ``OZONE_EXAMPLE_REPLICATION_SCHEDULE``

Provider runtime tuning (for example DAG runs)
----------------------------------------------

Example DAGs use the same provider runtime policy as regular tasks.
Retry and timeout behavior is configured in provider code and task parameters:

* Hook defaults: ``RETRY_ATTEMPTS``, ``FAST_TIMEOUT_SECONDS``, ``SLOW_TIMEOUT_SECONDS``
  (see ``airflow/providers/arenadata/ozone/hooks/ozone.py``).
* Operators/transfers/sensors can override ``retry_attempts`` and ``timeout``
  per task where needed.
