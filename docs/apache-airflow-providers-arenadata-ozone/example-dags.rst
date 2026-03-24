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

This provider ships example DAGs that demonstrate native Ozone workflows.
Examples are configured via environment variables to simplify local runs
and CI checks without editing DAG source code.

Where examples are located
--------------------------

``airflow/providers/arenadata/ozone/example_dags/``

Main examples:

* ``example_ozone_usage.py``
* ``example_ozone_data_pipeline.py``
* ``example_ozone_data_lifecycle.py``
* ``example_ozone_multi_tenant_management.py``

What each example DAG does
--------------------------

``example_ozone_usage.py`` (plain / SSL / Kerberos / SSL+Kerberos):

* Creates a volume and bucket with admin operators.
* Creates an ``ofs://`` directory path in Ozone FS.
* Uploads inline text content to a file in that directory.
* Waits for the file with ``OzoneKeySensor``.
* Works in all Ozone security modes; pick mode via
  ``OZONE_EXAMPLE_USAGE_ADMIN_CONN_ID`` and corresponding ``Connection Extra``.
* Best for first smoke-check of provider installation and CLI connectivity.

``example_ozone_data_pipeline.py`` (trigger + migration):

* Waits for a trigger marker file in Ozone landing zone.
* Applies storage quota to target volume.
* Migrates data from HDFS to Ozone with ``HdfsToOzoneOperator`` (DistCp).
* Useful as a template for ingestion pipelines where Ozone is destination storage.

``example_ozone_data_lifecycle.py`` (archive + backup + cleanup):

* Ensures landing/archive volumes and buckets exist.
* Lists files in landing zone with ``OzoneListOperator``.
* Simulates processing stage (``TaskGroup`` with a processing task).
* Moves processed files into date-partitioned archive path.
* Creates a snapshot backup and then cleans original landing files.
* Demonstrates full lifecycle automation: discover -> process -> archive -> backup -> cleanup.

``example_ozone_multi_tenant_management.py`` (provisioning):

* Creates a dedicated project volume.
* Sets volume quota.
* Creates standard landing/processed buckets with bucket quotas.
* Creates standard ``data`` directories in each bucket.
* Useful for platform teams that provision isolated tenant/project storage.

How configuration is applied
----------------------------

Each DAG reads environment variables at parse time through local helper
functions inside example files. If a variable is not set, the DAG uses
its built-in default value.

Common variable
---------------

Used by all examples:

* ``OZONE_EXAMPLE_OM_HOST``

Per-example variables
---------------------

Basic usage (``example_ozone_usage.py``):

* ``OZONE_EXAMPLE_USAGE_ADMIN_CONN_ID``
* ``OZONE_EXAMPLE_USAGE_VOLUME``
* ``OZONE_EXAMPLE_USAGE_BUCKET``
* ``OZONE_EXAMPLE_USAGE_DIR``
* ``OZONE_EXAMPLE_USAGE_FILE``

To switch mode for this single usage DAG, point
``OZONE_EXAMPLE_USAGE_ADMIN_CONN_ID`` to a connection configured for:

* plain Ozone;
* Ozone + SSL;
* Ozone + Kerberos;
* Ozone + SSL + Kerberos.

For Kerberos mode, required connection-extra keys include
``hadoop_security_authentication=kerberos``, ``kerberos_principal``,
``kerberos_keytab``, ``krb5_conf``, and ``ozone_conf_dir``.

Data pipeline (``example_ozone_data_pipeline.py``):

* ``OZONE_EXAMPLE_PIPELINE_CONN_ID``
* ``OZONE_EXAMPLE_PIPELINE_HDFS_CONN_ID`` (optional)
* ``OZONE_EXAMPLE_PIPELINE_VOLUME``
* ``OZONE_EXAMPLE_PIPELINE_BUCKET``
* ``OZONE_EXAMPLE_PIPELINE_TRIGGER_FILE``
* ``OZONE_EXAMPLE_PIPELINE_QUOTA``
* ``OZONE_EXAMPLE_PIPELINE_SOURCE_PATH``
* ``OZONE_EXAMPLE_PIPELINE_DEST_SUBPATH``

Data lifecycle (``example_ozone_data_lifecycle.py``):

* ``OZONE_EXAMPLE_LIFECYCLE_CONN_ID``
* ``OZONE_EXAMPLE_LIFECYCLE_LANDING_VOLUME``
* ``OZONE_EXAMPLE_LIFECYCLE_LANDING_BUCKET``
* ``OZONE_EXAMPLE_LIFECYCLE_ARCHIVE_VOLUME``
* ``OZONE_EXAMPLE_LIFECYCLE_ARCHIVE_BUCKET``

Multi-tenant management (``example_ozone_multi_tenant_management.py``):

* ``OZONE_EXAMPLE_MULTI_TENANT_CONN_ID``
* ``OZONE_EXAMPLE_MULTI_TENANT_PROJECT_VOLUME``
* ``OZONE_EXAMPLE_MULTI_TENANT_PROJECT_QUOTA``
* ``OZONE_EXAMPLE_MULTI_TENANT_LANDING_BUCKET``
* ``OZONE_EXAMPLE_MULTI_TENANT_PROCESSED_BUCKET``
* ``OZONE_EXAMPLE_MULTI_TENANT_BUCKET_QUOTA``

DAG developer notes
-------------------

* Example DAG variables are for demonstration only and are not part of provider runtime API.
* Production DAGs should keep operational parameters in task args and Airflow Connections.
* Connection parsing for runtime behavior is defined by
  ``airflow/providers/arenadata/ozone/utils/connection_schema.py``.
* Custom DAG extensions can read non-provider extra keys through
  ``hook.connection_snapshot.raw_extra``.

Provider runtime tuning (for example runs)
------------------------------------------

Example DAGs use the same provider runtime policy as regular tasks.
Retry/timeout behavior is configured in provider code and per-task arguments:

* Hook defaults: ``RETRY_ATTEMPTS``, ``FAST_TIMEOUT_SECONDS``, ``SLOW_TIMEOUT_SECONDS``
  (see ``airflow/providers/arenadata/ozone/utils/connection_schema.py``).
* Operators/transfers/sensors can override ``retry_attempts`` and ``timeout``
  per task where needed.
