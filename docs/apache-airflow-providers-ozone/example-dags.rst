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

Example DAGs
============

The provider ships example DAGs in the Airflow source tree (all Ozone examples follow
the ``example_ozone_*`` naming convention):

* ``airflow/providers/arenadata/ozone/example_dags/example_ozone_usage.py``
* ``airflow/providers/arenadata/ozone/example_dags/example_ozone_usage_ssl.py``
* ``airflow/providers/arenadata/ozone/example_dags/example_ozone_usage_ssl_kerberos.py``
* ``airflow/providers/arenadata/ozone/example_dags/example_ozone_data_pipeline.py``
* ``airflow/providers/arenadata/ozone/example_dags/example_ozone_multi_tenant_management.py``
* ``airflow/providers/arenadata/ozone/example_dags/example_ozone_cross_region_replication.py``
* ``airflow/providers/arenadata/ozone/example_dags/example_ozone_data_lifecycle.py``

What they demonstrate
---------------------

* ``example_ozone_usage``: basic native + S3 operations and sensors.
* ``example_ozone_usage_ssl``: native + S3 operations with SSL/TLS.
* ``example_ozone_usage_ssl_kerberos``: native operations with SSL/TLS + Kerberos.
* ``example_ozone_data_pipeline``: simple ETL with trigger sensor + quota + DistCp migration.
* ``example_ozone_multi_tenant_management``: provisioning a project volume/buckets + quotas + directory layout.
* ``example_ozone_cross_region_replication``: distcp-style replication between clusters.
* ``example_ozone_data_lifecycle``: end-to-end lifecycle (list → process → archive → hive partition → snapshot → cleanup).
