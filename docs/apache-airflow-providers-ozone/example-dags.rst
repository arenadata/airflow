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

The provider ships example DAGs in the Airflow source tree:

* ``airflow/providers/arenadata/ozone/example_dags/ozone_usage_example.py``
* ``airflow/providers/arenadata/ozone/example_dags/ozone_usage_ssl_example.py``
* ``airflow/providers/arenadata/ozone/example_dags/ozone_usage_ssl_kerberos_example.py``
* ``airflow/providers/arenadata/ozone/example_dags/ozone_data_pipeline.py``
* ``airflow/providers/arenadata/ozone/example_dags/ozone_multi_tenant_management.py``
* ``airflow/providers/arenadata/ozone/example_dags/ozone_cross_region_replication.py``
* ``airflow/providers/arenadata/ozone/example_dags/ozone_data_lifecycle.py``

What they demonstrate
---------------------

* ``ozone_usage_example``: basic native + S3 operations and sensors.
* ``ozone_usage_ssl_example``: native + S3 operations with SSL/TLS.
* ``ozone_usage_ssl_kerberos_example``: native operations with SSL/TLS + Kerberos.
* ``ozone_data_pipeline``: simple ETL with trigger sensor + quota + DistCp migration.
* ``ozone_multi_tenant_management``: provisioning a project volume/buckets + quotas + directory layout.
* ``ozone_cross_region_replication``: distcp-style replication between clusters.
* ``ozone_data_lifecycle``: end-to-end lifecycle (list → process → archive → hive partition → snapshot → cleanup).
