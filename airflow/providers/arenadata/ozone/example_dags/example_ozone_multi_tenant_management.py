#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Multi-Tenant Management Example DAG

This DAG demonstrates how to automate the setup of storage resources for new projects/tenants:
1. Creates a dedicated volume for a new project
2. Sets quota limits to prevent resource overuse
3. Creates standard directory structure (landing, processed, etc.)

This example shows how Airflow can be used for administrative tasks,
automating the provisioning of storage resources for new teams or projects.
Useful for multi-tenant environments where each project needs isolated storage.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.models.dag import DAG
from airflow.providers.arenadata.ozone.operators.ozone_admin import (
    OzoneCreateBucketOperator,
    OzoneCreateVolumeOperator,
    OzoneSetQuotaOperator,
)
from airflow.providers.arenadata.ozone.operators.ozone_fs import OzoneFsMkdirOperator

# Define a project name, which could come from a Trigger UI config
# Note: Ozone volume and bucket names must match the pattern `[a-z0-9.-]+`
# (lowercase letters, digits, dots and dashes; underscores are not allowed).
PROJECT_NAME = "project-alpha"
PROJECT_VOLUME = "project-alpha"  # Use project name as volume name (no _vol suffix)
PROJECT_QUOTA = "10GB"  # Reduced for test environment
# When volume has quota, buckets must also have quota
BUCKET_QUOTA = "1GB"

with DAG(
    dag_id="example_ozone_multi_tenant_management",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["ozone", "example"],
) as dag:
    # 1. Create a dedicated volume for the new project
    create_volume = OzoneCreateVolumeOperator(
        task_id="create_project_volume",
        volume_name=PROJECT_VOLUME,
        execution_timeout=timedelta(minutes=1),
    )

    # 2. Set a hard limit on the volume to prevent overuse
    set_quota = OzoneSetQuotaOperator(
        task_id="set_project_quota",
        volume=PROJECT_VOLUME,
        quota=PROJECT_QUOTA,
        execution_timeout=timedelta(minutes=1),
    )

    # 3. Create buckets with quotas (required when volume has quota)
    create_landing_bucket = OzoneCreateBucketOperator(
        task_id="create_landing_bucket",
        volume_name=PROJECT_VOLUME,
        bucket_name="landing",
        quota=BUCKET_QUOTA,
        execution_timeout=timedelta(minutes=1),
    )

    create_processed_bucket = OzoneCreateBucketOperator(
        task_id="create_processed_bucket",
        volume_name=PROJECT_VOLUME,
        bucket_name="processed",
        quota=BUCKET_QUOTA,
        execution_timeout=timedelta(minutes=1),
    )

    # 4. Create standard subdirectories inside the buckets
    create_landing_dir = OzoneFsMkdirOperator(
        task_id="create_landing_dir",
        path=f"ofs://om/{PROJECT_VOLUME}/landing/data",
        execution_timeout=timedelta(minutes=1),
    )

    create_processed_dir = OzoneFsMkdirOperator(
        task_id="create_processed_dir",
        path=f"ofs://om/{PROJECT_VOLUME}/processed/data",
        execution_timeout=timedelta(minutes=1),
    )

    create_volume >> set_quota
    set_quota >> [create_landing_bucket, create_processed_bucket]
    create_landing_bucket >> create_landing_dir
    create_processed_bucket >> create_processed_dir
