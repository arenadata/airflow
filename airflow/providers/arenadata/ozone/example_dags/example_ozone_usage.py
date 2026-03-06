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
Basic Ozone Usage Example DAG

This DAG demonstrates fundamental Ozone operations using both Native and S3 interfaces:
- Creates a volume and bucket via Native Admin CLI
- Creates directories and uploads files via Ozone Filesystem (ofs://)
- Creates buckets and uploads objects via S3 Gateway
- Uses Sensors to wait for files/objects to appear

This is the simplest example to get started with the Ozone provider.
It verifies that basic create, upload, and sensor operations work correctly.
S3 Gateway tasks use boto3 (no Amazon provider required).
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.arenadata.ozone.operators.ozone import (
    OzoneCreateBucketOperator,
    OzoneCreateVolumeOperator,
    OzoneFsMkdirOperator,
    OzoneFsPutOperator,
    OzoneS3CreateBucketOperator,
    OzoneS3PutObjectOperator,
)
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor, OzoneS3KeySensor
from airflow.providers.arenadata.ozone.utils.helpers import TypeNormalizationHelper
from airflow.utils import timezone


def get_env_str(name: str, default: str | None = None) -> str | None:
    return TypeNormalizationHelper.normalize_optional_str(os.getenv(name)) or default


OM_HOST = get_env_str("OZONE_EXAMPLE_OM_HOST", "om")
NATIVE_VOLUME = get_env_str("OZONE_EXAMPLE_USAGE_VOLUME", "vol1")
NATIVE_BUCKET = get_env_str("OZONE_EXAMPLE_USAGE_BUCKET", "bucket-native")
NATIVE_DIR = get_env_str("OZONE_EXAMPLE_USAGE_DIR", "data_dir")
NATIVE_FILE = get_env_str("OZONE_EXAMPLE_USAGE_FILE", "file.txt")
S3_BUCKET = get_env_str("OZONE_EXAMPLE_USAGE_S3_BUCKET", "s3bucket")
S3_KEY = get_env_str("OZONE_EXAMPLE_USAGE_S3_KEY", "s3_data/test.json")
S3_CONN_ID = get_env_str("OZONE_EXAMPLE_USAGE_S3_CONN_ID", "ozone_s3_default")
ADMIN_CONN_ID = get_env_str("OZONE_EXAMPLE_USAGE_ADMIN_CONN_ID", "ozone_admin_default")
FS_FILE_PATH = f"ofs://{OM_HOST}/{NATIVE_VOLUME}/{NATIVE_BUCKET}/{NATIVE_DIR}/{NATIVE_FILE}"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "example_ozone_usage",
    start_date=timezone.datetime(2024, 1, 1),
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["ozone", "example"],
) as dag:
    create_vol = OzoneCreateVolumeOperator(
        task_id="create_volume",
        volume_name=NATIVE_VOLUME,
        quota="10GB",
        ozone_conn_id=ADMIN_CONN_ID,
        execution_timeout=timedelta(minutes=5),
    )

    create_bucket_native = OzoneCreateBucketOperator(
        task_id="create_bucket_native",
        volume_name=NATIVE_VOLUME,
        bucket_name=NATIVE_BUCKET,
        quota="10GB",
        ozone_conn_id=ADMIN_CONN_ID,
        execution_timeout=timedelta(minutes=5),
    )

    fs_mkdir = OzoneFsMkdirOperator(
        task_id="fs_mkdir",
        path=f"ofs://{OM_HOST}/{NATIVE_VOLUME}/{NATIVE_BUCKET}/{NATIVE_DIR}",
        ozone_conn_id=ADMIN_CONN_ID,
        execution_timeout=timedelta(minutes=5),
    )

    fs_put = OzoneFsPutOperator(
        task_id="fs_put_file",
        content="Hello from FS Layer",
        remote_path=FS_FILE_PATH,
        ozone_conn_id=ADMIN_CONN_ID,
        execution_timeout=timedelta(minutes=5),
    )

    wait_fs_file = OzoneKeySensor(
        task_id="wait_fs_file",
        path=FS_FILE_PATH,
        ozone_conn_id=ADMIN_CONN_ID,
        mode="reschedule",
        timeout=60,
    )

    s3_create_bucket = OzoneS3CreateBucketOperator(
        task_id="s3_create_bucket",
        bucket_name=S3_BUCKET,
        ozone_conn_id=S3_CONN_ID,
        execution_timeout=timedelta(minutes=5),
    )

    s3_put = OzoneS3PutObjectOperator(
        task_id="s3_put",
        bucket_name=S3_BUCKET,
        key=S3_KEY,
        data='{"message": "Hello from S3 Layer"}',
        ozone_conn_id=S3_CONN_ID,
        execution_timeout=timedelta(minutes=5),
    )

    wait_s3_file = OzoneS3KeySensor(
        task_id="wait_s3_file",
        bucket_name=S3_BUCKET,
        bucket_key=S3_KEY,
        ozone_conn_id=S3_CONN_ID,
        mode="reschedule",
        timeout=60,
    )

    create_vol >> create_bucket_native >> fs_mkdir >> fs_put >> wait_fs_file
    s3_create_bucket >> s3_put >> wait_s3_file
