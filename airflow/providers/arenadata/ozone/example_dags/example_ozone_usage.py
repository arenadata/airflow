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

from datetime import timedelta

import pendulum

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

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "example_ozone_usage",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["ozone", "example"],
) as dag:
    # 1. Native Admin: Create a volume named 'vol1'
    create_vol = OzoneCreateVolumeOperator(
        task_id="create_volume",
        volume_name="vol1",
        quota="10GB",
        execution_timeout=timedelta(minutes=5),
    )

    # 2. Native Admin: Create a bucket 'bucket-native' inside 'vol1'
    create_bucket_native = OzoneCreateBucketOperator(
        task_id="create_bucket_native",
        volume_name="vol1",
        bucket_name="bucket-native",
        quota="10GB",
        execution_timeout=timedelta(minutes=5),
    )

    # 3. FS Layer: Create a directory via ofs://
    fs_mkdir = OzoneFsMkdirOperator(
        task_id="fs_mkdir",
        path="ofs://om/vol1/bucket-native/data_dir",
        execution_timeout=timedelta(minutes=5),
    )

    # 4. FS Layer: Put a file via ofs://
    fs_put = OzoneFsPutOperator(
        task_id="fs_put_file",
        content="Hello from FS Layer",
        remote_path="ofs://om/vol1/bucket-native/data_dir/file.txt",
        execution_timeout=timedelta(minutes=5),
    )

    # 5. FS Sensor: Wait for file to appear
    wait_fs_file = OzoneKeySensor(
        task_id="wait_fs_file",
        path="ofs://om/vol1/bucket-native/data_dir/file.txt",
        mode="reschedule",
        timeout=60,
    )

    # 6. S3 Layer: Create bucket via S3 Gateway (if not exists)
    s3_create_bucket = OzoneS3CreateBucketOperator(
        task_id="s3_create_bucket",
        bucket_name="s3bucket",
        ozone_conn_id="ozone_s3_default",
        execution_timeout=timedelta(minutes=5),
    )

    # 7. S3 Layer: Put a file into a bucket
    s3_put = OzoneS3PutObjectOperator(
        task_id="s3_put",
        bucket_name="s3bucket",
        key="s3_data/test.json",
        data='{"message": "Hello from S3 Layer"}',
        ozone_conn_id="ozone_s3_default",
        execution_timeout=timedelta(minutes=5),
    )

    # 8. S3 Sensor: Check file
    wait_s3_file = OzoneS3KeySensor(
        task_id="wait_s3_file",
        bucket_name="s3bucket",
        bucket_key="s3_data/test.json",
        ozone_conn_id="ozone_s3_default",
        mode="reschedule",
        timeout=60,
    )

    create_vol >> create_bucket_native >> fs_mkdir >> fs_put >> wait_fs_file
    s3_create_bucket >> s3_put >> wait_s3_file
