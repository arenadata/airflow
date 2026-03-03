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
Example DAG demonstrating Ozone operations with SSL/TLS encryption.

This DAG shows how to use the Ozone provider with SSL/TLS enabled connections.
It performs the same operations as example_ozone_usage.py but uses encrypted connections.

Prerequisites:
1. Ozone cluster must be configured with SSL/TLS (see README.md)
2. Airflow connections must be configured with SSL parameters:
   - ozone_admin_ssl: Ozone Native CLI connection with SSL config
   - ozone_s3_ssl: Ozone S3 Gateway connection with HTTPS endpoint

Note: This example uses test/self-signed certificates. For production, use
certificates from a trusted CA.
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

# Default arguments for the DAG
default_args = {
    "owner": "ozone-admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    # SSL operations may take longer due to certificate validation
    # Most operations: 2 minutes, sensors: 1 minute
    "execution_timeout": timedelta(minutes=2),
}

# Create the DAG
with DAG(
    "example_ozone_usage_ssl",
    default_args=default_args,
    description="Example DAG demonstrating Ozone operations with SSL/TLS encryption",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ozone", "example"],
) as dag:
    # Task 1: Create volume using SSL connection
    create_volume = OzoneCreateVolumeOperator(
        task_id="create_volume_ssl",
        volume_name="vol1",
        quota="10GB",
        ozone_conn_id="ozone_admin_ssl",  # SSL-enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 2: Create bucket in volume using SSL connection
    create_bucket_native = OzoneCreateBucketOperator(
        task_id="create_bucket_native_ssl",
        volume_name="vol1",
        bucket_name="bucket-native",
        quota="10GB",
        ozone_conn_id="ozone_admin_ssl",  # SSL-enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 3: Create directory in Ozone FS using SSL connection
    fs_mkdir = OzoneFsMkdirOperator(
        task_id="fs_mkdir_ssl",
        path="ofs://om/vol1/bucket-native/data_dir",
        ozone_conn_id="ozone_admin_ssl",  # SSL-enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 4: Upload file to Ozone FS using SSL connection
    fs_put_file = OzoneFsPutOperator(
        task_id="fs_put_file_ssl",
        content="Hello from FS Layer with SSL encryption",
        remote_path="ofs://om/vol1/bucket-native/data_dir/file.txt",
        ozone_conn_id="ozone_admin_ssl",  # SSL-enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 5: Wait for file in Ozone FS using SSL connection
    wait_fs_file = OzoneKeySensor(
        task_id="wait_fs_file_ssl",
        path="ofs://om/vol1/bucket-native/data_dir/file.txt",
        ozone_conn_id="ozone_admin_ssl",  # SSL-enabled connection
        mode="reschedule",
        timeout=60,  # 1 minute total
        poke_interval=5,  # Check every 5 seconds
    )

    # Task 6: Create S3 bucket using HTTPS connection
    s3_create_bucket = OzoneS3CreateBucketOperator(
        task_id="s3_create_bucket_ssl",
        bucket_name="s3bucket-ssl",
        ozone_conn_id="ozone_s3_ssl",  # HTTPS-enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 7: Upload file to S3 using HTTPS connection
    s3_put = OzoneS3PutObjectOperator(
        task_id="s3_put_ssl",
        bucket_name="s3bucket-ssl",
        key="s3_data/test.json",
        data='{"test": "data", "ssl": true}',
        ozone_conn_id="ozone_s3_ssl",  # HTTPS-enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 8: Wait for S3 key using HTTPS connection
    wait_s3_file = OzoneS3KeySensor(
        task_id="wait_s3_file_ssl",
        bucket_name="s3bucket-ssl",
        bucket_key="s3_data/test.json",
        ozone_conn_id="ozone_s3_ssl",  # HTTPS-enabled connection
        mode="reschedule",
        timeout=60,  # 1 minute total
        poke_interval=5,  # Check every 5 seconds
    )

    # Define task dependencies
    create_volume >> create_bucket_native >> fs_mkdir >> fs_put_file >> wait_fs_file
    create_volume >> s3_create_bucket >> s3_put >> wait_s3_file
