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
Example DAG demonstrating Ozone operations with SSL/TLS encryption and Kerberos authentication.

This DAG shows how to use the Ozone provider with both SSL/TLS encryption and Kerberos
authentication enabled. It performs the same operations as example_ozone_usage_ssl.py
but uses Kerberos for authentication instead of simple authentication.

Prerequisites:
1. Ozone cluster must be configured with SSL/TLS + Kerberos (see README.md)
2. Airflow connections must be configured with SSL + Kerberos parameters:
   - ozone_admin_ssl_kerberos: Ozone Native CLI connection with SSL + Kerberos config
   - ozone_s3_ssl_kerberos: Ozone S3 Gateway connection with HTTPS endpoint
     (Note: S3 Gateway uses AWS Signature V4, not Kerberos, but SSL is still used)

Note: This example uses test/self-signed certificates and test Kerberos realm.
For production, use certificates from a trusted CA and proper Kerberos KDC setup.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.arenadata.ozone.operators.ozone_admin import (
    OzoneCreateBucketOperator,
    OzoneCreateVolumeOperator,
)
from airflow.providers.arenadata.ozone.operators.ozone_fs import (
    OzoneFsMkdirOperator,
    OzoneFsPutOperator,
)
from airflow.providers.arenadata.ozone.operators.ozone_s3 import (
    OzoneS3CreateBucketOperator,
    OzoneS3PutObjectOperator,
)
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor
from airflow.providers.arenadata.ozone.sensors.ozone_s3 import OzoneS3KeySensor

# Default arguments for the DAG
default_args = {
    "owner": "ozone-admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    # SSL + Kerberos operations may take longer due to authentication and certificate validation
    # Most operations: 2 minutes, sensors: 1 minute
    "execution_timeout": timedelta(minutes=2),
}

# Create the DAG
with DAG(
    "example_ozone_usage_ssl_kerberos",
    default_args=default_args,
    description="Example DAG demonstrating Ozone operations with SSL/TLS encryption and Kerberos authentication",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ozone", "example"],
) as dag:
    # Task 1: Create volume using SSL + Kerberos connection
    create_volume = OzoneCreateVolumeOperator(
        task_id="create_volume_ssl_kerberos",
        volume_name="vol1",
        quota="10GB",
        ozone_conn_id="ozone_admin_ssl_kerberos",  # SSL + Kerberos enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 2: Create bucket in volume using SSL + Kerberos connection
    create_bucket_native = OzoneCreateBucketOperator(
        task_id="create_bucket_native_ssl_kerberos",
        volume_name="vol1",
        bucket_name="bucket-native",
        quota="10GB",
        ozone_conn_id="ozone_admin_ssl_kerberos",  # SSL + Kerberos enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 3: Create directory in Ozone FS using SSL + Kerberos connection
    fs_mkdir = OzoneFsMkdirOperator(
        task_id="fs_mkdir_ssl_kerberos",
        path="ofs://om/vol1/bucket-native/data_dir",
        ozone_conn_id="ozone_admin_ssl_kerberos",  # SSL + Kerberos enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 4: Upload file to Ozone FS using SSL + Kerberos connection
    fs_put_file = OzoneFsPutOperator(
        task_id="fs_put_file_ssl_kerberos",
        content="Hello from FS Layer with SSL encryption and Kerberos authentication",
        remote_path="ofs://om/vol1/bucket-native/data_dir/file.txt",
        ozone_conn_id="ozone_admin_ssl_kerberos",  # SSL + Kerberos enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 5: Wait for file in Ozone FS using SSL + Kerberos connection
    wait_fs_file = OzoneKeySensor(
        task_id="wait_fs_file_ssl_kerberos",
        path="ofs://om/vol1/bucket-native/data_dir/file.txt",
        ozone_conn_id="ozone_admin_ssl_kerberos",  # SSL + Kerberos enabled connection
        timeout=60,  # 1 minute total
        poke_interval=5,  # Check every 5 seconds
    )

    # Task 6: Create S3 bucket using HTTPS connection
    # Note: S3 Gateway uses AWS Signature V4 for authentication, not Kerberos
    s3_create_bucket = OzoneS3CreateBucketOperator(
        task_id="s3_create_bucket_ssl",
        bucket_name="s3bucket-ssl-kerberos",
        ozone_conn_id="ozone_s3_ssl_kerberos",  # HTTPS-enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 7: Upload file to S3 using HTTPS connection
    s3_put = OzoneS3PutObjectOperator(
        task_id="s3_put_ssl",
        bucket_name="s3bucket-ssl-kerberos",
        key="s3_data/test.json",
        data='{"test": "data", "ssl": true, "kerberos": "not_applicable_for_s3"}',
        ozone_conn_id="ozone_s3_ssl_kerberos",  # HTTPS-enabled connection
        execution_timeout=timedelta(minutes=1),
    )

    # Task 8: Wait for S3 key using HTTPS connection
    wait_s3_file = OzoneS3KeySensor(
        task_id="wait_s3_file_ssl",
        bucket_name="s3bucket-ssl-kerberos",
        bucket_key="s3_data/test.json",
        ozone_conn_id="ozone_s3_ssl_kerberos",  # HTTPS-enabled connection
        timeout=60,  # 1 minute total
        poke_interval=5,  # Check every 5 seconds
    )

    # Define task dependencies
    create_volume >> create_bucket_native >> fs_mkdir >> fs_put_file >> wait_fs_file
    create_volume >> s3_create_bucket >> s3_put >> wait_s3_file
