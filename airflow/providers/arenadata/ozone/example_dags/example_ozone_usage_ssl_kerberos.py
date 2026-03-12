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

from datetime import timedelta

from airflow import DAG
from airflow.providers.arenadata.ozone.operators.ozone import (
    OzoneCreateBucketOperator,
    OzoneCreatePathOperator,
    OzoneCreateVolumeOperator,
    OzoneS3CreateBucketOperator,
    OzoneS3PutObjectOperator,
    OzoneUploadContentOperator,
)
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor, OzoneS3KeySensor
from airflow.providers.arenadata.ozone.utils import EnvHelper
from airflow.utils import timezone

OM_HOST = EnvHelper.get_env_str("OZONE_EXAMPLE_OM_HOST", "om")
KRB_ADMIN_CONN_ID = EnvHelper.get_env_str("OZONE_EXAMPLE_KRB_ADMIN_CONN_ID", "ozone_admin_ssl_kerberos")
KRB_S3_CONN_ID = EnvHelper.get_env_str("OZONE_EXAMPLE_KRB_S3_CONN_ID", "ozone_s3_ssl_kerberos")
KRB_VOLUME = EnvHelper.get_env_str("OZONE_EXAMPLE_KRB_VOLUME", "vol1")
KRB_BUCKET = EnvHelper.get_env_str("OZONE_EXAMPLE_KRB_BUCKET", "bucket-native")
KRB_DIR = EnvHelper.get_env_str("OZONE_EXAMPLE_KRB_DIR", "data_dir")
KRB_FILE = EnvHelper.get_env_str("OZONE_EXAMPLE_KRB_FILE", "file.txt")
KRB_S3_BUCKET = EnvHelper.get_env_str("OZONE_EXAMPLE_KRB_S3_BUCKET", "s3bucket-ssl-kerberos")
KRB_S3_KEY = EnvHelper.get_env_str("OZONE_EXAMPLE_KRB_S3_KEY", "s3_data/test.json")
KRB_FS_FILE_PATH = f"ofs://{OM_HOST}/{KRB_VOLUME}/{KRB_BUCKET}/{KRB_DIR}/{KRB_FILE}"

default_args = {
    "owner": "ozone-admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=2),
}

with DAG(
    "example_ozone_usage_ssl_kerberos",
    default_args=default_args,
    description="Example DAG demonstrating Ozone operations with SSL/TLS encryption and Kerberos authentication",
    schedule=None,
    start_date=timezone.datetime(2024, 1, 1),
    catchup=False,
    tags=["ozone", "example"],
) as dag:
    create_volume = OzoneCreateVolumeOperator(
        task_id="create_volume_ssl_kerberos",
        volume_name=KRB_VOLUME,
        quota="10GB",
        ozone_conn_id=KRB_ADMIN_CONN_ID,
        execution_timeout=timedelta(minutes=1),
    )

    create_bucket_native = OzoneCreateBucketOperator(
        task_id="create_bucket_native_ssl_kerberos",
        volume_name=KRB_VOLUME,
        bucket_name=KRB_BUCKET,
        quota="10GB",
        ozone_conn_id=KRB_ADMIN_CONN_ID,
        execution_timeout=timedelta(minutes=1),
    )

    fs_mkdir = OzoneCreatePathOperator(
        task_id="fs_mkdir_ssl_kerberos",
        path=f"ofs://{OM_HOST}/{KRB_VOLUME}/{KRB_BUCKET}/{KRB_DIR}",
        ozone_conn_id=KRB_ADMIN_CONN_ID,
        execution_timeout=timedelta(minutes=1),
    )

    fs_put_file = OzoneUploadContentOperator(
        task_id="fs_put_file_ssl_kerberos",
        content="Hello from FS Layer with SSL encryption and Kerberos authentication",
        remote_path=KRB_FS_FILE_PATH,
        ozone_conn_id=KRB_ADMIN_CONN_ID,
        execution_timeout=timedelta(minutes=1),
    )

    wait_fs_file = OzoneKeySensor(
        task_id="wait_fs_file_ssl_kerberos",
        path=KRB_FS_FILE_PATH,
        ozone_conn_id=KRB_ADMIN_CONN_ID,
        mode="reschedule",
        timeout=60,
        poke_interval=5,
    )

    s3_create_bucket = OzoneS3CreateBucketOperator(
        task_id="s3_create_bucket_ssl",
        bucket_name=KRB_S3_BUCKET,
        ozone_conn_id=KRB_S3_CONN_ID,
        execution_timeout=timedelta(minutes=1),
    )

    s3_put = OzoneS3PutObjectOperator(
        task_id="s3_put_ssl",
        bucket_name=KRB_S3_BUCKET,
        key=KRB_S3_KEY,
        data='{"test": "data", "ssl": true, "kerberos": "not_applicable_for_s3"}',
        ozone_conn_id=KRB_S3_CONN_ID,
        execution_timeout=timedelta(minutes=1),
    )

    wait_s3_file = OzoneS3KeySensor(
        task_id="wait_s3_file_ssl",
        bucket_name=KRB_S3_BUCKET,
        bucket_key=KRB_S3_KEY,
        ozone_conn_id=KRB_S3_CONN_ID,
        mode="reschedule",
        timeout=60,
        poke_interval=5,
    )
    create_volume >> create_bucket_native >> fs_mkdir >> fs_put_file >> wait_fs_file
    create_volume >> s3_create_bucket >> s3_put >> wait_s3_file
