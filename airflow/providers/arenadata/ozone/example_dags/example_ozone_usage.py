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

This DAG demonstrates fundamental native Ozone operations:
- Creates a volume and bucket via Native Admin CLI
- Creates directories and uploads files via Ozone Filesystem (ofs://)
- Uses a Sensor to wait for a file to appear

This is the simplest example to get started with the Ozone provider.
It verifies that basic create, upload, and sensor operations work correctly.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.arenadata.ozone.operators.ozone import (
    OzoneCreateBucketOperator,
    OzoneCreatePathOperator,
    OzoneCreateVolumeOperator,
    OzoneUploadContentOperator,
)
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor
from airflow.utils import timezone


def _example_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip()
    return normalized if normalized else default


OM_HOST = _example_env("OZONE_EXAMPLE_OM_HOST", "om")
NATIVE_VOLUME = _example_env("OZONE_EXAMPLE_USAGE_VOLUME", "vol1")
NATIVE_BUCKET = _example_env("OZONE_EXAMPLE_USAGE_BUCKET", "bucket-native")
NATIVE_DIR = _example_env("OZONE_EXAMPLE_USAGE_DIR", "data_dir")
NATIVE_FILE = _example_env("OZONE_EXAMPLE_USAGE_FILE", "file.txt")
ADMIN_CONN_ID = _example_env("OZONE_EXAMPLE_USAGE_ADMIN_CONN_ID", "ozone_admin_default")
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

    fs_mkdir = OzoneCreatePathOperator(
        task_id="fs_mkdir",
        path=f"ofs://{OM_HOST}/{NATIVE_VOLUME}/{NATIVE_BUCKET}/{NATIVE_DIR}",
        ozone_conn_id=ADMIN_CONN_ID,
        execution_timeout=timedelta(minutes=5),
    )

    fs_put = OzoneUploadContentOperator(
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

    create_vol >> create_bucket_native >> fs_mkdir >> fs_put >> wait_fs_file
