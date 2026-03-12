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
ETL Data Pipeline Example DAG

This DAG demonstrates a typical ETL workflow with Ozone:
1. Waits for a trigger file to appear (using OzoneKeySensor)
2. Sets storage quota for the destination volume
3. Migrates data from HDFS to Ozone using HdfsToOzoneOperator (distcp)

This example shows how to integrate Ozone into existing Hadoop/HDFS workflows
and perform large-scale data migrations efficiently.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.providers.arenadata.ozone.operators.ozone import OzoneSetQuotaOperator
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor
from airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone import HdfsToOzoneOperator
from airflow.providers.arenadata.ozone.utils import EnvHelper
from airflow.utils import timezone

OM_HOST = EnvHelper.get_env_str("OZONE_EXAMPLE_OM_HOST", "om")
PIPELINE_CONN_ID = EnvHelper.get_env_str("OZONE_EXAMPLE_PIPELINE_CONN_ID", "ozone_admin_default")
PIPELINE_HDFS_CONN_ID = EnvHelper.get_env_str("OZONE_EXAMPLE_PIPELINE_HDFS_CONN_ID")
PIPELINE_VOLUME = EnvHelper.get_env_str("OZONE_EXAMPLE_PIPELINE_VOLUME", "vol1")
PIPELINE_BUCKET = EnvHelper.get_env_str("OZONE_EXAMPLE_PIPELINE_BUCKET", "bucket1")
PIPELINE_TRIGGER_FILE = EnvHelper.get_env_str("OZONE_EXAMPLE_PIPELINE_TRIGGER_FILE", "trigger.lck")
PIPELINE_QUOTA = EnvHelper.get_env_str("OZONE_EXAMPLE_PIPELINE_QUOTA", "500GB")
PIPELINE_SOURCE_PATH = EnvHelper.get_env_str(
    "OZONE_EXAMPLE_PIPELINE_SOURCE_PATH", "hdfs:///user/data/legacy/"
)
PIPELINE_DEST_SUBPATH = EnvHelper.get_env_str("OZONE_EXAMPLE_PIPELINE_DEST_SUBPATH", "migrated/")
PIPELINE_TRIGGER_PATH = f"ofs://{OM_HOST}/{PIPELINE_VOLUME}/{PIPELINE_BUCKET}/{PIPELINE_TRIGGER_FILE}"
PIPELINE_DEST_PATH = f"ofs://{OM_HOST}/{PIPELINE_VOLUME}/{PIPELINE_BUCKET}/{PIPELINE_DEST_SUBPATH}"

with DAG(
    "example_ozone_data_pipeline",
    start_date=timezone.datetime(2025, 1, 1),
    schedule=None,
    tags=["ozone", "example"],
) as dag:
    check_trigger = OzoneKeySensor(
        task_id="wait_for_landing_file",
        path=PIPELINE_TRIGGER_PATH,
        ozone_conn_id=PIPELINE_CONN_ID,
        mode="reschedule",
        execution_timeout=timedelta(minutes=1),
    )

    set_storage = OzoneSetQuotaOperator(
        task_id="prepare_quota",
        volume=PIPELINE_VOLUME,
        quota=PIPELINE_QUOTA,
        ozone_conn_id=PIPELINE_CONN_ID,
        execution_timeout=timedelta(minutes=1),
    )

    migrate_data = HdfsToOzoneOperator(
        task_id="migrate_legacy_data",
        source_path=PIPELINE_SOURCE_PATH,
        dest_path=PIPELINE_DEST_PATH,
        hdfs_conn_id=PIPELINE_HDFS_CONN_ID,
        execution_timeout=timedelta(minutes=5),
    )

    check_trigger >> set_storage >> migrate_data
