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

This example is import-safe even when the HDFS provider is not installed.
HdfsToOzoneOperator relies on Hadoop DistCp runtime, and runtime validation
happens only when task `migrate_legacy_data` executes.

Runtime values can be overridden from the Trigger UI or via `dag_run.conf`;
legacy `OZONE_EXAMPLE_*` environment variables are kept as defaults for
compatibility.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import PurePosixPath

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.arenadata.ozone.operators.ozone import OzoneSetQuotaOperator
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor
from airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone import HdfsToOzoneOperator
from airflow.utils import timezone

DEFAULT_OM_HOST = os.getenv("OZONE_EXAMPLE_OM_HOST") or "om"
DEFAULT_CONN_ID = os.getenv("OZONE_EXAMPLE_PIPELINE_CONN_ID") or "ozone_admin_default"
DEFAULT_HDFS_CONN_ID = os.getenv("OZONE_EXAMPLE_PIPELINE_HDFS_CONN_ID") or "hdfs_default"
DEFAULT_VOLUME = os.getenv("OZONE_EXAMPLE_PIPELINE_VOLUME") or "vol1"
DEFAULT_BUCKET = os.getenv("OZONE_EXAMPLE_PIPELINE_BUCKET") or "bucket1"
DEFAULT_TRIGGER_FILE = os.getenv("OZONE_EXAMPLE_PIPELINE_TRIGGER_FILE") or "trigger.lck"
DEFAULT_QUOTA = os.getenv("OZONE_EXAMPLE_PIPELINE_QUOTA") or "500GB"
DEFAULT_SOURCE_PATH = os.getenv("OZONE_EXAMPLE_PIPELINE_SOURCE_PATH") or "hdfs:///user/data/legacy/"
DEFAULT_DEST_SUBPATH = os.getenv("OZONE_EXAMPLE_PIPELINE_DEST_SUBPATH") or "migrated/"

with DAG(
    "example_ozone_data_pipeline",
    start_date=timezone.datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ozone", "example"],
    params={
        "om_host": Param(DEFAULT_OM_HOST, type="string", title="OM host / Ozone authority"),
        "pipeline_conn_id": Param(DEFAULT_CONN_ID, type="string", title="Ozone connection ID"),
        "hdfs_conn_id": Param(DEFAULT_HDFS_CONN_ID, type="string", title="HDFS connection ID"),
        "volume": Param(DEFAULT_VOLUME, type="string", title="Destination volume"),
        "bucket": Param(DEFAULT_BUCKET, type="string", title="Destination bucket"),
        "trigger_file": Param(DEFAULT_TRIGGER_FILE, type="string", title="Trigger file name"),
        "quota": Param(DEFAULT_QUOTA, type="string", title="Destination quota"),
        "source_path": Param(DEFAULT_SOURCE_PATH, type="string", title="HDFS source path"),
        "dest_subpath": Param(DEFAULT_DEST_SUBPATH, type="string", title="Destination subpath in bucket"),
    },
) as dag:
    PIPELINE_TRIGGER_PATH = (
        f"ofs://{{{{ params.om_host }}}}/"
        f"{PurePosixPath('{{ params.volume }}', '{{ params.bucket }}', '{{ params.trigger_file }}')}"
    )
    PIPELINE_DEST_PATH = (
        f"ofs://{{{{ params.om_host }}}}/"
        f"{PurePosixPath('{{ params.volume }}', '{{ params.bucket }}', '{{ params.dest_subpath }}')}"
    )

    check_trigger = OzoneKeySensor(
        task_id="wait_for_landing_file",
        path=PIPELINE_TRIGGER_PATH,
        ozone_conn_id="{{ params.pipeline_conn_id }}",
        mode="reschedule",
        execution_timeout=timedelta(minutes=1),
    )

    set_storage = OzoneSetQuotaOperator(
        task_id="prepare_quota",
        volume="{{ params.volume }}",
        quota="{{ params.quota }}",
        ozone_conn_id="{{ params.pipeline_conn_id }}",
        execution_timeout=timedelta(minutes=1),
    )

    migrate_data = HdfsToOzoneOperator(
        task_id="migrate_legacy_data",
        source_path="{{ params.source_path }}",
        dest_path=PIPELINE_DEST_PATH,
        hdfs_conn_id="{{ params.hdfs_conn_id }}",
        execution_timeout=timedelta(minutes=5),
    )

    check_trigger >> set_storage >> migrate_data
