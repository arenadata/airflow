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

import pendulum

from airflow import DAG
from airflow.providers.arenadata.ozone.operators.ozone import OzoneSetQuotaOperator
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor
from airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone import HdfsToOzoneOperator

with DAG(
    "example_ozone_data_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    tags=["ozone", "example"],
) as dag:
    check_trigger = OzoneKeySensor(
        task_id="wait_for_landing_file",
        path="ofs://om/vol1/bucket1/trigger.lck",
        mode="reschedule",
        execution_timeout=timedelta(minutes=1),
    )

    set_storage = OzoneSetQuotaOperator(
        task_id="prepare_quota",
        volume="vol1",
        quota="500GB",
        execution_timeout=timedelta(minutes=1),
    )

    migrate_data = HdfsToOzoneOperator(
        task_id="migrate_legacy_data",
        source_path="hdfs:///user/data/legacy/",
        dest_path="ofs://om/vol1/bucket1/migrated/",
        execution_timeout=timedelta(minutes=5),
    )

    check_trigger >> set_storage >> migrate_data
