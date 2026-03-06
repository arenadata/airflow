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
Cross-Region Replication Example DAG

This DAG demonstrates replicating data from a primary Ozone cluster to a DR (Disaster Recovery) cluster:
- Uses HdfsToOzoneOperator with distcp for efficient cross-cluster data replication
- Scheduled to run daily (can be adjusted)
- Shows how to replicate critical data for disaster recovery purposes

This example requires:
- Two Ozone clusters (primary and DR) with network connectivity
- Proper Hadoop configuration for cross-cluster communication (core-site.xml / ozone-site.xml)
- Optional HDFS connection for SSL/TLS settings used by distcp

Note: This DAG uses distcp, which is the standard tool for cross-cluster replication in Hadoop ecosystems.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone import HdfsToOzoneOperator
from airflow.providers.arenadata.ozone.utils.helpers import TypeNormalizationHelper
from airflow.utils import timezone


def get_env_str(name: str, default: str | None = None) -> str | None:
    return TypeNormalizationHelper.normalize_optional_str(os.getenv(name)) or default


REPLICATION_SOURCE_CLUSTER = get_env_str("OZONE_EXAMPLE_REPLICATION_SOURCE_CLUSTER", "primary-cluster")
REPLICATION_TARGET_CLUSTER = get_env_str("OZONE_EXAMPLE_REPLICATION_TARGET_CLUSTER", "dr-cluster")
REPLICATION_SOURCE_BASE = get_env_str("OZONE_EXAMPLE_REPLICATION_SOURCE_BASE", "critical_data")
REPLICATION_TARGET_BASE = get_env_str(
    "OZONE_EXAMPLE_REPLICATION_TARGET_BASE", "replicated_data/critical_data"
)
REPLICATION_HDFS_CONN_ID = get_env_str("OZONE_EXAMPLE_REPLICATION_HDFS_CONN_ID")
REPLICATION_SOURCE_PATH = f"ofs://{REPLICATION_SOURCE_CLUSTER}/{REPLICATION_SOURCE_BASE}/{{{{ ds }}}}/"
REPLICATION_DEST_PATH = f"ofs://{REPLICATION_TARGET_CLUSTER}/{REPLICATION_TARGET_BASE}/{{{{ ds }}}}/"

with DAG(
    dag_id="example_ozone_cross_region_replication",
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    schedule=get_env_str("OZONE_EXAMPLE_REPLICATION_SCHEDULE", "0 1 * * *"),
    tags=["ozone", "example"],
    doc_md="""
    ### Cross-Region Replication Example

    This DAG demonstrates replicating data from a primary Ozone cluster to a DR (Disaster Recovery) cluster.
    It uses `HdfsToOzoneOperator`, which leverages `distcp`, the standard tool for this task.

    **Prerequisites:**
    1. Network connectivity between the Airflow worker and both clusters.
    2. DistCp must be configured for cross-cluster communication (for example via `core-site.xml` / `ozone-site.xml`).
    3. Optional: set `OZONE_EXAMPLE_REPLICATION_HDFS_CONN_ID` to pass SSL/TLS parameters for the source HDFS side.
    """,
) as dag:
    replicate_critical_data = HdfsToOzoneOperator(
        task_id="replicate_critical_data_to_dr",
        source_path=REPLICATION_SOURCE_PATH,
        dest_path=REPLICATION_DEST_PATH,
        hdfs_conn_id=REPLICATION_HDFS_CONN_ID,
        execution_timeout=timedelta(minutes=5),
    )
