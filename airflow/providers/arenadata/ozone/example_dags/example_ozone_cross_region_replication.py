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
- Proper Hadoop configuration for cross-cluster communication
- Separate Airflow connections for each cluster (if needed)

Note: This DAG uses distcp, which is the standard tool for cross-cluster replication in Hadoop ecosystems.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.models.dag import DAG
from airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone import HdfsToOzoneOperator

with DAG(
    dag_id="example_ozone_cross_region_replication",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule="0 1 * * *",  # Daily at 1 AM
    tags=["ozone", "example"],
    doc_md="""
    ### Cross-Region Replication Example

    This DAG demonstrates replicating data from a primary Ozone cluster to a DR (Disaster Recovery) cluster.
    It uses `HdfsToOzoneOperator`, which leverages `distcp`, the standard tool for this task.

    **Prerequisites:**
    1. Two Airflow connections: `ozone_primary_cluster` and `ozone_dr_cluster`.
    2. Network connectivity between the Airflow worker and both clusters.
    3. The `distcp` command must be configured to handle cross-cluster communication (e.g., via `core-site.xml`).
    """,
) as dag:
    replicate_critical_data = HdfsToOzoneOperator(
        task_id="replicate_critical_data_to_dr",
        # Note: We provide the full HDFS path including the cluster name (nameservice)
        source_path="ofs://primary-cluster/critical_data/{{ ds }}/",
        dest_path="ofs://dr-cluster/replicated_data/critical_data/{{ ds }}/",
        # In a real setup, you might have separate hooks/connections for each
        # but distcp handles this at the Hadoop config level.
        execution_timeout=timedelta(minutes=5),
    )
