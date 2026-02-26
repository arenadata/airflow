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
Complete Data Lifecycle Management Example DAG

This DAG demonstrates advanced data lifecycle management with dynamic task generation:
1. Lists all files in a landing directory (OzoneListOperator)
2. Dynamically creates processing tasks for each file found (fan-out pattern)
3. Archives processed files to a new location (OzoneToOzoneOperator)
4. Registers archived data as a Hive table partition (OzoneToHiveOperator)
5. Creates a disaster-recovery snapshot (OzoneBackupOperator)
6. Cleans up original files from the landing zone

This example showcases:
- Dynamic task generation based on discovered files
- Data archiving and lifecycle management
- Integration with Hive for data lake queries
- Backup and disaster recovery workflows
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.arenadata.ozone.operators.ozone_admin import (
    OzoneCreateBucketOperator,
    OzoneCreateVolumeOperator,
)
from airflow.providers.arenadata.ozone.operators.ozone_fs import (
    OzoneDeleteKeyOperator,
    OzoneFsMkdirOperator,
    OzoneListOperator,
)
from airflow.providers.arenadata.ozone.operators.ozone_transfer import OzoneToOzoneOperator
from airflow.providers.arenadata.ozone.transfers.ozone_backup import OzoneBackupOperator
from airflow.providers.arenadata.ozone.transfers.ozone_to_hive import OzoneToHiveOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="example_ozone_data_lifecycle",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["ozone", "example"],
    doc_md="""
    ### Advanced Data Lifecycle and Dynamic Processing Example

    This DAG demonstrates a complete data lifecycle management workflow:
    1. **List**: Finds all files in a landing directory using `OzoneListOperator`.
    2. **Dynamic Processing**: For each file found, it dynamically spawns a processing task (fan-out).
    3. **Archive**: Moves the processed source files to an archive path using `OzoneToOzoneOperator`.
    4. **Register**: Registers the archived data as a new partition in Hive with `OzoneToHiveOperator`.
    5. **Backup**: Creates a disaster-recovery snapshot of the bucket using `OzoneBackupOperator`.
    6. **Cleanup**: Deletes the original files from the landing directory.
    """,
) as dag:
    # Assume we have some files in this landing path.
    # Ozone volume and bucket names must match the pattern `[a-z0-9.-]+`
    # (lowercase letters, digits, dots and dashes; underscores are not allowed).
    # We use:
    #   - Volumes:  'landing', 'archive'
    #   - Buckets:  'raw', 'processed'
    LANDING_VOLUME = "landing"
    LANDING_BUCKET = "raw"
    ARCHIVE_VOLUME = "archive"
    ARCHIVE_BUCKET = "processed"

    # Note: Ozone requires 'ofs://om/<volume>/<bucket>/...' format, where `om`
    # is the Ozone Manager hostname in the docker compose cluster.
    LANDING_PATH = f"ofs://om/{LANDING_VOLUME}/{LANDING_BUCKET}"
    # Base archive path (without date partition)
    ARCHIVE_BASE_PATH = f"ofs://om/{ARCHIVE_VOLUME}/{ARCHIVE_BUCKET}"

    # 0. Ensure required volumes and buckets exist.
    create_landing_volume = OzoneCreateVolumeOperator(
        task_id="create_landing_volume",
        volume_name=LANDING_VOLUME,
        execution_timeout=timedelta(minutes=1),
    )
    create_archive_volume = OzoneCreateVolumeOperator(
        task_id="create_archive_volume",
        volume_name=ARCHIVE_VOLUME,
        execution_timeout=timedelta(minutes=1),
    )

    create_landing_bucket = OzoneCreateBucketOperator(
        task_id="create_landing_bucket",
        volume_name=LANDING_VOLUME,
        bucket_name=LANDING_BUCKET,
        execution_timeout=timedelta(minutes=1),
    )
    create_archive_bucket = OzoneCreateBucketOperator(
        task_id="create_archive_bucket",
        volume_name=ARCHIVE_VOLUME,
        bucket_name=ARCHIVE_BUCKET,
        execution_timeout=timedelta(minutes=1),
    )

    # 1. List all files in the landing directory. The result is pushed to XComs.
    list_files_in_landing_zone = OzoneListOperator(
        task_id="list_files_in_landing_zone",
        path=LANDING_PATH,
        execution_timeout=timedelta(minutes=1),
    )

    # 2. This TaskGroup emulates file processing in a single task.
    with TaskGroup(group_id="dynamic_file_processing") as processing_group:
        process_files = BashOperator(
            task_id="process_files",
            bash_command=(
                'echo "Processing files: {{ ti.xcom_pull(task_ids="list_files_in_landing_zone") }}"'
            ),
            execution_timeout=timedelta(minutes=1),
        )

    # 3. Create archive directory for date-based partitioning
    create_archive_dir = OzoneFsMkdirOperator(
        task_id="create_archive_dir",
        path=ARCHIVE_BASE_PATH + "/ds={{ ds }}",
        execution_timeout=timedelta(minutes=1),
    )

    # 4. After processing, move the original files to an archive directory.
    archive_files = OzoneToOzoneOperator(
        task_id="archive_landing_files",
        source_path=f"{LANDING_PATH}/*",  # Move all files
        dest_path=ARCHIVE_BASE_PATH + "/ds={{ ds }}",
        execution_timeout=timedelta(minutes=5),
    )

    def check_hive_available(**context):
        """Check if Hive provider is installed."""

        try:
            from airflow.providers.apache.hive.hooks.hive import HiveCliHook  # noqa: F401

            context["ti"].log.info("Hive provider is available, will register partition")
            return True
        except ModuleNotFoundError:
            context["ti"].log.warning(
                "Hive provider not installed. Skipping partition registration. "
                "Install 'apache-airflow-providers-apache-hive' to enable this feature."
            )
            return False

    check_hive = ShortCircuitOperator(
        task_id="check_hive_available",
        python_callable=check_hive_available,
        execution_timeout=timedelta(minutes=1),
    )

    # 5a. Register the new archive path as a partition in a Hive table (only if Hive is available)
    register_hive_partition = OzoneToHiveOperator(
        task_id="register_hive_partition",
        ozone_path=ARCHIVE_BASE_PATH + "/ds={{ ds }}",
        table_name="processed_events",
        partition_spec={"ds": "{{ ds }}"},
        execution_timeout=timedelta(minutes=5),
    )

    # 6. Create a snapshot of the entire archive volume for backup.
    backup_archive = OzoneBackupOperator(
        task_id="backup_archive_via_snapshot",
        volume=ARCHIVE_VOLUME,
        bucket=ARCHIVE_BUCKET,
        snapshot_name="snap-{{ ds_nodash }}",
        execution_timeout=timedelta(minutes=5),
    )

    # 7. Cleanup original files from landing zone.
    cleanup_landing_zone = OzoneDeleteKeyOperator(
        task_id="cleanup_landing_zone",
        path=f"{LANDING_PATH}/*",
        execution_timeout=timedelta(minutes=1),
    )

    create_landing_volume >> create_landing_bucket
    create_archive_volume >> create_archive_bucket

    create_landing_bucket >> list_files_in_landing_zone

    list_files_in_landing_zone >> processing_group
    create_archive_bucket >> create_archive_dir
    processing_group >> create_archive_dir >> archive_files
    archive_files >> check_hive >> register_hive_partition
    archive_files >> backup_archive
    backup_archive >> cleanup_landing_zone
