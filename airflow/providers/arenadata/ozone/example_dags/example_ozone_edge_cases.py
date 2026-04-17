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
Ozone provider example DAG for edge-case verification.

This DAG is intended for manual verification of non-standard provider behavior:
1. Creates a real volume and bucket to make missing-key checks unambiguous.
2. Verifies `OzoneFsHook.exists/path_exists/key_exists` return `False` for a missing key.
3. Verifies `OzoneAdminHook.bucket_exists/volume_exists` return `False` for missing resources.
4. Verifies delete operations on missing resources behave as no-op/idempotent checks.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import PurePosixPath

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneAdminHook, OzoneFsHook
from airflow.providers.arenadata.ozone.operators.ozone import (
    OzoneCreateBucketOperator,
    OzoneCreateVolumeOperator,
    OzoneDeleteBucketOperator,
    OzoneDeleteVolumeOperator,
)
from airflow.utils import timezone
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)


def _example_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip()
    return normalized if normalized else default


OM_HOST = _example_env("OZONE_EXAMPLE_OM_HOST", "om")
TEST_CONN_ID = _example_env("OZONE_EXAMPLE_TEST_CONN_ID", "ozone_admin_default")
TEST_VOLUME = _example_env("OZONE_EXAMPLE_TEST_VOLUME", "provider-test-volume")
TEST_BUCKET = _example_env("OZONE_EXAMPLE_TEST_BUCKET", "provider-test-bucket")
MISSING_BUCKET = _example_env("OZONE_EXAMPLE_TEST_MISSING_BUCKET", "provider-test-missing-bucket")
MISSING_VOLUME = _example_env("OZONE_EXAMPLE_TEST_MISSING_VOLUME", "provider-test-missing-volume")
MISSING_KEY = _example_env("OZONE_EXAMPLE_TEST_MISSING_KEY", "missing/to_delete.txt")


def _missing_key_path() -> str:
    return f"ofs://{OM_HOST}/{PurePosixPath(TEST_VOLUME, TEST_BUCKET, MISSING_KEY)}"


def _verify_missing_fs_path() -> dict[str, bool]:
    hook = OzoneFsHook(ozone_conn_id=TEST_CONN_ID)
    path = _missing_key_path()
    results = {
        "exists": hook.exists(path),
        "path_exists": hook.path_exists(path),
        "key_exists": hook.key_exists(path),
    }
    logger.info("Missing FS path probe for %s returned: %s", path, results)
    unexpected = [name for name, value in results.items() if value is not False]
    if unexpected:
        raise AirflowException(f"Expected False for missing FS path probe(s), got True for: {unexpected}")
    return results


def _delete_missing_fs_path() -> None:
    hook = OzoneFsHook(ozone_conn_id=TEST_CONN_ID)
    path = _missing_key_path()
    hook.delete_key(path)
    logger.info("Delete missing FS key completed without error: %s", path)


def _verify_missing_bucket() -> dict[str, bool]:
    hook = OzoneAdminHook(ozone_conn_id=TEST_CONN_ID)
    result = hook.bucket_exists(TEST_VOLUME, MISSING_BUCKET)
    logger.info("Missing bucket probe for /%s/%s returned: %s", TEST_VOLUME, MISSING_BUCKET, result)
    if result is not False:
        raise AirflowException(f"Expected False for missing bucket check: /{TEST_VOLUME}/{MISSING_BUCKET}")
    return {"bucket_exists": result}


def _delete_missing_bucket() -> None:
    hook = OzoneAdminHook(ozone_conn_id=TEST_CONN_ID)
    hook.delete_bucket(TEST_VOLUME, MISSING_BUCKET)
    logger.info("Delete missing bucket completed without error: /%s/%s", TEST_VOLUME, MISSING_BUCKET)


def _verify_missing_volume() -> dict[str, bool]:
    hook = OzoneAdminHook(ozone_conn_id=TEST_CONN_ID)
    result = hook.volume_exists(MISSING_VOLUME)
    logger.info("Missing volume probe for /%s returned: %s", MISSING_VOLUME, result)
    if result is not False:
        raise AirflowException(f"Expected False for missing volume check: /{MISSING_VOLUME}")
    return {"volume_exists": result}


def _delete_missing_volume() -> None:
    hook = OzoneAdminHook(ozone_conn_id=TEST_CONN_ID)
    hook.delete_volume(MISSING_VOLUME)
    logger.info("Delete missing volume completed without error: /%s", MISSING_VOLUME)


with DAG(
    dag_id="example_ozone_edge_cases",
    start_date=timezone.datetime(2026, 1, 1),
    catchup=False,
    schedule=None,
    tags=["ozone", "example", "edge-cases"],
    default_args={"owner": "airflow", "retries": 0},
    doc_md=__doc__,
) as dag:
    create_test_volume = OzoneCreateVolumeOperator(
        task_id="create_test_volume",
        volume_name=TEST_VOLUME,
        ozone_conn_id=TEST_CONN_ID,
        execution_timeout=timedelta(minutes=2),
    )

    create_test_bucket = OzoneCreateBucketOperator(
        task_id="create_test_bucket",
        volume_name=TEST_VOLUME,
        bucket_name=TEST_BUCKET,
        ozone_conn_id=TEST_CONN_ID,
        execution_timeout=timedelta(minutes=2),
    )

    verify_missing_fs_path = PythonOperator(
        task_id="verify_missing_fs_path",
        python_callable=_verify_missing_fs_path,
        execution_timeout=timedelta(minutes=2),
    )

    delete_missing_fs_path = PythonOperator(
        task_id="delete_missing_fs_path",
        python_callable=_delete_missing_fs_path,
        execution_timeout=timedelta(minutes=2),
    )

    verify_missing_bucket = PythonOperator(
        task_id="verify_missing_bucket",
        python_callable=_verify_missing_bucket,
        execution_timeout=timedelta(minutes=2),
    )

    delete_missing_bucket = PythonOperator(
        task_id="delete_missing_bucket",
        python_callable=_delete_missing_bucket,
        execution_timeout=timedelta(minutes=2),
    )

    verify_missing_volume = PythonOperator(
        task_id="verify_missing_volume",
        python_callable=_verify_missing_volume,
        execution_timeout=timedelta(minutes=2),
    )

    delete_missing_volume = PythonOperator(
        task_id="delete_missing_volume",
        python_callable=_delete_missing_volume,
        execution_timeout=timedelta(minutes=2),
    )

    cleanup_test_bucket = OzoneDeleteBucketOperator(
        task_id="cleanup_test_bucket",
        volume_name=TEST_VOLUME,
        bucket_name=TEST_BUCKET,
        ozone_conn_id=TEST_CONN_ID,
        execution_timeout=timedelta(minutes=2),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    cleanup_test_volume = OzoneDeleteVolumeOperator(
        task_id="cleanup_test_volume",
        volume_name=TEST_VOLUME,
        ozone_conn_id=TEST_CONN_ID,
        execution_timeout=timedelta(minutes=2),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    diagnostics = [
        verify_missing_fs_path,
        delete_missing_fs_path,
        verify_missing_bucket,
        delete_missing_bucket,
        verify_missing_volume,
        delete_missing_volume,
    ]

    create_test_volume >> create_test_bucket
    create_test_bucket >> diagnostics
    diagnostics >> cleanup_test_bucket >> cleanup_test_volume
