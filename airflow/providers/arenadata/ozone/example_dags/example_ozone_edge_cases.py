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

Runtime values can be overridden from the Trigger UI or via `dag_run.conf`;
legacy `OZONE_EXAMPLE_*` environment variables are kept as defaults for
compatibility.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import PurePosixPath

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
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

DEFAULT_OM_HOST = os.getenv("OZONE_EXAMPLE_OM_HOST") or "om"
DEFAULT_CONN_ID = os.getenv("OZONE_EXAMPLE_TEST_CONN_ID") or "ozone_admin_default"
DEFAULT_TEST_VOLUME = os.getenv("OZONE_EXAMPLE_TEST_VOLUME") or "provider-test-volume"
DEFAULT_TEST_BUCKET = os.getenv("OZONE_EXAMPLE_TEST_BUCKET") or "provider-test-bucket"
DEFAULT_MISSING_BUCKET = os.getenv("OZONE_EXAMPLE_TEST_MISSING_BUCKET") or "provider-test-missing-bucket"
DEFAULT_MISSING_VOLUME = os.getenv("OZONE_EXAMPLE_TEST_MISSING_VOLUME") or "provider-test-missing-volume"
DEFAULT_MISSING_KEY = os.getenv("OZONE_EXAMPLE_TEST_MISSING_KEY") or "missing/to_delete.txt"


def _missing_key_path(params: dict[str, str]) -> str:
    return (
        f"ofs://{params['om_host']}/"
        f"{PurePosixPath(params['test_volume'], params['test_bucket'], params['missing_key'])}"
    )


def _verify_missing_fs_path_runtime(**context) -> dict[str, bool]:
    params = context["params"]
    hook = OzoneFsHook(ozone_conn_id=params["test_conn_id"])
    path = _missing_key_path(params)
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


def _delete_missing_fs_path_runtime(**context) -> None:
    params = context["params"]
    hook = OzoneFsHook(ozone_conn_id=params["test_conn_id"])
    path = _missing_key_path(params)
    hook.delete_key(path)
    logger.info("Delete missing FS key completed without error: %s", path)


def _verify_missing_bucket_runtime(**context) -> dict[str, bool]:
    params = context["params"]
    hook = OzoneAdminHook(ozone_conn_id=params["test_conn_id"])
    result = hook.bucket_exists(params["test_volume"], params["missing_bucket"])
    logger.info(
        "Missing bucket probe for /%s/%s returned: %s",
        params["test_volume"],
        params["missing_bucket"],
        result,
    )
    if result is not False:
        raise AirflowException(
            f"Expected False for missing bucket check: /{params['test_volume']}/{params['missing_bucket']}"
        )
    return {"bucket_exists": result}


def _delete_missing_bucket_runtime(**context) -> None:
    params = context["params"]
    hook = OzoneAdminHook(ozone_conn_id=params["test_conn_id"])
    hook.delete_bucket(params["test_volume"], params["missing_bucket"])
    logger.info(
        "Delete missing bucket completed without error: /%s/%s",
        params["test_volume"],
        params["missing_bucket"],
    )


def _verify_missing_volume_runtime(**context) -> dict[str, bool]:
    params = context["params"]
    hook = OzoneAdminHook(ozone_conn_id=params["test_conn_id"])
    result = hook.volume_exists(params["missing_volume"])
    logger.info("Missing volume probe for /%s returned: %s", params["missing_volume"], result)
    if result is not False:
        raise AirflowException(f"Expected False for missing volume check: /{params['missing_volume']}")
    return {"volume_exists": result}


def _delete_missing_volume_runtime(**context) -> None:
    params = context["params"]
    hook = OzoneAdminHook(ozone_conn_id=params["test_conn_id"])
    hook.delete_volume(params["missing_volume"])
    logger.info("Delete missing volume completed without error: /%s", params["missing_volume"])


with DAG(
    dag_id="example_ozone_edge_cases",
    start_date=timezone.datetime(2026, 1, 1),
    catchup=False,
    schedule=None,
    tags=["ozone", "example", "edge-cases"],
    default_args={"owner": "airflow", "retries": 0},
    params={
        "om_host": Param(DEFAULT_OM_HOST, type="string", title="OM host / Ozone authority"),
        "test_conn_id": Param(DEFAULT_CONN_ID, type="string", title="Ozone connection ID"),
        "test_volume": Param(DEFAULT_TEST_VOLUME, type="string", title="Test volume"),
        "test_bucket": Param(DEFAULT_TEST_BUCKET, type="string", title="Test bucket"),
        "missing_bucket": Param(DEFAULT_MISSING_BUCKET, type="string", title="Missing bucket probe"),
        "missing_volume": Param(DEFAULT_MISSING_VOLUME, type="string", title="Missing volume probe"),
        "missing_key": Param(DEFAULT_MISSING_KEY, type="string", title="Missing key probe path"),
    },
    doc_md=__doc__,
) as dag:
    create_test_volume = OzoneCreateVolumeOperator(
        task_id="create_test_volume",
        volume_name="{{ params.test_volume }}",
        ozone_conn_id="{{ params.test_conn_id }}",
        execution_timeout=timedelta(minutes=2),
    )

    create_test_bucket = OzoneCreateBucketOperator(
        task_id="create_test_bucket",
        volume_name="{{ params.test_volume }}",
        bucket_name="{{ params.test_bucket }}",
        ozone_conn_id="{{ params.test_conn_id }}",
        execution_timeout=timedelta(minutes=2),
    )

    verify_missing_fs_path = PythonOperator(
        task_id="verify_missing_fs_path",
        python_callable=_verify_missing_fs_path_runtime,
        execution_timeout=timedelta(minutes=2),
    )

    delete_missing_fs_path = PythonOperator(
        task_id="delete_missing_fs_path",
        python_callable=_delete_missing_fs_path_runtime,
        execution_timeout=timedelta(minutes=2),
    )

    verify_missing_bucket = PythonOperator(
        task_id="verify_missing_bucket",
        python_callable=_verify_missing_bucket_runtime,
        execution_timeout=timedelta(minutes=2),
    )

    delete_missing_bucket = PythonOperator(
        task_id="delete_missing_bucket",
        python_callable=_delete_missing_bucket_runtime,
        execution_timeout=timedelta(minutes=2),
    )

    verify_missing_volume = PythonOperator(
        task_id="verify_missing_volume",
        python_callable=_verify_missing_volume_runtime,
        execution_timeout=timedelta(minutes=2),
    )

    delete_missing_volume = PythonOperator(
        task_id="delete_missing_volume",
        python_callable=_delete_missing_volume_runtime,
        execution_timeout=timedelta(minutes=2),
    )

    cleanup_test_bucket = OzoneDeleteBucketOperator(
        task_id="cleanup_test_bucket",
        volume_name="{{ params.test_volume }}",
        bucket_name="{{ params.test_bucket }}",
        ozone_conn_id="{{ params.test_conn_id }}",
        execution_timeout=timedelta(minutes=2),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    cleanup_test_volume = OzoneDeleteVolumeOperator(
        task_id="cleanup_test_volume",
        volume_name="{{ params.test_volume }}",
        ozone_conn_id="{{ params.test_conn_id }}",
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
