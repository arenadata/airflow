#
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
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from airflow.models import Connection
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneAdminHook, OzoneFsHook
from airflow.providers.arenadata.ozone.operators.ozone import (
    OzoneCreateBucketOperator,
    OzoneCreatePathOperator,
    OzoneCreateVolumeOperator,
    OzoneDeleteBucketOperator,
    OzoneDeletePathOperator,
    OzoneDeleteVolumeOperator,
    OzoneDownloadFileOperator,
    OzoneListOperator,
    OzonePathExistsOperator,
    OzoneUploadContentOperator,
)
from airflow.utils import db

CONN_ID = "ozone_operator_test"
OZONE_HOST = os.environ.get("OZONE_HOST", "om")
OZONE_PORT = int(os.environ.get("OZONE_PORT", "9862"))
OZONE_FS_AUTHORITY = os.environ.get("OZONE_FS_AUTHORITY", "om")
VOLUME = "inttest-operator-volume"
BUCKET = "inttest-operator-bucket"


@pytest.fixture(autouse=True)
def _setup_connection():
    db.merge_conn(
        Connection(
            conn_id=CONN_ID,
            conn_type="ozone",
            host=OZONE_HOST,
            port=OZONE_PORT,
        )
    )


def _cleanup_volume(hook: OzoneAdminHook) -> None:
    """Delete test volume with all buckets."""
    if not hook.volume_exists(VOLUME):
        return
    if hook.bucket_exists(VOLUME, BUCKET):
        try:
            hook.run_cli(
                [
                    "ozone",
                    "fs",
                    "-rm",
                    "-r",
                    "-skipTrash",
                    f"ofs://{OZONE_FS_AUTHORITY}/{VOLUME}/{BUCKET}/*",
                ],
                check=False,
                log_output=False,
                retry_attempts=0,
            )
        except Exception:
            pass
        try:
            hook.delete_bucket(VOLUME, BUCKET)
        except Exception:
            pass
    try:
        hook.delete_volume(VOLUME)
    except Exception:
        pass


@pytest.mark.integration("ozone")
class TestOzoneOperatorIntegration:
    def setup_method(self):
        self.admin = OzoneAdminHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        self.fs = OzoneFsHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_volume(self.admin)

    def teardown_method(self):
        _cleanup_volume(self.admin)

    def test_admin_and_filesystem_operator_smoke_flow(self):
        base_path = f"ofs://{OZONE_FS_AUTHORITY}/{VOLUME}/{BUCKET}"
        directory = f"{base_path}/incoming"
        remote_file = f"{directory}/payload.txt"

        OzoneCreateVolumeOperator(
            task_id="create_volume",
            volume_name=VOLUME,
            ozone_conn_id=CONN_ID,
            retry_attempts=1,
        ).execute({})
        assert self.admin.volume_exists(VOLUME)

        OzoneCreateBucketOperator(
            task_id="create_bucket",
            volume_name=VOLUME,
            bucket_name=BUCKET,
            ozone_conn_id=CONN_ID,
            retry_attempts=1,
        ).execute({})
        assert self.admin.bucket_exists(VOLUME, BUCKET)

        OzoneCreatePathOperator(
            task_id="create_path",
            path=directory,
            ozone_conn_id=CONN_ID,
            retry_attempts=1,
        ).execute({})
        assert self.fs.path_exists(directory)

        OzoneUploadContentOperator(
            task_id="upload_content",
            content="hello from operator integration\n",
            remote_path=remote_file,
            ozone_conn_id=CONN_ID,
            retry_attempts=1,
        ).execute({})
        assert self.fs.key_exists(remote_file)

        assert OzonePathExistsOperator(
            task_id="path_exists",
            path=remote_file,
            ozone_conn_id=CONN_ID,
            retry_attempts=1,
        ).execute({})

        listed_paths = OzoneListOperator(
            task_id="list_keys",
            path=f"{directory}/*",
            ozone_conn_id=CONN_ID,
            retry_attempts=1,
        ).execute({})
        assert remote_file in listed_paths

        with tempfile.TemporaryDirectory(prefix="ozone_operator_download_") as tmp_dir:
            download_path = Path(tmp_dir) / "payload.txt"
            OzoneDownloadFileOperator(
                task_id="download_file",
                remote_path=remote_file,
                local_path=str(download_path),
                ozone_conn_id=CONN_ID,
                retry_attempts=1,
            ).execute({})
            assert download_path.read_text(encoding="utf-8") == "hello from operator integration\n"

        OzoneDeletePathOperator(
            task_id="delete_path",
            path=directory,
            ozone_conn_id=CONN_ID,
            recursive=True,
            retry_attempts=1,
        ).execute({})
        assert not self.fs.path_exists(directory)

        OzoneDeleteBucketOperator(
            task_id="delete_bucket",
            volume_name=VOLUME,
            bucket_name=BUCKET,
            recursive=True,
            force=True,
            ozone_conn_id=CONN_ID,
            retry_attempts=1,
        ).execute({})
        assert not self.admin.bucket_exists(VOLUME, BUCKET)

        OzoneDeleteVolumeOperator(
            task_id="delete_volume",
            volume_name=VOLUME,
            ozone_conn_id=CONN_ID,
            retry_attempts=1,
        ).execute({})
        assert not self.admin.volume_exists(VOLUME)
