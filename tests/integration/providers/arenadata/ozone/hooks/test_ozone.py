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
from pathlib import Path, PurePosixPath

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneAdminHook, OzoneFsHook
from airflow.utils import db

CONN_ID = "ozone_test"
OZONE_HOST = os.environ.get("OZONE_HOST", "om")
OZONE_PORT = int(os.environ.get("OZONE_PORT", "9862"))
OZONE_FS_AUTHORITY = os.environ.get("OZONE_FS_AUTHORITY", "om")
VOLUME = "inttest-volume"
BUCKET = "inttest-bucket"
EDGE_VOLUME = "inttest-edge-volume"
EDGE_BUCKET = "inttest-edge-bucket"
EDGE_MISSING_VOLUME = "inttest-edge-missing-volume"
EDGE_MISSING_BUCKET = "inttest-edge-missing-bucket"
EDGE_MISSING_KEY = "missing/to_delete.txt"
EDGE_EXISTING_KEY = "existing-policy.txt"


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
    _cleanup_named_volume(hook, VOLUME, BUCKET)


def _cleanup_edge_volume(hook: OzoneAdminHook) -> None:
    """Delete edge-case test volume with all buckets."""
    _cleanup_named_volume(hook, EDGE_VOLUME, EDGE_BUCKET)


def _cleanup_named_volume(hook: OzoneAdminHook, volume: str, bucket: str) -> None:
    """Delete named test volume with all buckets."""
    if not hook.volume_exists(volume):
        return
    if hook.bucket_exists(volume, bucket):
        try:
            hook.run_cli(
                [
                    "ozone",
                    "fs",
                    "-rm",
                    "-r",
                    "-skipTrash",
                    f"ofs://{OZONE_FS_AUTHORITY}/{volume}/{bucket}/*",
                ],
                check=False,
                log_output=False,
                retry_attempts=0,
            )
        except Exception:
            pass
        try:
            hook.delete_bucket(volume, bucket)
        except Exception:
            pass
    try:
        hook.delete_volume(volume)
    except Exception:
        pass


def _edge_key_path(key: str) -> str:
    return f"ofs://{OZONE_FS_AUTHORITY}/{PurePosixPath(EDGE_VOLUME, EDGE_BUCKET, key)}"


@pytest.mark.integration("ozone")
class TestOzoneAdminHookIntegration:
    def setup_method(self):
        self.hook = OzoneAdminHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_volume(self.hook)

    def teardown_method(self):
        _cleanup_volume(self.hook)

    def test_create_and_delete_volume(self):
        self.hook.create_volume(VOLUME)
        assert self.hook.volume_exists(VOLUME)

        self.hook.delete_volume(VOLUME)
        assert not self.hook.volume_exists(VOLUME)

    def test_create_volume_existing_target_policy(self):
        self.hook.create_volume(VOLUME)

        self.hook.create_volume(VOLUME, if_exists="ignore")

        with pytest.raises(AirflowException, match="already exists"):
            self.hook.create_volume(VOLUME, if_exists="error")

    def test_volume_not_exists(self):
        assert not self.hook.volume_exists("nonexistent_volume")

    def test_list_volumes(self):
        self.hook.create_volume(VOLUME)
        volumes = self.hook.list_volumes()
        volume_names = [v["name"] for v in volumes]
        assert VOLUME in volume_names

    def test_get_volume_info(self):
        self.hook.create_volume(VOLUME)
        info = self.hook.get_volume_info(VOLUME)
        assert info["name"] == VOLUME

    def test_create_and_delete_bucket(self):
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET)
        assert self.hook.bucket_exists(VOLUME, BUCKET)

        self.hook.delete_bucket(VOLUME, BUCKET)
        assert not self.hook.bucket_exists(VOLUME, BUCKET)

    def test_list_buckets(self):
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET)
        buckets = self.hook.list_buckets(VOLUME)
        bucket_names = [b["name"] for b in buckets]
        assert BUCKET in bucket_names

    def test_get_bucket_info(self):
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET)
        info = self.hook.get_bucket_info(VOLUME, BUCKET)
        assert info["name"] == BUCKET
        assert info["volumeName"] == VOLUME


@pytest.mark.integration("ozone")
class TestOzoneFsHookIntegration:
    def setup_method(self):
        self.admin = OzoneAdminHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        self.hook = OzoneFsHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_volume(self.admin)
        self.admin.create_volume(VOLUME)
        self.admin.create_bucket(VOLUME, BUCKET, replication_type="RATIS", replication="ONE")
        self.base_path = f"ofs://{OZONE_FS_AUTHORITY}/{VOLUME}/{BUCKET}"

    def teardown_method(self):
        _cleanup_volume(self.admin)

    def test_create_and_check_path(self):
        path = f"{self.base_path}/test_dir"
        self.hook.make_path(path)
        assert self.hook.path_exists(path)

    def test_create_path_existing_target_policy(self):
        path = f"{self.base_path}/existing_dir"
        self.hook.make_path(path)

        self.hook.make_path(path, if_exists="ignore")

        with pytest.raises(AirflowException, match="already exists"):
            self.hook.make_path(path, if_exists="error")

    def test_upload_and_read_key(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("hello ozone")
            local_path = f.name

        try:
            remote_path = f"{self.base_path}/test_file.txt"
            self.hook.upload_key(local_path, remote_path)
            assert self.hook.key_exists(remote_path)

            content = self.hook.read_text(remote_path)
            assert "hello ozone" in content
        finally:
            os.unlink(local_path)

    def test_upload_key_existing_target_policy(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("first")
            first_local_path = f.name
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("second")
            second_local_path = f.name

        try:
            remote_path = f"{self.base_path}/existing_upload.txt"
            self.hook.upload_key(first_local_path, remote_path)

            with pytest.raises(AirflowException, match="already exists"):
                self.hook.upload_key(second_local_path, remote_path, if_exists="error")

            self.hook.upload_key(second_local_path, remote_path, if_exists="ignore")
            assert self.hook.read_text(remote_path) == "first"

            self.hook.upload_key(second_local_path, remote_path, if_exists="overwrite")
            assert self.hook.read_text(remote_path) == "second"
        finally:
            os.unlink(first_local_path)
            os.unlink(second_local_path)

    def test_download_key(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("download test")
            local_path = f.name

        download_path = local_path + ".downloaded"
        try:
            remote_path = f"{self.base_path}/download_test.txt"
            self.hook.upload_key(local_path, remote_path)

            self.hook.download_key(remote_path, download_path)
            with open(download_path) as f:
                assert f.read() == "download test"
        finally:
            os.unlink(local_path)
            if os.path.exists(download_path):
                os.unlink(download_path)

    def test_delete_key(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("to delete")
            local_path = f.name

        try:
            remote_path = f"{self.base_path}/to_delete.txt"
            self.hook.upload_key(local_path, remote_path)
            assert self.hook.key_exists(remote_path)

            self.hook.delete_key(remote_path)
            assert not self.hook.key_exists(remote_path)
        finally:
            os.unlink(local_path)

    def test_list_keys(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("list test")
            local_path = f.name

        try:
            for i in range(3):
                self.hook.upload_key(local_path, f"{self.base_path}/list_{i}.txt")

            keys = self.hook.list_keys(self.base_path)
            assert len(keys) == 3
        finally:
            os.unlink(local_path)


@pytest.mark.integration("ozone")
class TestOzoneHookEdgeCaseIntegration:
    def setup_method(self):
        self.admin = OzoneAdminHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        self.fs = OzoneFsHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_edge_volume(self.admin)
        self.admin.create_volume(EDGE_VOLUME)
        self.admin.create_bucket(EDGE_VOLUME, EDGE_BUCKET, replication_type="RATIS", replication="ONE")

    def teardown_method(self):
        _cleanup_edge_volume(self.admin)

    def test_missing_fs_path_checks_return_false(self):
        path = _edge_key_path(EDGE_MISSING_KEY)

        assert self.fs.exists(path) is False
        assert self.fs.path_exists(path) is False
        assert self.fs.key_exists(path) is False

    def test_delete_missing_fs_key_is_idempotent(self):
        path = _edge_key_path(EDGE_MISSING_KEY)

        self.fs.delete_key(path)

        assert self.fs.key_exists(path) is False

    def test_missing_bucket_checks_and_delete_are_idempotent(self):
        assert self.admin.bucket_exists(EDGE_VOLUME, EDGE_MISSING_BUCKET) is False

        self.admin.delete_bucket(EDGE_VOLUME, EDGE_MISSING_BUCKET)

        assert self.admin.bucket_exists(EDGE_VOLUME, EDGE_MISSING_BUCKET) is False

    def test_missing_volume_checks_and_delete_are_idempotent(self):
        assert self.admin.volume_exists(EDGE_MISSING_VOLUME) is False

        self.admin.delete_volume(EDGE_MISSING_VOLUME)

        assert self.admin.volume_exists(EDGE_MISSING_VOLUME) is False

    def test_existing_fs_upload_policy_fails_or_skips_without_overwrite(self):
        path = _edge_key_path(EDGE_EXISTING_KEY)
        self.fs.create_key(path, if_exists="ignore")

        with tempfile.TemporaryDirectory(prefix="ozone_existing_policy_") as tmp_dir:
            local_path = Path(tmp_dir) / "payload.txt"
            local_path.write_text("existing object policy probe\n", encoding="utf-8")

            with pytest.raises(AirflowException, match="already exists"):
                self.fs.upload_key(str(local_path), path, if_exists="error", timeout=60)

            self.fs.upload_key(str(local_path), path, if_exists="ignore", timeout=60)
