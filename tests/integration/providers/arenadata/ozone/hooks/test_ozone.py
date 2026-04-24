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

import pytest

from airflow.models import Connection
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneAdminHook, OzoneFsHook
from airflow.utils import db

CONN_ID = "ozone_test"
OZONE_HOST = os.environ.get("OZONE_HOST", "om")
OZONE_PORT = int(os.environ.get("OZONE_PORT", "9862"))
VOLUME = "inttest-volume"
BUCKET = "inttest-bucket"


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
        # Purge bucket contents via fs -rm -r, then delete bucket
        try:
            hook.run_cli(
                ["ozone", "fs", "-rm", "-r", "-skipTrash", f"ofs://om/{VOLUME}/{BUCKET}/*"],
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
        self.base_path = f"ofs://om/{VOLUME}/{BUCKET}"

    def teardown_method(self):
        _cleanup_volume(self.admin)

    def test_create_and_check_path(self):
        path = f"{self.base_path}/test_dir"
        self.hook.create_path(path)
        assert self.hook.path_exists(path)

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
