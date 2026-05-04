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
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor
from airflow.utils import db

CONN_ID = "ozone_sensor_test"
OZONE_HOST = os.environ.get("OZONE_HOST", "om")
OZONE_PORT = int(os.environ.get("OZONE_PORT", "9862"))
VOLUME = "inttest-sensor-volume"
BUCKET = "inttest-sensor-bucket"


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
class TestOzoneKeySensorIntegration:
    def setup_method(self):
        self.admin = OzoneAdminHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        self.fs = OzoneFsHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_volume(self.admin)
        self.admin.create_volume(VOLUME)
        self.admin.create_bucket(VOLUME, BUCKET, replication_type="RATIS", replication="ONE")
        self.base_path = f"ofs://om/{VOLUME}/{BUCKET}"

    def teardown_method(self):
        _cleanup_volume(self.admin)

    def _upload_text(self, remote_path: str, content: str = "sensor payload\n") -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as file:
            file.write(content)
            local_path = file.name
        try:
            self.fs.upload_key(local_path, remote_path)
        finally:
            os.unlink(local_path)

    def test_poke_returns_true_for_existing_key(self):
        remote_path = f"{self.base_path}/ready.txt"
        self._upload_text(remote_path)

        sensor = OzoneKeySensor(
            task_id="wait_for_ready",
            path=remote_path,
            ozone_conn_id=CONN_ID,
            retry_attempts=1,
            cli_timeout=30,
            timeout=120,
        )

        assert sensor.poke({}) is True
        assert sensor.cli_timeout == 30
        assert sensor.timeout == 120

    def test_poke_returns_false_for_missing_key(self):
        sensor = OzoneKeySensor(
            task_id="wait_for_missing",
            path=f"{self.base_path}/missing.txt",
            ozone_conn_id=CONN_ID,
            retry_attempts=1,
            cli_timeout=30,
            timeout=120,
        )

        assert sensor.poke({}) is False
        assert sensor.cli_timeout == 30
        assert sensor.timeout == 120
