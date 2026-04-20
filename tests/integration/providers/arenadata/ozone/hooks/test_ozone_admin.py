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
"""Integration tests for OzoneAdminHook — quotas, ownership, idempotency."""
from __future__ import annotations

import os

import pytest

from airflow.models import Connection
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneAdminHook
from airflow.utils import db

CONN_ID = "ozone_test_admin_extra"
OZONE_HOST = os.environ.get("OZONE_HOST", "om")
OZONE_PORT = int(os.environ.get("OZONE_PORT", "9862"))
VOLUME = "inttest-admin-vol"
BUCKET = "inttest-admin-bkt"


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


def _cleanup(hook: OzoneAdminHook) -> None:
    if not hook.volume_exists(VOLUME):
        return
    try:
        if hook.bucket_exists(VOLUME, BUCKET):
            hook.delete_bucket(VOLUME, BUCKET)
    except Exception:
        pass
    try:
        hook.delete_volume(VOLUME)
    except Exception:
        pass


# test_connection
@pytest.mark.integration("ozone")
class TestOzoneAdminTestConnection:

    def test_test_connection_succeeds(self):
        hook = OzoneAdminHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        ok, msg = hook.test_connection()
        assert ok, f"test_connection failed: {msg}"
        assert "succeeded" in msg.lower()


# Idempotent create / delete
@pytest.mark.integration("ozone")
class TestOzoneAdminIdempotency:

    def setup_method(self):
        self.hook = OzoneAdminHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup(self.hook)

    def teardown_method(self):
        _cleanup(self.hook)

    def test_create_volume_idempotent(self):
        self.hook.create_volume(VOLUME)
        self.hook.create_volume(VOLUME)  # should not raise
        assert self.hook.volume_exists(VOLUME)

    def test_create_bucket_idempotent(self):
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET)
        self.hook.create_bucket(VOLUME, BUCKET)  # should not raise
        assert self.hook.bucket_exists(VOLUME, BUCKET)

    def test_delete_nonexistent_volume(self):
        self.hook.delete_volume("nonexistent_vol_xyz")  # should not raise

    def test_delete_nonexistent_bucket(self):
        self.hook.create_volume(VOLUME)
        self.hook.delete_bucket(VOLUME, "nonexistent_bkt_xyz")  # should not raise

    def test_bucket_not_exists(self):
        self.hook.create_volume(VOLUME)
        assert not self.hook.bucket_exists(VOLUME, "no_such_bucket")


# set_volume_owner
@pytest.mark.integration("ozone")
class TestOzoneAdminSetVolumeOwner:

    def setup_method(self):
        self.hook = OzoneAdminHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup(self.hook)
        self.hook.create_volume(VOLUME)

    def teardown_method(self):
        _cleanup(self.hook)

    def test_set_volume_owner(self):
        result = self.hook.set_volume_owner(VOLUME, "testuser")
        assert isinstance(result, dict)

        info = self.hook.get_volume_info(VOLUME)
        assert info.get("owner") == "testuser"


# Quotas — volume level
@pytest.mark.integration("ozone")
class TestOzoneAdminVolumeQuota:

    def setup_method(self):
        self.hook = OzoneAdminHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup(self.hook)
        self.hook.create_volume(VOLUME)

    def teardown_method(self):
        _cleanup(self.hook)

    def test_set_and_get_volume_space_quota(self):
        self.hook.set_volume_quota(VOLUME, space_quota="1GB")

        quota = self.hook.get_volume_quota(VOLUME)
        assert quota["quota_in_bytes"] is not None
        assert quota["quota_in_bytes"] > 0

    def test_set_and_get_volume_namespace_quota(self):
        # Ozone requires space quota to be set before namespace quota
        self.hook.set_volume_quota(VOLUME, space_quota="1GB", namespace_quota=100)

        quota = self.hook.get_volume_quota(VOLUME)
        assert quota["quota_in_namespace"] == 100

    def test_set_both_volume_quotas(self):
        self.hook.set_volume_quota(VOLUME, space_quota="500MB", namespace_quota=50)

        quota = self.hook.get_volume_quota(VOLUME)
        assert quota["quota_in_bytes"] is not None
        assert quota["quota_in_bytes"] > 0
        assert quota["quota_in_namespace"] == 50

    def test_clear_volume_space_quota(self):
        self.hook.set_volume_quota(VOLUME, space_quota="1GB")
        self.hook.clear_volume_quota(VOLUME, clear_space_quota=True)

        quota = self.hook.get_volume_quota(VOLUME)
        # After clearing, quota_in_bytes should be -1 (unlimited) or very large
        assert quota["quota_in_bytes"] is not None
        assert quota["quota_in_bytes"] < 0 or quota["quota_in_bytes"] > 10**18

    def test_clear_volume_namespace_quota(self):
        self.hook.set_volume_quota(VOLUME, space_quota="1GB", namespace_quota=100)
        self.hook.clear_volume_quota(VOLUME, clear_space_quota=False, clear_namespace_quota=True)

        quota = self.hook.get_volume_quota(VOLUME)
        assert quota["quota_in_namespace"] is not None
        assert quota["quota_in_namespace"] < 0 or quota["quota_in_namespace"] > 10**18

    def test_set_quota_generic_method(self):
        self.hook.set_quota(volume=VOLUME, space_quota="2GB")

        quota = self.hook.get_quota(volume=VOLUME)
        assert quota["quota_in_bytes"] is not None
        assert quota["quota_in_bytes"] > 0


# Quotas — bucket level
@pytest.mark.integration("ozone")
class TestOzoneAdminBucketQuota:

    def setup_method(self):
        self.hook = OzoneAdminHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup(self.hook)
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET, replication_type="RATIS", replication="ONE")

    def teardown_method(self):
        _cleanup(self.hook)

    def test_set_and_get_bucket_space_quota(self):
        self.hook.set_bucket_quota(VOLUME, BUCKET, space_quota="500MB")

        quota = self.hook.get_bucket_quota(VOLUME, BUCKET)
        assert quota["quota_in_bytes"] is not None
        assert quota["quota_in_bytes"] > 0

    def test_set_and_get_bucket_namespace_quota(self):
        # Ozone requires space quota to be set before namespace quota
        self.hook.set_bucket_quota(VOLUME, BUCKET, space_quota="500MB", namespace_quota=200)

        quota = self.hook.get_bucket_quota(VOLUME, BUCKET)
        assert quota["quota_in_namespace"] == 200

    def test_clear_bucket_space_quota(self):
        self.hook.set_bucket_quota(VOLUME, BUCKET, space_quota="500MB")
        self.hook.clear_bucket_quota(VOLUME, BUCKET, clear_space_quota=True)

        quota = self.hook.get_bucket_quota(VOLUME, BUCKET)
        assert quota["quota_in_bytes"] is not None
        assert quota["quota_in_bytes"] < 0 or quota["quota_in_bytes"] > 10**18

    def test_set_quota_generic_method_for_bucket(self):
        self.hook.set_quota(volume=VOLUME, bucket=BUCKET, space_quota="100MB")

        quota = self.hook.get_quota(volume=VOLUME, bucket=BUCKET)
        assert quota["quota_in_bytes"] is not None
        assert quota["quota_in_bytes"] > 0
