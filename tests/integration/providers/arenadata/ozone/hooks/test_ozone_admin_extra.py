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
"""Integration tests for OzoneAdminExtraHook — snapshots, replication, bucket links, container report."""
from __future__ import annotations

import os
import tempfile

import pytest

from airflow.models import Connection
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneAdminExtraHook, OzoneFsHook
from airflow.utils import db

import uuid

CONN_ID = "ozone_test_extra"
OZONE_HOST = os.environ.get("OZONE_HOST", "om")
OZONE_PORT = int(os.environ.get("OZONE_PORT", "9862"))
VOLUME = "inttest-extra-vol"
BUCKET = "inttest-extra-bkt"
LINK_BUCKET = "inttest-extra-link"


def _unique_snap(prefix: str) -> str:
    """Generate a unique snapshot name (no underscores, 3-63 chars)."""
    return f"{prefix}{uuid.uuid4().hex[:8]}"


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


def _delete_snapshots(hook: OzoneAdminExtraHook, names: list[str]) -> None:
    for name in names:
        try:
            hook.delete_snapshot(VOLUME, BUCKET, name)
        except Exception:
            pass


def _cleanup_base(hook: OzoneAdminExtraHook) -> None:
    """Clean up volume/bucket without touching snapshots."""
    if not hook.volume_exists(VOLUME):
        return
    try:
        if hook.bucket_exists(VOLUME, LINK_BUCKET):
            hook.delete_bucket(VOLUME, LINK_BUCKET)
    except Exception:
        pass
    try:
        fs = OzoneFsHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        fs.run_cli(
            ["ozone", "fs", "-rm", "-r", "-skipTrash", f"ofs://om/{VOLUME}/{BUCKET}/*"],
            check=False, log_output=False, retry_attempts=0,
        )
    except Exception:
        pass
    try:
        if hook.bucket_exists(VOLUME, BUCKET):
            hook.delete_bucket(VOLUME, BUCKET)
    except Exception:
        pass
    try:
        hook.delete_volume(VOLUME)
    except Exception:
        pass


# Snapshots
@pytest.mark.integration("ozone")
class TestOzoneSnapshotCreateList:

    def setup_method(self):
        self.hook = OzoneAdminExtraHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_base(self.hook)
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET, replication_type="RATIS", replication="ONE")
        self.snap_name = _unique_snap("list")

    def teardown_method(self):
        _delete_snapshots(self.hook, [self.snap_name])
        _cleanup_base(self.hook)

    def test_create_and_list_snapshot(self):
        self.hook.create_snapshot(VOLUME, BUCKET, self.snap_name)

        snapshots = self.hook.list_snapshots(VOLUME, BUCKET)
        snap_text = " ".join(snapshots)
        assert self.snap_name in snap_text


@pytest.mark.integration("ozone")
class TestOzoneSnapshotInfo:

    def setup_method(self):
        self.hook = OzoneAdminExtraHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_base(self.hook)
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET, replication_type="RATIS", replication="ONE")
        self.snap_name = _unique_snap("info")

    def teardown_method(self):
        _delete_snapshots(self.hook, [self.snap_name])
        _cleanup_base(self.hook)

    def test_get_snapshot_info(self):
        self.hook.create_snapshot(VOLUME, BUCKET, self.snap_name)

        info = self.hook.get_snapshot_info(VOLUME, BUCKET, self.snap_name)
        assert info
        assert self.snap_name in info


@pytest.mark.integration("ozone")
class TestOzoneSnapshotDelete:

    def setup_method(self):
        self.hook = OzoneAdminExtraHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_base(self.hook)
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET, replication_type="RATIS", replication="ONE")
        self.snap_name = _unique_snap("del")

    def teardown_method(self):
        _delete_snapshots(self.hook, [self.snap_name])
        _cleanup_base(self.hook)

    def test_delete_snapshot(self):
        self.hook.create_snapshot(VOLUME, BUCKET, self.snap_name)

        self.hook.delete_snapshot(VOLUME, BUCKET, self.snap_name)

        # Ozone marks deleted snapshots as SNAPSHOT_DELETED (pending reclamation),
        # so they may still appear in list. Verify status instead of absence.
        snapshots = self.hook.list_snapshots(VOLUME, BUCKET)
        snap_text = " ".join(snapshots)
        if self.snap_name in snap_text:
            assert "SNAPSHOT_DELETED" in snap_text


@pytest.mark.integration("ozone")
class TestOzoneSnapshotDiff:

    def setup_method(self):
        self.hook = OzoneAdminExtraHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_base(self.hook)
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET, replication_type="RATIS", replication="ONE")
        self.snap_a = _unique_snap("diffa")
        self.snap_b = _unique_snap("diffb")
        self.snap_jobs = _unique_snap("jobs")

    def teardown_method(self):
        _delete_snapshots(self.hook, [self.snap_a, self.snap_b, self.snap_jobs])
        _cleanup_base(self.hook)

    def test_diff_snapshots(self):
        self.hook.create_snapshot(VOLUME, BUCKET, self.snap_a)

        fs = OzoneFsHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("diff test")
            local_path = f.name
        try:
            fs.upload_key(local_path, f"ofs://om/{VOLUME}/{BUCKET}/difffile.txt")
        finally:
            os.unlink(local_path)

        self.hook.create_snapshot(VOLUME, BUCKET, self.snap_b)

        diff_output = self.hook.diff_snapshots(VOLUME, BUCKET, self.snap_a, self.snap_b)
        assert diff_output is not None

    def test_list_snapshot_diff_jobs(self):
        self.hook.create_snapshot(VOLUME, BUCKET, self.snap_jobs)

        result = self.hook.list_snapshot_diff_jobs(VOLUME, BUCKET)
        assert result is not None


@pytest.mark.integration("ozone")
class TestOzoneSnapshotMultiple:

    def setup_method(self):
        self.hook = OzoneAdminExtraHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_base(self.hook)
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET, replication_type="RATIS", replication="ONE")
        self.snap_names = [_unique_snap(f"multi{i}") for i in range(3)]

    def teardown_method(self):
        _delete_snapshots(self.hook, self.snap_names)
        _cleanup_base(self.hook)

    def test_multiple_snapshots(self):
        for name in self.snap_names:
            self.hook.create_snapshot(VOLUME, BUCKET, name)

        snapshots = self.hook.list_snapshots(VOLUME, BUCKET)
        snap_text = " ".join(snapshots)
        for name in self.snap_names:
            assert name in snap_text


# Bucket replication config
@pytest.mark.integration("ozone")
class TestOzoneBucketReplicationConfig:

    def setup_method(self):
        self.hook = OzoneAdminExtraHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_base(self.hook)
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET, replication_type="RATIS", replication="ONE")

    def teardown_method(self):
        _cleanup_base(self.hook)

    def test_set_bucket_replication_config(self):
        result = self.hook.set_bucket_replication_config(
            VOLUME, BUCKET, replication_type="RATIS", replication="ONE"
        )
        assert result is not None


# Bucket links
@pytest.mark.integration("ozone")
class TestOzoneBucketLink:

    def setup_method(self):
        self.hook = OzoneAdminExtraHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        _cleanup_base(self.hook)
        self.hook.create_volume(VOLUME)
        self.hook.create_bucket(VOLUME, BUCKET, replication_type="RATIS", replication="ONE")

    def teardown_method(self):
        _cleanup_base(self.hook)

    def test_create_bucket_link(self):
        result = self.hook.create_bucket_link(VOLUME, BUCKET, VOLUME, LINK_BUCKET)
        assert result is not None

        assert self.hook.bucket_exists(VOLUME, LINK_BUCKET)

        info = self.hook.get_bucket_info(VOLUME, LINK_BUCKET)
        assert info["name"] == LINK_BUCKET

    def test_bucket_link_reads_source_data(self):
        fs = OzoneFsHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("link test")
            local_path = f.name
        try:
            fs.upload_key(local_path, f"ofs://om/{VOLUME}/{BUCKET}/linkfile.txt")
        finally:
            os.unlink(local_path)

        self.hook.create_bucket_link(VOLUME, BUCKET, VOLUME, LINK_BUCKET)

        content = fs.read_text(f"ofs://om/{VOLUME}/{LINK_BUCKET}/linkfile.txt")
        assert "link test" in content


# Container report
@pytest.mark.integration("ozone")
class TestOzoneContainerReport:

    def test_get_container_report(self):
        hook = OzoneAdminExtraHook(ozone_conn_id=CONN_ID, retry_attempts=1)
        report = hook.get_container_report()
        assert isinstance(report, (dict, list))


# Tenant operations — require Ranger/multi-tenancy (skipped in plain mode)
@pytest.mark.integration("ozone")
@pytest.mark.skip(reason="Tenant operations require Ranger/multi-tenancy enabled Ozone cluster")
class TestOzoneTenantOperations:
    """Placeholder for tenant integration tests.

    These tests require an Ozone cluster with:
    - Ranger integration enabled
    - ozone.om.multitenancy.enabled=true
    """

    def test_create_and_delete_tenant(self):
        pass

    def test_list_tenants(self):
        pass

    def test_tenant_assign_and_revoke_user(self):
        pass

    def test_tenant_assign_and_revoke_admin(self):
        pass

    def test_list_tenant_users(self):
        pass

    def test_get_tenant_user_info(self):
        pass

    def test_get_and_set_tenant_user_secret(self):
        pass
