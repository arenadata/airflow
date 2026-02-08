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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.arenadata.ozone.hooks.ozone_admin import OzoneAdminHook
from airflow.providers.arenadata.ozone.operators.ozone_admin import (
    OzoneCreateBucketOperator,
    OzoneCreateVolumeOperator,
    OzoneDeleteBucketOperator,
    OzoneDeleteVolumeOperator,
    OzoneSetQuotaOperator,
)


class TestOzoneAdminOperators:
    """Unit tests for Admin Operators."""

    @patch("airflow.providers.arenadata.ozone.operators.ozone_admin.OzoneAdminHook")
    def test_create_volume_operator(self, mock_admin_hook: MagicMock):
        """Test that OzoneCreateVolumeOperator calls the hook correctly."""

        mock_hook_instance = mock_admin_hook.return_value

        operator = OzoneCreateVolumeOperator(task_id="create_volume_test", volume_name="test_vol")
        operator.execute(context={})

        mock_admin_hook.assert_called_once_with(ozone_conn_id=OzoneAdminHook.default_conn_name)
        mock_hook_instance.create_volume.assert_called_once_with("test_vol", None)

    @patch("airflow.providers.arenadata.ozone.operators.ozone_admin.OzoneAdminHook")
    def test_create_volume_operator_with_quota(self, mock_admin_hook: MagicMock):
        """Test that OzoneCreateVolumeOperator passes quota to hook."""

        mock_hook_instance = mock_admin_hook.return_value

        operator = OzoneCreateVolumeOperator(
            task_id="create_volume_test", volume_name="test_vol", quota="100GB"
        )
        operator.execute(context={})

        mock_hook_instance.create_volume.assert_called_once_with("test_vol", "100GB")

    @patch("airflow.providers.arenadata.ozone.operators.ozone_admin.OzoneAdminHook")
    def test_create_bucket_operator(self, mock_admin_hook: MagicMock):
        """Test that OzoneCreateBucketOperator calls the hook correctly."""

        mock_hook_instance = mock_admin_hook.return_value

        operator = OzoneCreateBucketOperator(
            task_id="create_bucket_test", volume_name="test_vol", bucket_name="test_bucket"
        )
        operator.execute(context={})

        mock_admin_hook.assert_called_once_with(ozone_conn_id=OzoneAdminHook.default_conn_name)
        mock_hook_instance.create_bucket.assert_called_once_with("test_vol", "test_bucket", None)

    @patch("airflow.providers.arenadata.ozone.operators.ozone_admin.OzoneAdminHook")
    def test_delete_volume_operator(self, mock_admin_hook: MagicMock):
        """Test that OzoneDeleteVolumeOperator calls the hook correctly."""

        mock_hook_instance = mock_admin_hook.return_value

        operator = OzoneDeleteVolumeOperator(task_id="delete_volume_test", volume_name="test_vol")
        operator.execute(context={})

        mock_admin_hook.assert_called_once_with(ozone_conn_id=OzoneAdminHook.default_conn_name)
        mock_hook_instance.delete_volume.assert_called_once_with("test_vol", False, False)

    @patch("airflow.providers.arenadata.ozone.operators.ozone_admin.OzoneAdminHook")
    def test_delete_volume_operator_recursive(self, mock_admin_hook: MagicMock):
        """Test that OzoneDeleteVolumeOperator passes recursive flag."""

        mock_hook_instance = mock_admin_hook.return_value

        operator = OzoneDeleteVolumeOperator(
            task_id="delete_volume_test", volume_name="test_vol", recursive=True, force=True
        )
        operator.execute(context={})

        mock_hook_instance.delete_volume.assert_called_once_with("test_vol", True, True)

    @patch("airflow.providers.arenadata.ozone.operators.ozone_admin.OzoneAdminHook")
    def test_delete_bucket_operator(self, mock_admin_hook: MagicMock):
        """Test that OzoneDeleteBucketOperator calls the hook correctly."""

        mock_hook_instance = mock_admin_hook.return_value

        operator = OzoneDeleteBucketOperator(
            task_id="delete_bucket_test", volume_name="test_vol", bucket_name="test_bucket"
        )
        operator.execute(context={})

        mock_admin_hook.assert_called_once_with(ozone_conn_id=OzoneAdminHook.default_conn_name)
        mock_hook_instance.delete_bucket.assert_called_once_with("test_vol", "test_bucket", False, False)

    @patch("airflow.providers.arenadata.ozone.operators.ozone_admin.OzoneAdminHook")
    def test_set_quota_operator_volume(self, mock_admin_hook: MagicMock):
        """Test that OzoneSetQuotaOperator sets volume quota."""

        mock_hook_instance = mock_admin_hook.return_value
        mock_hook_instance.run_cli = MagicMock()

        operator = OzoneSetQuotaOperator(task_id="set_quota_test", volume="test_vol", quota="1TB")
        operator.execute(context={})

        mock_admin_hook.assert_called_once()
        mock_hook_instance.run_cli.assert_called_once_with(
            ["ozone", "sh", "volume", "setquota", "/test_vol", "--quota", "1TB"]
        )

    @patch("airflow.providers.arenadata.ozone.operators.ozone_admin.OzoneAdminHook")
    def test_set_quota_operator_bucket(self, mock_admin_hook: MagicMock):
        """Test that OzoneSetQuotaOperator sets bucket quota."""

        mock_hook_instance = mock_admin_hook.return_value
        mock_hook_instance.run_cli = MagicMock()

        operator = OzoneSetQuotaOperator(
            task_id="set_quota_test", volume="test_vol", bucket="test_bucket", quota="100GB"
        )
        operator.execute(context={})

        mock_hook_instance.run_cli.assert_called_once_with(
            ["ozone", "sh", "bucket", "setquota", "/test_vol/test_bucket", "--quota", "100GB"]
        )

    def test_delete_volume_operator_validation(self):
        """Test that OzoneDeleteVolumeOperator validates force parameter."""

        with pytest.raises(ValueError, match="force=True requires recursive=True"):
            OzoneDeleteVolumeOperator(task_id="test", volume_name="test_vol", force=True, recursive=False)

    def test_delete_bucket_operator_validation(self):
        """Test that OzoneDeleteBucketOperator validates force parameter."""

        with pytest.raises(ValueError, match="force=True requires recursive=True"):
            OzoneDeleteBucketOperator(
                task_id="test", volume_name="test_vol", bucket_name="test_bucket", force=True, recursive=False
            )
