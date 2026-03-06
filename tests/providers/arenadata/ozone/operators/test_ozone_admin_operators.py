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

from airflow.providers.arenadata.ozone.hooks.ozone import OzoneAdminHook
from airflow.providers.arenadata.ozone.operators.ozone import (
    OzoneCreateVolumeOperator,
    OzoneDeleteBucketOperator,
    OzoneDeleteVolumeOperator,
    OzoneSetQuotaOperator,
)


class TestOzoneAdminOperators:
    """Unit tests for Admin Operators."""

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneAdminHook")
    def test_create_volume_operator(self, mock_admin_hook: MagicMock):
        """Test that OzoneCreateVolumeOperator calls the hook correctly."""

        mock_hook_instance = mock_admin_hook.return_value

        operator = OzoneCreateVolumeOperator(task_id="create_volume_test", volume_name="test_vol")
        operator.execute(context={})

        mock_admin_hook.assert_called_once()
        assert mock_admin_hook.call_args.kwargs["ozone_conn_id"] == OzoneAdminHook.default_conn_name
        mock_hook_instance.create_volume.assert_called_once()
        assert mock_hook_instance.create_volume.call_args.args == ("test_vol", None)

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneAdminHook")
    def test_delete_volume_operator_recursive(self, mock_admin_hook: MagicMock):
        """Test that OzoneDeleteVolumeOperator passes recursive flag."""

        mock_hook_instance = mock_admin_hook.return_value

        operator = OzoneDeleteVolumeOperator(
            task_id="delete_volume_test", volume_name="test_vol", recursive=True, force=True
        )
        operator.execute(context={})

        mock_hook_instance.delete_volume.assert_called_once()
        assert mock_hook_instance.delete_volume.call_args.args == ("test_vol", True, True)

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneAdminHook")
    def test_set_quota_operator_volume(self, mock_admin_hook: MagicMock):
        """Test that OzoneSetQuotaOperator sets volume quota."""

        mock_hook_instance = mock_admin_hook.return_value
        mock_hook_instance.run_cli = MagicMock()

        operator = OzoneSetQuotaOperator(task_id="set_quota_test", volume="test_vol", quota="1TB")
        operator.execute(context={})

        mock_admin_hook.assert_called_once()
        mock_hook_instance.run_cli.assert_called_once()
        quota_cmd = mock_hook_instance.run_cli.call_args.args[0]
        assert quota_cmd[:4] == ["ozone", "sh", "volume", "setquota"]
        assert "/test_vol" in quota_cmd
        assert "--quota" in quota_cmd
        assert "1TB" in quota_cmd

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
