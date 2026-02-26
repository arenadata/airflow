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

import subprocess
from unittest.mock import MagicMock, patch

from airflow.providers.arenadata.ozone.transfers.ozone_backup import OzoneBackupOperator


class TestOzoneBackupOperator:
    """Unit tests for OzoneBackupOperator."""

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_backup.subprocess.run")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_backup.OzoneAdminHook")
    def test_execute_create_snapshot(self, mock_admin_hook: MagicMock, mock_subprocess_run: MagicMock):
        """Test that OzoneBackupOperator creates a snapshot correctly."""

        mock_hook_instance = mock_admin_hook.return_value
        mock_hook_instance._get_merged_env = MagicMock(return_value=None)
        mock_subprocess_run.return_value = MagicMock(returncode=0)

        operator = OzoneBackupOperator(
            task_id="test_backup", volume="test_vol", bucket="test_bucket", snapshot_name="snapshot_20240101"
        )
        operator.execute(context={})

        mock_admin_hook.assert_called_once_with(ozone_conn_id="ozone_admin_default")
        # subprocess.run called with snapshot create; hook may prepend --config etc.
        assert mock_subprocess_run.call_count == 1
        cmd_args, cmd_kwargs = mock_subprocess_run.call_args
        cmd_list = cmd_args[0]
        assert "ozone" in cmd_list[0], "Expected ozone CLI as the first element of the command"
        assert ["sh", "snapshot", "create", "/test_vol/test_bucket", "snapshot_20240101"] == cmd_list[-5:]
        assert cmd_kwargs["check"] is True
        assert cmd_kwargs["timeout"] == 300

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_backup.subprocess.run")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_backup.OzoneAdminHook")
    def test_execute_custom_connection(self, mock_admin_hook: MagicMock, mock_subprocess_run: MagicMock):
        """Test that OzoneBackupOperator uses custom connection ID."""

        mock_hook_instance = mock_admin_hook.return_value
        mock_hook_instance._get_merged_env = MagicMock(return_value=None)
        mock_subprocess_run.return_value = MagicMock(returncode=0)

        operator = OzoneBackupOperator(
            task_id="test_backup",
            volume="test_vol",
            bucket="test_bucket",
            snapshot_name="snapshot_20240101",
            ozone_conn_id="custom_conn",
        )
        operator.execute(context={})

        mock_admin_hook.assert_called_once_with(ozone_conn_id="custom_conn")
        mock_subprocess_run.assert_called_once()

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_backup.subprocess.run")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_backup.OzoneAdminHook")
    def test_execute_idempotent_snapshot_exists(
        self, mock_admin_hook: MagicMock, mock_subprocess_run: MagicMock
    ):
        """Existing snapshot is treated as success (no exception)."""

        mock_hook_instance = mock_admin_hook.return_value
        mock_hook_instance._get_merged_env = MagicMock(return_value=None)

        mock_error = subprocess.CalledProcessError(
            returncode=255,
            cmd=["ozone", "sh", "snapshot", "create", "/test_vol/test_bucket", "snapshot_20240101"],
            stderr="FILE_ALREADY_EXISTS Snapshot already exists",
        )
        mock_subprocess_run.side_effect = mock_error

        operator = OzoneBackupOperator(
            task_id="test_backup", volume="test_vol", bucket="test_bucket", snapshot_name="snapshot_20240101"
        )
        operator.execute(context={})

        mock_subprocess_run.assert_called_once()
