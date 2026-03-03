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

# The path to the mocked method is in the base class
MOCK_CLI_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.OzoneCliHook.run_cli"


@pytest.fixture
def admin_hook():
    """Provides a reusable instance of the OzoneAdminHook."""

    return OzoneAdminHook(ozone_conn_id="test_admin_conn")


class TestOzoneAdminHook:
    """Unit tests for OzoneAdminHook."""

    @patch("airflow.providers.arenadata.ozone.hooks.ozone.subprocess.run")
    def test_create_volume(self, mock_subprocess_run: MagicMock, admin_hook: OzoneAdminHook):
        """Test that `create_volume` calls the correct CLI command without a quota."""

        mock_subprocess_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        admin_hook.create_volume(volume_name="test_vol")
        mock_subprocess_run.assert_called_once()
        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args == ["ozone", "sh", "volume", "create", "/test_vol"]

    @patch("airflow.providers.arenadata.ozone.hooks.ozone.subprocess.run")
    def test_create_volume_with_quota(self, mock_subprocess_run: MagicMock, admin_hook: OzoneAdminHook):
        """Test that `create_volume` correctly adds the --quota argument."""

        mock_subprocess_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        admin_hook.create_volume(volume_name="test_vol_quota", quota="100GB")
        mock_subprocess_run.assert_called_once()
        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args == ["ozone", "sh", "volume", "create", "/test_vol_quota", "--quota", "100GB"]

    @patch("airflow.providers.arenadata.ozone.hooks.ozone.subprocess.run")
    def test_create_bucket(self, mock_subprocess_run: MagicMock, admin_hook: OzoneAdminHook):
        """Test the `create_bucket` command."""

        mock_subprocess_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        admin_hook.create_bucket(volume_name="test_vol", bucket_name="test_bkt")
        mock_subprocess_run.assert_called_once()
        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args == ["ozone", "sh", "bucket", "create", "/test_vol/test_bkt"]

    @patch(MOCK_CLI_PATH)
    def test_get_container_report(self, mock_run_cli: MagicMock, admin_hook: OzoneAdminHook):
        """Test that `get_container_report` parses JSON output correctly."""

        # Simulate a JSON string returned by the CLI
        mock_run_cli.return_value = '{"total": 1, "containers": [{"id": 1}]}'

        result = admin_hook.get_container_report()

        mock_run_cli.assert_called_once_with(["ozone", "admin", "container", "report", "--json"])
        assert result["total"] == 1
        assert len(result["containers"]) == 1
