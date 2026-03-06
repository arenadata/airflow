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

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneAdminHook
from airflow.providers.arenadata.ozone.utils.errors import OzoneCliError

MOCK_CLI_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.OzoneCliHook.run_cli"
MOCK_RUN_RETRY_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.CliRunner.run_ozone"


@pytest.fixture
def admin_hook():
    """Provides a reusable instance of the OzoneAdminHook."""
    hook = OzoneAdminHook(ozone_conn_id="test_admin_conn")
    conn = MagicMock()
    conn.host = "ozone-om"
    conn.port = 9862
    conn.extra_dejson = {}
    hook.__dict__["connection"] = conn
    return hook


class TestOzoneAdminHook:
    """Unit tests for OzoneAdminHook."""

    @patch(MOCK_RUN_RETRY_PATH)
    def test_create_volume_uses_run_with_retry(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        """create_volume should execute via run_with_retry and succeed on returncode=0."""
        mock_run_with_retry.return_value = MagicMock(returncode=0, stdout="", stderr="")
        admin_hook.create_volume(volume_name="test_vol")
        mock_run_with_retry.assert_called_once()
        assert mock_run_with_retry.call_args.args[0] == ["ozone", "sh", "volume", "create", "/test_vol"]

    @patch(MOCK_RUN_RETRY_PATH)
    def test_create_volume_already_exists_is_idempotent(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        """create_volume should not fail on VOLUME_ALREADY_EXISTS marker."""
        mock_run_with_retry.side_effect = OzoneCliError(
            "VOLUME_ALREADY_EXISTS",
            command=["ozone", "sh", "volume", "create", "/test_vol"],
            stderr="VOLUME_ALREADY_EXISTS",
            returncode=1,
            retryable=False,
        )
        admin_hook.create_volume(volume_name="test_vol")
        mock_run_with_retry.assert_called_once()

    @patch(MOCK_RUN_RETRY_PATH)
    def test_create_bucket_raises_on_non_idempotent_failure(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        """create_bucket should raise AirflowException on non-idempotent CLI errors."""
        mock_run_with_retry.side_effect = OzoneCliError(
            "ACCESS_DENIED",
            command=["ozone", "sh", "bucket", "create", "/test_vol/test_bkt", "--quota", "100GB"],
            stderr="ACCESS_DENIED",
            returncode=1,
            retryable=False,
        )
        with pytest.raises(AirflowException, match="Ozone command failed"):
            admin_hook.create_bucket(volume_name="test_vol", bucket_name="test_bkt", quota="100GB")

    @patch(MOCK_CLI_PATH)
    def test_get_container_report(self, mock_run_cli: MagicMock, admin_hook: OzoneAdminHook):
        """Test that `get_container_report` parses JSON output correctly."""

        # Simulate a JSON string returned by the CLI
        mock_run_cli.return_value = '{"total": 1, "containers": [{"id": 1}]}'

        result = admin_hook.get_container_report()

        mock_run_cli.assert_called_once_with(
            ["ozone", "admin", "container", "report", "--json"], timeout=3600
        )
        assert result["total"] == 1
        assert len(result["containers"]) == 1
