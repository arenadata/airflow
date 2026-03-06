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
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneFsHook
from airflow.providers.arenadata.ozone.utils.errors import OzoneCliError

# A constant for the path to the mocked run_cli method
MOCK_CLI_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.OzoneCliHook.run_cli"
MOCK_RUN_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.CliRunner.run_ozone_once"
MOCK_RUN_RETRY_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.CliRunner.run_ozone"


@pytest.fixture
def ozone_fs_hook():
    """Provides a reusable instance of the OzoneFsHook."""
    hook = OzoneFsHook(ozone_conn_id="test_conn")
    conn = MagicMock()
    conn.host = "ozone-om"
    conn.port = 9862
    conn.extra_dejson = {}
    hook.__dict__["connection"] = conn
    return hook


class TestOzoneFsHook:
    """
    Unit tests for OzoneFsHook.
    We mock the `run_cli` method from the parent OzoneCliHook to isolate our tests
    from the actual subprocess execution.
    """

    @patch(MOCK_RUN_RETRY_PATH)
    def test_exists_false(self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook):
        """Test that `exists` returns False when the CLI command fails."""
        mock_run_cli.side_effect = OzoneCliError(
            "not found",
            command=["ozone", "fs", "-test", "-e", "ofs://path/does_not_exist"],
            stderr="not found",
            returncode=1,
            retryable=False,
        )
        assert ozone_fs_hook.exists("ofs://path/does_not_exist") is False
        mock_run_cli.assert_called_once()
        command = mock_run_cli.call_args.args[0]
        assert command[:4] == ["ozone", "fs", "-test", "-e"]
        assert command[-1] == "ofs://path/does_not_exist"

    @patch(MOCK_RUN_RETRY_PATH)
    def test_exists_raises_when_cli_missing(self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook):
        """Test that `exists` re-raises when the Ozone CLI is not available."""
        mock_run_cli.side_effect = OzoneCliError("Ozone CLI not found.", retryable=False)
        with pytest.raises(AirflowException):
            ozone_fs_hook.exists("ofs://path/any")

    @patch(MOCK_CLI_PATH)
    def test_copy_from_local_raises_when_file_missing(
        self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook, tmp_path
    ):
        """copy_from_local should raise AirflowException when local file does not exist."""

        missing_path = tmp_path / "missing.txt"

        with pytest.raises(AirflowException):
            ozone_fs_hook.copy_from_local(str(missing_path), "ofs://vol1/bucket1/file.txt")

        mock_run_cli.assert_not_called()

    @patch(MOCK_RUN_PATH)
    def test_test_connection_failure(self, mock_run_cli_check: MagicMock, ozone_fs_hook: OzoneFsHook):
        """test_connection returns False and error text on failure."""
        mock_run_cli_check.return_value = MagicMock(returncode=1, stdout="", stderr="auth failed")
        ok, message = ozone_fs_hook.test_connection()
        assert ok is False
        assert "auth failed" in message

    @patch(MOCK_RUN_PATH)
    def test_test_connection_timeout(self, mock_run_cli_check: MagicMock, ozone_fs_hook: OzoneFsHook):
        """test_connection returns False when CLI probe times out."""
        mock_run_cli_check.side_effect = OzoneCliError("Ozone command timed out", retryable=True)
        ok, message = ozone_fs_hook.test_connection()
        assert ok is False
        assert "connection test failed" in message
