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

# A constant for the path to the mocked run_cli method
MOCK_CLI_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.OzoneCliHook.run_cli"
MOCK_CLI_CHECK_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.OzoneCliHook.run_cli_check"


@pytest.fixture
def ozone_fs_hook():
    """Provides a reusable instance of the OzoneFsHook."""

    return OzoneFsHook(ozone_conn_id="test_conn")


class TestOzoneFsHook:
    """
    Unit tests for OzoneFsHook.
    We mock the `run_cli` method from the parent OzoneCliHook to isolate our tests
    from the actual subprocess execution.
    """

    @patch(MOCK_CLI_PATH)
    def test_mkdir_success(self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook):
        """Test that mkdir calls `run_cli` with the correct 'ozone fs -mkdir -p' command."""

        test_path = "ofs://vol1/bucket1/new_dir"

        ozone_fs_hook.mkdir(test_path)

        mock_run_cli.assert_called_once_with(["ozone", "fs", "-mkdir", "-p", test_path])

    @patch(MOCK_CLI_PATH)
    def test_list_paths_returns_parsed_list(self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook):
        """Test that `list_paths` correctly parses the multiline output from `run_cli`."""

        test_path = "ofs://vol1/bucket1/"
        mock_cli_output = "ofs://vol1/bucket1/file1.txt\nofs://vol1/bucket1/file2.csv"
        mock_run_cli.return_value = mock_cli_output

        result = ozone_fs_hook.list_paths(test_path)

        mock_run_cli.assert_called_once_with(["ozone", "fs", "-ls", "-C", test_path])
        assert result == ["ofs://vol1/bucket1/file1.txt", "ofs://vol1/bucket1/file2.csv"]

    @patch(MOCK_CLI_CHECK_PATH)
    def test_exists_false(self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook):
        """Test that `exists` returns False when the CLI command fails."""

        mock_run_cli.return_value = MagicMock(returncode=1, stdout="", stderr="not found")

        assert ozone_fs_hook.exists("ofs://path/does_not_exist") is False
        mock_run_cli.assert_called_once_with(
            ["ozone", "fs", "-test", "-e", "ofs://path/does_not_exist"], timeout=30
        )

    @patch(MOCK_CLI_CHECK_PATH)
    def test_exists_raises_when_cli_missing(self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook):
        """Test that `exists` re-raises when the Ozone CLI is not available."""

        mock_run_cli.side_effect = AirflowException(
            "Ozone CLI not found. Please ensure Ozone is installed and available in PATH."
        )

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

    @patch(MOCK_CLI_CHECK_PATH)
    def test_test_connection_failure(self, mock_run_cli_check: MagicMock, ozone_fs_hook: OzoneFsHook):
        """test_connection returns False and error text on failure."""
        mock_run_cli_check.return_value = MagicMock(returncode=1, stdout="", stderr="auth failed")
        ok, message = ozone_fs_hook.test_connection()
        assert ok is False
        assert "auth failed" in message
