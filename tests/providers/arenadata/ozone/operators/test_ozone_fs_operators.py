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

from airflow.providers.arenadata.ozone.hooks.ozone_fs import OzoneFsHook
from airflow.providers.arenadata.ozone.operators.ozone_fs import OzoneDeleteKeyOperator, OzoneListOperator


class TestOzoneFsOperators:
    """Unit tests for File System Operators."""

    @patch("airflow.providers.arenadata.ozone.operators.ozone_fs.OzoneFsHook")
    def test_ozone_list_operator(self, mock_ozone_fs_hook: MagicMock):
        """Test that OzoneListOperator calls the hook and returns its result via XCom."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        expected_files = ["ofs://vol1/b1/f1", "ofs://vol1/b1/f2"]
        mock_hook_instance.list_paths.return_value = expected_files

        operator = OzoneListOperator(task_id="list_files_test", path="ofs://vol1/b1/")
        result = operator.execute(context={})

        mock_ozone_fs_hook.assert_called_once_with(ozone_conn_id=OzoneFsHook.default_conn_name)
        mock_hook_instance.list_paths.assert_called_once_with("ofs://vol1/b1/")
        assert result == expected_files

    @patch("airflow.providers.arenadata.ozone.operators.ozone_fs.OzoneFsHook")
    def test_ozone_delete_key_operator(self, mock_ozone_fs_hook: MagicMock):
        """Test that OzoneDeleteKeyOperator calls the correct run_cli command."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.exists.return_value = True

        operator = OzoneDeleteKeyOperator(task_id="delete_key_test", path="ofs://vol1/b1/f1")
        operator.execute(context={})

        mock_hook_instance.exists.assert_called_once_with("ofs://vol1/b1/f1")
        mock_hook_instance.run_cli.assert_called_once_with(["ozone", "fs", "-rm", "ofs://vol1/b1/f1"])

    @patch("airflow.providers.arenadata.ozone.operators.ozone_fs.OzoneFsHook")
    def test_ozone_delete_key_operator_idempotent(self, mock_ozone_fs_hook: MagicMock):
        """Test that OzoneDeleteKeyOperator is idempotent (skips deletion if file doesn't exist)."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.exists.return_value = False

        operator = OzoneDeleteKeyOperator(task_id="delete_key_test", path="ofs://vol1/b1/f1")
        operator.execute(context={})

        mock_hook_instance.exists.assert_called_once_with("ofs://vol1/b1/f1")
        mock_hook_instance.run_cli.assert_not_called()

    @patch("airflow.providers.arenadata.ozone.operators.ozone_fs.OzoneFsHook")
    def test_ozone_delete_key_operator_wildcard(self, mock_ozone_fs_hook: MagicMock):
        """Test that OzoneDeleteKeyOperator handles wildcard patterns."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.list_paths.return_value = [
            "ofs://vol1/b1/f1",
            "ofs://vol1/b1/f2",
            "ofs://vol1/b1/f3",
        ]

        operator = OzoneDeleteKeyOperator(task_id="delete_key_test", path="ofs://vol1/b1/*")
        operator.execute(context={})

        mock_hook_instance.list_paths.assert_called_once_with("ofs://vol1/b1")
        assert mock_hook_instance.run_cli.call_count == 3
        mock_hook_instance.run_cli.assert_any_call(["ozone", "fs", "-rm", "ofs://vol1/b1/f1"])
        mock_hook_instance.run_cli.assert_any_call(["ozone", "fs", "-rm", "ofs://vol1/b1/f2"])
        mock_hook_instance.run_cli.assert_any_call(["ozone", "fs", "-rm", "ofs://vol1/b1/f3"])
