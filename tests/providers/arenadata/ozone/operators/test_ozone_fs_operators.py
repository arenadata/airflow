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
from airflow.providers.arenadata.ozone.operators.ozone import OzoneDeleteKeyOperator, OzoneListOperator


class TestOzoneFsOperators:
    """Unit tests for File System Operators."""

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_ozone_list_operator(self, mock_ozone_fs_hook: MagicMock):
        """Test that OzoneListOperator calls the hook and returns its result via XCom."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        expected_files = ["ofs://vol1/b1/f1", "ofs://vol1/b1/f2"]
        mock_hook_instance.list_paths.return_value = expected_files

        operator = OzoneListOperator(task_id="list_files_test", path="ofs://vol1/b1/")
        result = operator.execute(context={})

        mock_ozone_fs_hook.assert_called_once()
        assert mock_ozone_fs_hook.call_args.kwargs["ozone_conn_id"] == OzoneFsHook.default_conn_name
        mock_hook_instance.list_paths.assert_called_once()
        assert mock_hook_instance.list_paths.call_args.args[0] == "ofs://vol1/b1/"
        assert result == expected_files

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_ozone_delete_key_operator(self, mock_ozone_fs_hook: MagicMock):
        """Test that OzoneDeleteKeyOperator calls the correct run_cli command."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.exists.return_value = True

        operator = OzoneDeleteKeyOperator(task_id="delete_key_test", path="ofs://vol1/b1/f1")
        operator.execute(context={})

        mock_hook_instance.exists.assert_called_once()
        assert mock_hook_instance.exists.call_args.args[0] == "ofs://vol1/b1/f1"
        mock_hook_instance.run_cli.assert_called_once()
        delete_cmd = mock_hook_instance.run_cli.call_args.args[0]
        assert delete_cmd[:3] == ["ozone", "fs", "-rm"]
        assert delete_cmd[-1] == "ofs://vol1/b1/f1"

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_ozone_delete_key_operator_idempotent(self, mock_ozone_fs_hook: MagicMock):
        """DeleteKeyOperator skips run_cli when path does not exist."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.exists.return_value = False

        operator = OzoneDeleteKeyOperator(task_id="delete_key_test", path="ofs://vol1/b1/f1")
        operator.execute(context={})

        mock_hook_instance.exists.assert_called_once()
        assert mock_hook_instance.exists.call_args.args[0] == "ofs://vol1/b1/f1"
        mock_hook_instance.run_cli.assert_not_called()

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
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

        mock_hook_instance.list_paths.assert_called_once()
        assert mock_hook_instance.list_paths.call_args.args[0] == "ofs://vol1/b1"
        deleted_targets = {call.args[0][-1] for call in mock_hook_instance.run_cli.call_args_list}
        assert deleted_targets == {"ofs://vol1/b1/f1", "ofs://vol1/b1/f2", "ofs://vol1/b1/f3"}

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_ozone_delete_key_operator_wildcard_propagates_non_retryable_error(
        self, mock_ozone_fs_hook: MagicMock
    ):
        """Non-retryable list failures should not trigger fallback delete."""
        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.list_paths.side_effect = AirflowException("auth failed")

        operator = OzoneDeleteKeyOperator(task_id="delete_key_test", path="ofs://vol1/b1/*")
        with pytest.raises(AirflowException, match="auth failed"):
            operator.execute(context={})
        mock_hook_instance.run_cli.assert_not_called()
