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

from airflow.providers.arenadata.ozone.hooks.ozone import OzoneFsHook
from airflow.providers.arenadata.ozone.operators.ozone import (
    OzoneCopyOperator,
    OzoneCreatePathOperator,
    OzoneDeleteKeyOperator,
    OzoneDeletePathOperator,
    OzoneDownloadFileOperator,
    OzoneListOperator,
    OzoneMoveOperator,
    OzonePathExistsOperator,
)


class TestOzoneFsOperators:
    """Unit tests for File System Operators."""

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_ozone_list_operator(self, mock_ozone_fs_hook: MagicMock):
        """Test that OzoneListOperator calls the hook and returns its result via XCom."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        expected_files = ["ofs://vol1/b1/f1", "ofs://vol1/b1/f2"]
        mock_hook_instance.list_keys.return_value = expected_files

        operator = OzoneListOperator(task_id="list_files_test", path="ofs://vol1/b1/")
        result = operator.execute(context={})

        mock_ozone_fs_hook.assert_called_once()
        assert mock_ozone_fs_hook.call_args.kwargs["ozone_conn_id"] == OzoneFsHook.default_conn_name
        mock_hook_instance.list_keys.assert_called_once()
        assert mock_hook_instance.list_keys.call_args.args[0] == "ofs://vol1/b1/"
        assert result == expected_files

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_ozone_delete_key_operator(self, mock_ozone_fs_hook: MagicMock):
        """Test that OzoneDeleteKeyOperator delegates deletion to the hook."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.delete_key = MagicMock()

        operator = OzoneDeleteKeyOperator(task_id="delete_key_test", path="ofs://vol1/b1/f1")
        operator.execute(context={})

        mock_hook_instance.delete_key.assert_called_once_with("ofs://vol1/b1/f1", timeout=operator.timeout)

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_ozone_delete_key_operator_idempotent(self, mock_ozone_fs_hook: MagicMock):
        """DeleteKeyOperator delegates idempotent behavior to the hook."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.delete_key = MagicMock()

        operator = OzoneDeleteKeyOperator(task_id="delete_key_test", path="ofs://vol1/b1/f1")
        operator.execute(context={})

        mock_hook_instance.delete_key.assert_called_once_with("ofs://vol1/b1/f1", timeout=operator.timeout)

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_ozone_delete_key_operator_wildcard(self, mock_ozone_fs_hook: MagicMock):
        """Wildcard handling is delegated to hook.delete_key."""

        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.delete_key = MagicMock()

        operator = OzoneDeleteKeyOperator(task_id="delete_key_test", path="ofs://vol1/b1/*")
        operator.execute(context={})

        mock_hook_instance.delete_key.assert_called_once_with("ofs://vol1/b1/*", timeout=operator.timeout)

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_ozone_delete_key_operator_wildcard_propagates_non_retryable_error(
        self, mock_ozone_fs_hook: MagicMock
    ):
        """Errors from hook.delete_key should propagate."""
        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.delete_key.side_effect = RuntimeError("auth failed")

        operator = OzoneDeleteKeyOperator(task_id="delete_key_test", path="ofs://vol1/b1/*")
        with pytest.raises(RuntimeError, match="auth failed"):
            operator.execute(context={})
        mock_hook_instance.delete_key.assert_called_once_with("ofs://vol1/b1/*", timeout=operator.timeout)

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_create_path_operator(self, mock_ozone_fs_hook: MagicMock):
        """CreatePathOperator should call hook.create_path."""
        operator = OzoneCreatePathOperator(task_id="create_path", path="ofs://vol1/b1/dir")
        operator.execute(context={})
        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.create_path.assert_called_once_with("ofs://vol1/b1/dir", timeout=operator.timeout)

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_delete_path_operator(self, mock_ozone_fs_hook: MagicMock):
        """DeletePathOperator should call hook.delete_path."""
        operator = OzoneDeletePathOperator(task_id="delete_path", path="ofs://vol1/b1/dir", recursive=True)
        operator.execute(context={})
        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.delete_path.assert_called_once_with(
            "ofs://vol1/b1/dir",
            recursive=True,
            timeout=operator.timeout,
        )

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_path_exists_operator(self, mock_ozone_fs_hook: MagicMock):
        """PathExistsOperator should return hook.path_exists value."""
        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.path_exists.return_value = True
        operator = OzonePathExistsOperator(task_id="path_exists", path="ofs://vol1/b1/dir")
        result = operator.execute(context={})
        assert result is True
        mock_hook_instance.path_exists.assert_called_once_with("ofs://vol1/b1/dir", timeout=operator.timeout)

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_copy_operator(self, mock_ozone_fs_hook: MagicMock):
        """CopyOperator should call hook.copy_path."""
        operator = OzoneCopyOperator(
            task_id="copy_path",
            source_path="ofs://vol1/b1/src.txt",
            dest_path="ofs://vol1/b1/dst.txt",
        )
        operator.execute(context={})
        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.copy_path.assert_called_once_with(
            "ofs://vol1/b1/src.txt",
            "ofs://vol1/b1/dst.txt",
            timeout=operator.timeout,
        )

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_move_operator(self, mock_ozone_fs_hook: MagicMock):
        """MoveOperator should call hook.move."""
        operator = OzoneMoveOperator(
            task_id="move_path",
            source_path="ofs://vol1/b1/src.txt",
            dest_path="ofs://vol1/b1/dst.txt",
        )
        operator.execute(context={})
        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.move.assert_called_once_with(
            "ofs://vol1/b1/src.txt",
            "ofs://vol1/b1/dst.txt",
            timeout=operator.timeout,
        )

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneFsHook")
    def test_download_file_operator(self, mock_ozone_fs_hook: MagicMock):
        """DownloadFileOperator should call hook.download_key."""
        operator = OzoneDownloadFileOperator(
            task_id="download_path",
            remote_path="ofs://vol1/b1/src.txt",
            local_path="/tmp/dst.txt",
            overwrite=True,
        )
        operator.execute(context={})
        mock_hook_instance = mock_ozone_fs_hook.return_value
        mock_hook_instance.download_key.assert_called_once_with(
            "ofs://vol1/b1/src.txt",
            "/tmp/dst.txt",
            overwrite=True,
            timeout=operator.timeout,
        )
