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

from unittest.mock import MagicMock, Mock, patch

import pytest
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.transfers.ozone_to_s3 import OzoneToS3Operator


class TestOzoneToS3Operator:
    """Unit tests for OzoneToS3Operator."""

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.S3Hook")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Hook")
    def test_execute_single_key(self, mock_ozone_hook: MagicMock, mock_s3_hook: MagicMock):
        """Test that OzoneToS3Operator transfers a single key."""

        mock_ozone_instance = mock_ozone_hook.return_value
        mock_s3_instance = mock_s3_hook.return_value

        mock_ozone_instance.list_keys_with_retry.return_value = ["data/file1.txt"]

        mock_key_obj = MagicMock()
        mock_body = Mock()
        mock_body.read.return_value = b"test content"
        mock_key_obj.get.return_value = {"Body": mock_body}
        mock_ozone_instance.get_key_with_retry.return_value = mock_key_obj

        mock_s3_instance.load_file_obj = MagicMock()

        operator = OzoneToS3Operator(
            task_id="test_transfer",
            ozone_bucket="ozone_bucket",
            s3_bucket="s3_bucket",
            ozone_prefix="data/",
            s3_prefix="backup/",
            max_workers=1,
        )
        operator.execute(context={})

        mock_ozone_hook.assert_called_once_with(ozone_conn_id="ozone_s3_default")
        mock_s3_hook.assert_called_once_with(aws_conn_id="aws_default")

        mock_ozone_instance.list_keys_with_retry.assert_called_once_with("ozone_bucket", prefix="data/")

        mock_ozone_instance.get_key_with_retry.assert_called_once_with("data/file1.txt", "ozone_bucket")

        mock_s3_instance.load_file_obj.assert_called_once()
        call_args = mock_s3_instance.load_file_obj.call_args[0]
        call_kwargs = mock_s3_instance.load_file_obj.call_args[1]
        assert call_args[0] == mock_body
        assert call_args[1] == "backup/file1.txt"
        assert call_args[2] == "s3_bucket"
        assert call_kwargs.get("replace") is True

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.S3Hook")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Hook")
    def test_execute_no_keys(self, mock_ozone_hook: MagicMock, mock_s3_hook: MagicMock):
        """Test that OzoneToS3Operator handles empty key list."""

        mock_ozone_instance = mock_ozone_hook.return_value
        mock_ozone_instance.list_keys_with_retry.return_value = []

        operator = OzoneToS3Operator(
            task_id="test_transfer", ozone_bucket="ozone_bucket", s3_bucket="s3_bucket"
        )
        operator.execute(context={})

        mock_ozone_instance.list_keys_with_retry.assert_called_once()

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.S3Hook")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Hook")
    def test_execute_all_keys_fail(self, mock_ozone_hook: MagicMock, mock_s3_hook: MagicMock):
        """Test that OzoneToS3Operator raises exception when all keys fail."""

        mock_ozone_instance = mock_ozone_hook.return_value
        mock_ozone_instance.list_keys_with_retry.return_value = ["data/file1.txt"]
        mock_ozone_instance.get_key_with_retry.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}}, "GetObject"
        )

        operator = OzoneToS3Operator(
            task_id="test_transfer", ozone_bucket="ozone_bucket", s3_bucket="s3_bucket"
        )

        with pytest.raises(AirflowException, match=r"All .* key\(s\) failed to transfer"):
            operator.execute(context={})

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.S3Hook")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Hook")
    def test_execute_multiple_keys_parallel(self, mock_ozone_hook: MagicMock, mock_s3_hook: MagicMock):
        """Test that OzoneToS3Operator handles multiple keys with parallel execution."""

        mock_ozone_instance = mock_ozone_hook.return_value
        mock_s3_instance = mock_s3_hook.return_value

        keys = [f"data/file{i}.txt" for i in range(5)]
        mock_ozone_instance.list_keys_with_retry.return_value = keys

        mock_key_obj = MagicMock()
        mock_body = Mock()
        mock_body.read.return_value = b"test content"
        mock_key_obj.get.return_value = {"Body": mock_body}
        mock_ozone_instance.get_key_with_retry.return_value = mock_key_obj

        mock_s3_instance.load_file_obj = MagicMock()

        operator = OzoneToS3Operator(
            task_id="test_transfer", ozone_bucket="ozone_bucket", s3_bucket="s3_bucket", max_workers=3
        )
        operator.execute(context={})

        assert mock_ozone_instance.get_key_with_retry.call_count == 5
        assert mock_s3_instance.load_file_obj.call_count == 5
