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

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.load_file_obj")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.get_s3_client")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Hook")
    def test_execute_single_key(
        self, mock_ozone_hook: MagicMock, mock_get_s3_client: MagicMock, mock_load_file_obj: MagicMock
    ):
        """Test that OzoneToS3Operator transfers a single key."""
        mock_ozone_instance = mock_ozone_hook.return_value
        mock_dest_client = MagicMock()
        mock_get_s3_client.return_value = mock_dest_client

        mock_ozone_instance.list_keys_with_retry.return_value = ["data/file1.txt"]

        mock_body = Mock()
        mock_body.read.return_value = b"test content"
        mock_ozone_instance.get_key_with_retry.return_value = {"Body": mock_body}

        operator = OzoneToS3Operator(
            task_id="test_transfer",
            ozone_bucket="ozone_bucket",
            s3_bucket="s3_bucket",
            ozone_prefix="data/",
            s3_prefix="backup/",
            max_workers=1,
        )
        result = operator.execute(context={})

        mock_ozone_hook.assert_called_once_with(ozone_conn_id="ozone_s3_default")
        mock_ozone_instance.list_keys_with_retry.assert_called_once_with("ozone_bucket", prefix="data/")
        mock_ozone_instance.get_key_with_retry.assert_called_once_with("data/file1.txt", "ozone_bucket")

        mock_load_file_obj.assert_called_once()
        call_args = mock_load_file_obj.call_args[0]
        call_kwargs = mock_load_file_obj.call_args[1]
        assert call_args[0] == mock_dest_client
        assert call_args[1] == mock_body
        assert call_args[2] == "backup/file1.txt"
        assert call_args[3] == "s3_bucket"
        assert call_kwargs.get("replace") is True
        assert result == {"transferred": 1, "failed": 0, "total": 1}
        mock_body.close.assert_called_once()

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.get_s3_client")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Hook")
    def test_execute_no_keys(self, mock_ozone_hook: MagicMock, mock_get_s3_client: MagicMock):
        """Test that OzoneToS3Operator handles empty key list."""
        mock_ozone_instance = mock_ozone_hook.return_value
        mock_ozone_instance.list_keys_with_retry.return_value = []

        operator = OzoneToS3Operator(
            task_id="test_transfer", ozone_bucket="ozone_bucket", s3_bucket="s3_bucket"
        )
        result = operator.execute(context={})

        mock_ozone_instance.list_keys_with_retry.assert_called_once()
        assert result == {"transferred": 0, "failed": 0, "total": 0}

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.load_file_obj")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.get_s3_client")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Hook")
    def test_execute_partial_failure_raises(
        self, mock_ozone_hook: MagicMock, mock_get_s3_client: MagicMock, mock_load_file_obj: MagicMock
    ):
        """Task must fail when at least one key fails."""
        mock_ozone_instance = mock_ozone_hook.return_value
        mock_get_s3_client.return_value = MagicMock()
        mock_ozone_instance.list_keys_with_retry.return_value = ["data/file1.txt", "data/file2.txt"]

        ok_body = Mock()
        ok_body.read.return_value = b"ok-content"
        mock_ozone_instance.get_key_with_retry.side_effect = [{"Body": ok_body}, AirflowException("boom")]

        operator = OzoneToS3Operator(
            task_id="test_transfer",
            ozone_bucket="ozone_bucket",
            s3_bucket="s3_bucket",
            max_workers=1,
        )

        with pytest.raises(AirflowException, match=r"Failed to transfer 1 of 2 key\(s\)"):
            operator.execute(context={})

        assert mock_load_file_obj.call_count == 1

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.load_file_obj")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.get_s3_client")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Hook")
    def test_execute_prefix_mismatch_raises(
        self, mock_ozone_hook: MagicMock, mock_get_s3_client: MagicMock, mock_load_file_obj: MagicMock
    ):
        """Key outside ozone_prefix must fail transfer."""
        mock_ozone_instance = mock_ozone_hook.return_value
        mock_get_s3_client.return_value = MagicMock()
        mock_ozone_instance.list_keys_with_retry.return_value = ["other/file1.txt"]

        operator = OzoneToS3Operator(
            task_id="test_transfer",
            ozone_bucket="ozone_bucket",
            s3_bucket="s3_bucket",
            ozone_prefix="data/",
            s3_prefix="backup/",
            max_workers=1,
        )

        with pytest.raises(AirflowException, match=r"Failed to transfer 1 of 1 key\(s\)"):
            operator.execute(context={})

        mock_load_file_obj.assert_not_called()

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.load_file_obj")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.get_s3_client")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Hook")
    def test_execute_parallel_multiple_keys_success(
        self, mock_ozone_hook: MagicMock, mock_get_s3_client: MagicMock, mock_load_file_obj: MagicMock
    ):
        """Parallel mode should transfer all keys and return stable counters."""
        mock_ozone_instance = mock_ozone_hook.return_value
        mock_get_s3_client.return_value = MagicMock()
        mock_ozone_instance.list_keys_with_retry.return_value = [
            "data/file1.txt",
            "data/file2.txt",
            "data/file3.txt",
        ]

        def _key_obj():
            body = Mock()
            return {"Body": body}

        mock_ozone_instance.get_key_with_retry.side_effect = [_key_obj(), _key_obj(), _key_obj()]

        operator = OzoneToS3Operator(
            task_id="test_transfer_parallel",
            ozone_bucket="ozone_bucket",
            s3_bucket="s3_bucket",
            ozone_prefix="data/",
            s3_prefix="backup/",
            max_workers=3,
        )
        result = operator.execute(context={})
        assert result == {"transferred": 3, "failed": 0, "total": 3}
        assert mock_load_file_obj.call_count == 3

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.load_file_obj")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Client.get_s3_client")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_s3.OzoneS3Hook")
    def test_execute_destination_retryable_upload_error_retried(
        self, mock_ozone_hook: MagicMock, mock_get_s3_client: MagicMock, mock_load_file_obj: MagicMock
    ):
        """Destination upload retryable ClientError should be retried and succeed."""
        mock_ozone_instance = mock_ozone_hook.return_value
        mock_get_s3_client.return_value = MagicMock()
        mock_ozone_instance.list_keys_with_retry.return_value = ["data/file1.txt"]

        body = Mock()
        mock_ozone_instance.get_key_with_retry.return_value = {"Body": body}

        retryable_error = ClientError(
            {
                "Error": {"Code": "ServiceUnavailable", "Message": "temporary"},
                "ResponseMetadata": {"HTTPStatusCode": 503},
            },
            "PutObject",
        )
        mock_load_file_obj.side_effect = [retryable_error, None]

        operator = OzoneToS3Operator(
            task_id="test_transfer_retry_dest",
            ozone_bucket="ozone_bucket",
            s3_bucket="s3_bucket",
            ozone_prefix="data/",
            s3_prefix="backup/",
            max_workers=1,
        )
        result = operator.execute(context={})

        assert result == {"transferred": 1, "failed": 0, "total": 1}
        assert mock_load_file_obj.call_count == 2

    def test_prefixes_are_normalized_for_stable_mapping(self):
        """Prefix normalization should make key mapping independent of leading/trailing slashes."""
        operator = OzoneToS3Operator(
            task_id="test_prefix_normalization",
            ozone_bucket="ozone_bucket",
            s3_bucket="s3_bucket",
            ozone_prefix=" /data ",
            s3_prefix="/backup ",
            max_workers=1,
        )

        assert operator.ozone_prefix == " /data "
        assert operator.s3_prefix == "/backup "
        assert operator._build_dest_key("data/file1.txt") == "backup/file1.txt"
