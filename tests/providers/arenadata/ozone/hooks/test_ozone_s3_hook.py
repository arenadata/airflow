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
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.hooks.ozone_s3 import OzoneS3Hook


@pytest.fixture
def ozone_s3_hook():
    """Provides a reusable instance of the OzoneS3Hook."""
    return OzoneS3Hook(ozone_conn_id="test_s3_conn")


class TestOzoneS3Hook:
    """Unit tests for OzoneS3Hook retry methods."""

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.OzoneS3Client.get_key")
    def test_get_key_with_retry_no_such_key_not_retried(
        self, mock_get_key: MagicMock, ozone_s3_hook: OzoneS3Hook
    ):
        """NoSuchKey is terminal and should not be retried."""
        mock_get_key.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}}, "GetObject"
        )

        with patch.object(ozone_s3_hook, "get_conn", return_value=MagicMock()):
            with pytest.raises(AirflowException):
                ozone_s3_hook.get_key_with_retry(key="test_key", bucket_name="test_bucket")

        assert mock_get_key.call_count == 1

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.OzoneS3Client.get_key")
    def test_get_key_with_retry_service_unavailable_retried(
        self, mock_get_key: MagicMock, ozone_s3_hook: OzoneS3Hook
    ):
        """ServiceUnavailable is retryable and should be retried."""
        mock_get_key.side_effect = ClientError(
            {"Error": {"Code": "ServiceUnavailable", "Message": "Service is temporarily unavailable"}},
            "GetObject",
        )

        with patch.object(ozone_s3_hook, "get_conn", return_value=MagicMock()):
            with pytest.raises(AirflowException):
                ozone_s3_hook.get_key_with_retry(key="test_key", bucket_name="test_bucket")

        assert mock_get_key.call_count == 3

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.OzoneS3Client.load_file_obj")
    def test_load_file_obj_with_retry_success(
        self, mock_load_file_obj: MagicMock, ozone_s3_hook: OzoneS3Hook
    ):
        """Test that load_file_obj_with_retry successfully uploads a file object."""
        mock_file_obj = MagicMock()
        with patch.object(ozone_s3_hook, "get_conn", return_value=MagicMock()):
            ozone_s3_hook.load_file_obj_with_retry(
                file_obj=mock_file_obj, key="test_key", bucket_name="test_bucket", replace=True
            )

        mock_load_file_obj.assert_called_once()
        call_kw = mock_load_file_obj.call_args[1]
        assert call_kw["key"] == "test_key"
        assert call_kw["bucket_name"] == "test_bucket"
        assert call_kw["replace"] is True

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.OzoneS3Client.create_bucket")
    def test_create_bucket_with_retry_already_exists(
        self, mock_create_bucket: MagicMock, ozone_s3_hook: OzoneS3Hook
    ):
        """create_bucket_with_retry treats BucketAlreadyExists as success (no exception)."""
        error = ClientError(
            {"Error": {"Code": "BucketAlreadyExists", "Message": "Bucket already exists"}}, "CreateBucket"
        )
        mock_create_bucket.side_effect = error

        with patch.object(ozone_s3_hook, "get_conn", return_value=MagicMock()):
            ozone_s3_hook.create_bucket_with_retry(bucket_name="test_bucket")

        mock_create_bucket.assert_called_once()

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.OzoneS3Client.list_keys")
    def test_list_keys_with_retry_success(self, mock_list_keys: MagicMock, ozone_s3_hook: OzoneS3Hook):
        """Test that list_keys_with_retry successfully lists keys."""
        expected_keys = ["key1", "key2", "key3"]
        mock_list_keys.return_value = expected_keys

        with patch.object(ozone_s3_hook, "get_conn", return_value=MagicMock()):
            result = ozone_s3_hook.list_keys_with_retry(bucket_name="test_bucket", prefix="test/")

        mock_list_keys.assert_called_once()
        call_kw = mock_list_keys.call_args[1]
        assert call_kw["bucket_name"] == "test_bucket"
        assert call_kw["prefix"] == "test/"
        assert result == expected_keys

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.OzoneS3Client.get_key")
    def test_get_key_with_retry_uses_mapped_error_message(
        self, mock_get_key: MagicMock, ozone_s3_hook: OzoneS3Hook
    ):
        """ClientError should be mapped to a human-readable Ozone S3 message."""
        mock_get_key.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}}, "GetObject"
        )

        with patch.object(ozone_s3_hook, "get_conn", return_value=MagicMock()):
            with pytest.raises(AirflowException) as excinfo:
                ozone_s3_hook.get_key_with_retry(key="missing", bucket_name="bucket")

        assert "Ozone S3 Gateway Error" in str(excinfo.value)

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.OzoneS3Hook.get_connection")
    def test_verify_is_loaded_from_connection_extra(self, mock_get_connection: MagicMock):
        """Test that verify is taken from connection Extra."""
        mock_conn = MagicMock()
        mock_conn.extra_dejson = {"endpoint_url": "https://s3g:9879", "verify": False}
        mock_conn.login = "test_access_key"
        mock_conn.password = "test_secret_key"
        mock_get_connection.return_value = mock_conn

        hook = OzoneS3Hook(ozone_conn_id="test_conn")
        assert hook._connection.extra_dejson["verify"] is False

    def test_test_connection_client_error(self, ozone_s3_hook: OzoneS3Hook):
        """test_connection maps ClientError to readable message."""
        error = ClientError({"Error": {"Code": "NoSuchBucket", "Message": "missing"}}, "ListBuckets")
        with patch.object(ozone_s3_hook, "get_conn", side_effect=error):
            ok, message = ozone_s3_hook.test_connection()
        assert ok is False
        assert "bucket does not exist" in message.lower()
