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

    with patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.S3Hook.__init__", return_value=None):
        return OzoneS3Hook(ozone_conn_id="test_s3_conn")


class TestOzoneS3Hook:
    """Unit tests for OzoneS3Hook retry methods."""

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.S3Hook.get_key")
    def test_get_key_with_retry_success(self, mock_get_key: MagicMock, ozone_s3_hook: OzoneS3Hook):
        """Test that get_key_with_retry successfully retrieves a key."""

        mock_key = MagicMock()
        mock_get_key.return_value = mock_key

        result = ozone_s3_hook.get_key_with_retry(key="test_key", bucket_name="test_bucket")

        mock_get_key.assert_called_once_with("test_key", "test_bucket")
        assert result == mock_key

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.S3Hook.get_key")
    def test_get_key_with_retry_client_error(self, mock_get_key: MagicMock, ozone_s3_hook: OzoneS3Hook):
        """Test that get_key_with_retry retries on ClientError."""

        mock_get_key.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Key not found"}}, "GetObject"
        )

        with pytest.raises(AirflowException):
            ozone_s3_hook.get_key_with_retry(key="test_key", bucket_name="test_bucket")

        # Should retry 3 times (initial + 2 retries)
        assert mock_get_key.call_count == 3

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.S3Hook.load_file_obj")
    def test_load_file_obj_with_retry_success(
        self, mock_load_file_obj: MagicMock, ozone_s3_hook: OzoneS3Hook
    ):
        """Test that load_file_obj_with_retry successfully uploads a file object."""

        mock_file_obj = MagicMock()
        ozone_s3_hook.load_file_obj_with_retry(
            file_obj=mock_file_obj, key="test_key", bucket_name="test_bucket", replace=True
        )

        mock_load_file_obj.assert_called_once_with(mock_file_obj, "test_key", "test_bucket", replace=True)

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.S3Hook.load_string")
    def test_load_string_with_retry_success(self, mock_load_string: MagicMock, ozone_s3_hook: OzoneS3Hook):
        """Test that load_string_with_retry successfully uploads string data."""

        test_data = "test content"
        ozone_s3_hook.load_string_with_retry(
            string_data=test_data, key="test_key", bucket_name="test_bucket", replace=False
        )

        mock_load_string.assert_called_once_with(test_data, "test_key", "test_bucket", replace=False)

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.S3Hook.create_bucket")
    def test_create_bucket_with_retry_success(
        self, mock_create_bucket: MagicMock, ozone_s3_hook: OzoneS3Hook
    ):
        """Test that create_bucket_with_retry successfully creates a bucket."""

        ozone_s3_hook.create_bucket_with_retry(bucket_name="test_bucket")

        mock_create_bucket.assert_called_once_with(bucket_name="test_bucket")

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.S3Hook.create_bucket")
    def test_create_bucket_with_retry_already_exists(
        self, mock_create_bucket: MagicMock, ozone_s3_hook: OzoneS3Hook
    ):
        """Test that create_bucket_with_retry handles BucketAlreadyExists gracefully."""

        error = ClientError(
            {"Error": {"Code": "BucketAlreadyExists", "Message": "Bucket already exists"}}, "CreateBucket"
        )
        mock_create_bucket.side_effect = error

        # Should not raise an exception, just return
        ozone_s3_hook.create_bucket_with_retry(bucket_name="test_bucket")

        mock_create_bucket.assert_called_once_with(bucket_name="test_bucket")

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.S3Hook.list_keys")
    def test_list_keys_with_retry_success(self, mock_list_keys: MagicMock, ozone_s3_hook: OzoneS3Hook):
        """Test that list_keys_with_retry successfully lists keys."""

        expected_keys = ["key1", "key2", "key3"]
        mock_list_keys.return_value = expected_keys

        result = ozone_s3_hook.list_keys_with_retry(bucket_name="test_bucket", prefix="test/")

        mock_list_keys.assert_called_once_with(bucket_name="test_bucket", prefix="test/")
        assert result == expected_keys

    @patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.OzoneS3Hook.get_connection")
    def test_verify_is_loaded_from_connection_extra(self, mock_get_connection: MagicMock):
        """Test that verify is taken from connection Extra."""

        mock_conn = MagicMock()
        mock_conn.extra_dejson = {"endpoint_url": "https://s3g:9879", "verify": False}
        mock_conn.login = "test_access_key"
        mock_conn.password = "test_secret_key"
        mock_get_connection.return_value = mock_conn

        with patch("airflow.providers.arenadata.ozone.hooks.ozone_s3.S3Hook.__init__", return_value=None):
            hook = OzoneS3Hook(ozone_conn_id="test_conn")
            assert getattr(hook, "_verify", None) is False
