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

import io
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.hooks.ozone_s3 import OzoneS3Hook
from airflow.providers.arenadata.ozone.operators.ozone import (
    OzoneS3CreateBucketOperator,
    OzoneS3PutObjectOperator,
)


class TestOzoneS3Operators:
    """Unit tests for S3 Gateway Operators."""

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneS3Hook")
    def test_ozone_s3_create_bucket_operator(self, mock_ozone_s3_hook: MagicMock):
        """Test that OzoneS3CreateBucketOperator calls the hook correctly."""

        mock_hook_instance = mock_ozone_s3_hook.return_value

        operator = OzoneS3CreateBucketOperator(task_id="create_bucket_test", bucket_name="test_bucket")
        operator.execute(context={})

        mock_ozone_s3_hook.assert_called_once_with(ozone_conn_id=OzoneS3Hook.default_conn_name)
        mock_hook_instance.create_bucket_with_retry.assert_called_once_with(bucket_name="test_bucket")

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneS3Hook")
    def test_ozone_s3_put_object_operator_bytes(self, mock_ozone_s3_hook: MagicMock):
        """Test that bytes payload is uploaded via file-like API."""
        mock_hook_instance = mock_ozone_s3_hook.return_value
        test_data = b"binary-content"

        operator = OzoneS3PutObjectOperator(
            task_id="put_object_bytes_test", bucket_name="test_bucket", key="test_key", data=test_data
        )
        operator.execute(context={})

        mock_hook_instance.load_string_with_retry.assert_not_called()
        mock_hook_instance.load_file_obj_with_retry.assert_called_once()
        call_kwargs = mock_hook_instance.load_file_obj_with_retry.call_args.kwargs
        assert call_kwargs["key"] == "test_key"
        assert call_kwargs["bucket_name"] == "test_bucket"
        assert call_kwargs["replace"] is True
        assert isinstance(call_kwargs["file_obj"], io.BytesIO)
        assert call_kwargs["file_obj"].getvalue() == test_data

    def test_ozone_s3_put_object_operator_validation(self):
        """Test that OzoneS3PutObjectOperator validates data payload when required."""
        with pytest.raises(ValueError, match="data parameter cannot be None"):
            OzoneS3PutObjectOperator(task_id="test", bucket_name="test_bucket", key="test_key", data=None)

    @patch("airflow.providers.arenadata.ozone.operators.ozone.OzoneS3Hook")
    def test_ozone_s3_put_object_operator_non_json_serializable(self, mock_ozone_s3_hook: MagicMock):
        """Test that non-serializable payload raises readable AirflowException."""
        operator = OzoneS3PutObjectOperator(
            task_id="put_object_bad_payload",
            bucket_name="test_bucket",
            key="test_key",
            data={"bad": {1, 2, 3}},  # set is not JSON-serializable
        )

        with pytest.raises(AirflowException, match="JSON-serializable object"):
            operator.execute(context={})

        mock_hook_instance = mock_ozone_s3_hook.return_value
        mock_hook_instance.load_string_with_retry.assert_not_called()
        mock_hook_instance.load_file_obj_with_retry.assert_not_called()
