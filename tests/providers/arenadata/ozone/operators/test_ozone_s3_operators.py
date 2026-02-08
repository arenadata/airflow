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

from airflow.providers.arenadata.ozone.hooks.ozone_s3 import OzoneS3Hook
from airflow.providers.arenadata.ozone.operators.ozone_s3 import (
    OzoneS3CreateBucketOperator,
    OzoneS3PutObjectOperator,
)


class TestOzoneS3Operators:
    """Unit tests for S3 Gateway Operators."""

    @patch("airflow.providers.arenadata.ozone.operators.ozone_s3.OzoneS3Hook")
    def test_ozone_s3_create_bucket_operator(self, mock_ozone_s3_hook: MagicMock):
        """Test that OzoneS3CreateBucketOperator calls the hook correctly."""

        mock_hook_instance = mock_ozone_s3_hook.return_value

        operator = OzoneS3CreateBucketOperator(task_id="create_bucket_test", bucket_name="test_bucket")
        operator.execute(context={})

        mock_ozone_s3_hook.assert_called_once_with(ozone_conn_id=OzoneS3Hook.default_conn_name)
        mock_hook_instance.create_bucket_with_retry.assert_called_once_with(bucket_name="test_bucket")

    @patch("airflow.providers.arenadata.ozone.operators.ozone_s3.OzoneS3Hook")
    def test_ozone_s3_create_bucket_operator_custom_conn(self, mock_ozone_s3_hook: MagicMock):
        """Test that OzoneS3CreateBucketOperator uses custom connection ID."""

        mock_hook_instance = mock_ozone_s3_hook.return_value

        operator = OzoneS3CreateBucketOperator(
            task_id="create_bucket_test", bucket_name="test_bucket", ozone_conn_id="custom_conn"
        )
        operator.execute(context={})

        mock_ozone_s3_hook.assert_called_once_with(ozone_conn_id="custom_conn")
        mock_hook_instance.create_bucket_with_retry.assert_called_once_with(bucket_name="test_bucket")

    @patch("airflow.providers.arenadata.ozone.operators.ozone_s3.OzoneS3Hook")
    def test_ozone_s3_put_object_operator(self, mock_ozone_s3_hook: MagicMock):
        """Test that OzoneS3PutObjectOperator calls the hook correctly."""

        mock_hook_instance = mock_ozone_s3_hook.return_value
        test_data = "test content"

        operator = OzoneS3PutObjectOperator(
            task_id="put_object_test", bucket_name="test_bucket", key="test_key", data=test_data
        )
        operator.execute(context={})

        mock_ozone_s3_hook.assert_called_once_with(ozone_conn_id=OzoneS3Hook.default_conn_name)
        mock_hook_instance.load_string_with_retry.assert_called_once_with(
            string_data=test_data,
            key="test_key",
            bucket_name="test_bucket",
            replace=True,
        )

    def test_ozone_s3_put_object_operator_validation(self):
        """Test that OzoneS3PutObjectOperator validates parameters."""

        with pytest.raises(ValueError, match="bucket_name parameter cannot be empty"):
            OzoneS3PutObjectOperator(task_id="test", bucket_name="", key="test_key", data="test")

        with pytest.raises(ValueError, match="key parameter cannot be empty"):
            OzoneS3PutObjectOperator(task_id="test", bucket_name="test_bucket", key="", data="test")

        with pytest.raises(ValueError, match="data parameter cannot be None"):
            OzoneS3PutObjectOperator(task_id="test", bucket_name="test_bucket", key="test_key", data=None)
