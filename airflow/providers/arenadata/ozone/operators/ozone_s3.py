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

from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone_s3 import OzoneS3Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OzoneS3CreateBucketOperator(BaseOperator):
    """Create a bucket via S3 Gateway."""

    template_fields = ("bucket_name", "ozone_conn_id")

    def __init__(self, bucket_name: str, ozone_conn_id: str = OzoneS3Hook.default_conn_name, **kwargs):
        super().__init__(**kwargs)
        if not bucket_name or not bucket_name.strip():
            raise ValueError("bucket_name parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")

        self.bucket_name = bucket_name
        self.ozone_conn_id = ozone_conn_id

        self.log.debug(
            "Initializing OzoneS3CreateBucketOperator - bucket: %s, connection: %s",
            self.bucket_name,
            self.ozone_conn_id,
        )

    def execute(self, context: Context):
        self.log.info("Starting S3 bucket creation operation via Ozone S3 Gateway")
        self.log.debug("Bucket name: %s", self.bucket_name)
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneS3Hook(ozone_conn_id=self.ozone_conn_id)
        hook.create_bucket_with_retry(bucket_name=self.bucket_name)

        self.log.info("Successfully created bucket via S3 Gateway: %s", self.bucket_name)


class OzoneS3PutObjectOperator(BaseOperator):
    """Put object via S3 Gateway."""

    template_fields = ("bucket_name", "key", "data", "ozone_conn_id")

    def __init__(
        self,
        bucket_name: str,
        key: str,
        data: Any,
        ozone_conn_id: str = OzoneS3Hook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not bucket_name or not bucket_name.strip():
            raise ValueError("bucket_name parameter cannot be empty")
        if not key or not key.strip():
            raise ValueError("key parameter cannot be empty")
        if data is None:
            raise ValueError("data parameter cannot be None")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")

        # NOTE: validate-operators-init requires template fields to be assigned
        # directly from __init__ arguments after super().__init__(**kwargs).
        self.bucket_name = bucket_name
        self.key = key
        self.data = data
        self.ozone_conn_id = ozone_conn_id

        data_size = len(str(data)) if isinstance(data, (str, bytes)) else 0
        self.log.debug(
            "Initializing OzoneS3PutObjectOperator - bucket: %s, key: %s, data_size: %d bytes, connection: %s",
            self.bucket_name,
            self.key,
            data_size,
            self.ozone_conn_id,
        )

    def execute(self, context: Context):
        s3_path = f"s3://{self.bucket_name}/{self.key}"
        # Avoid converting arbitrarily large payloads to string implicitly; only measure when cheap.
        data_repr = self.data if isinstance(self.data, (str, bytes)) else "<non-string payload>"
        data_size = len(data_repr) if isinstance(data_repr, (str, bytes)) else 0

        self.log.info("Starting S3 object upload operation via Ozone S3 Gateway")
        self.log.debug("S3 path: %s", s3_path)
        self.log.debug("Data size: %d bytes", data_size)
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneS3Hook(ozone_conn_id=self.ozone_conn_id)
        hook.load_string_with_retry(
            string_data=str(self.data), key=self.key, bucket_name=self.bucket_name, replace=True
        )

        self.log.info("Successfully uploaded object to Ozone S3: %s (%d bytes)", s3_path, data_size)
