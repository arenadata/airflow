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

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.arenadata.ozone.hooks.ozone_s3 import OzoneS3Hook


class OzoneS3KeySensor(S3KeySensor):
    """Waits for a key (a file-like object) to be present in an Ozone S3 bucket."""

    def __init__(self, ozone_conn_id: str = OzoneS3Hook.default_conn_name, **kwargs):
        # Map ozone_conn_id to aws_conn_id required by S3KeySensor
        if "aws_conn_id" not in kwargs:
            kwargs["aws_conn_id"] = ozone_conn_id

        bucket_name = kwargs.get("bucket_name", "unknown")
        bucket_key = kwargs.get("bucket_key", "unknown")

        super().__init__(**kwargs)

        # Log after super().__init__() so self.log is available
        self.log.info(
            "Initializing OzoneS3KeySensor - bucket: %s, key: %s, connection: %s",
            bucket_name,
            bucket_key,
            ozone_conn_id,
        )

    def poke(self, context):
        """Override poke to add detailed logging."""
        bucket_name = self.bucket_name
        bucket_key = self.bucket_key
        s3_path = f"s3://{bucket_name}/{bucket_key}"

        self.log.debug("Checking if S3 key exists in Ozone: %s", s3_path)
        result = super().poke(context)

        if result:
            self.log.info("S3 key found in Ozone: %s", s3_path)
        else:
            self.log.debug("S3 key not found yet in Ozone: %s", s3_path)

        return result
