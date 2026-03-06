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

import logging
from collections.abc import Callable
from functools import cached_property

from botocore.exceptions import BotoCoreError, ClientError

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.arenadata.ozone.utils.cli_runner import CliRunner
from airflow.providers.arenadata.ozone.utils.errors import (
    OzoneS3Error,
    OzoneS3Errors,
)
from airflow.providers.arenadata.ozone.utils.s3_client import OzoneS3Client

log = logging.getLogger(__name__)


class OzoneS3Hook(BaseHook):
    """Interact with Ozone via S3 Gateway using boto3."""

    hook_name = "Ozone S3"
    default_conn_name = "ozone_s3_default"
    conn_name_attr = "ozone_conn_id"
    conn_type = "ozone_s3"

    def __init__(self, ozone_conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ozone_conn_id = ozone_conn_id

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, object]:
        """Describe Ozone S3 connection extras in Airflow UI."""
        return {
            "hidden_fields": ["schema"],
            "relabeling": {"host": "S3 Endpoint Host", "port": "S3 Endpoint Port"},
            "placeholders": {
                "login": "access-key",
                "password": "secret-key or secret://kv/ozone/secret-key",
                "extra": (
                    '{"endpoint_url": "https://ozone-s3g:9879", "verify": true, '
                    '"addressing_style": "path", "max_attempts": 3, "retries_mode": "standard"}'
                ),
            },
        }

    def test_connection(self) -> tuple[bool, str]:
        """Run a minimal S3 call to verify endpoint reachability and credentials."""
        try:
            client = self.get_conn()
            client.list_buckets()
            return True, "Ozone S3 connection test succeeded."
        except ClientError as err:
            return False, OzoneS3Errors.map_s3_error_to_ozone(err)
        except BotoCoreError as err:
            return False, f"Ozone S3 connection test failed: {err}"

    @cached_property
    def _connection(self):
        conn = self.get_connection(self.ozone_conn_id)
        self._log_connection_security_details(conn)
        return conn

    @cached_property
    def _client(self):
        return OzoneS3Client.get_s3_client(self._connection)

    def _raise_retryable_s3_error(self, err: Exception, action: str) -> None:
        """Raise retryable or terminal errors based on S3 error code classification."""
        if isinstance(err, ClientError):
            error_code = str(err.response.get("Error", {}).get("Code", "")).strip()
            http_status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            human_msg = OzoneS3Errors.map_s3_error_to_ozone(err)
            if error_code in OzoneS3Errors.non_retryable_errors:
                self.log.error("Failed to %s (non-retryable): %s", action, human_msg)
                raise AirflowException(human_msg)
            if OzoneS3Errors.is_retryable_failure(http_status, error_code):
                self.log.warning("Failed to %s (will retry): %s", action, human_msg)
                raise OzoneS3Error(human_msg, retryable=True)
            # Unknown error code: keep conservative retry behavior.
            self.log.warning("Failed to %s (unknown code=%s, will retry): %s", action, error_code, human_msg)
            raise OzoneS3Error(human_msg, retryable=True)
        if isinstance(err, BotoCoreError):
            self.log.warning("Failed to %s (will retry, boto error): %s", action, str(err))
            raise OzoneS3Error(f"Failed to {action} due to boto error: {str(err)}", retryable=True)
        raise err

    def _run_retryable_s3_operation(
        self,
        *,
        action: str,
        operation: Callable[[], object],
        success_message: str | None = None,
    ) -> object | None:
        """Run one S3 operation with unified retryable error handling."""
        try:
            result = operation()
            if success_message:
                self.log.debug("%s", success_message)
            return result
        except (ClientError, BotoCoreError) as err:
            self._raise_retryable_s3_error(err, action)
        return None

    def _log_connection_security_details(self, conn) -> None:
        """Log secrets backend and SSL details lazily after connection resolution."""
        parsed_config = OzoneS3Client.parse_s3_connection_config(conn)
        endpoint_url = parsed_config.endpoint_url or ""
        verify = parsed_config.verify

        if endpoint_url.startswith("https://"):
            self.log.debug("SSL/TLS enabled: using HTTPS endpoint: %s", endpoint_url)
            if verify is False:
                self.log.warning(
                    "SSL certificate verification is disabled (verify=False). "
                    "This should only be used for development/testing."
                )
            elif isinstance(verify, str):
                self.log.debug("Using custom CA certificate for SSL verification: %s", verify)
            else:
                self.log.debug("Using default SSL certificate verification")
        elif endpoint_url.startswith("http://"):
            self.log.warning("Using unencrypted HTTP connection. Consider using HTTPS for production.")

    def get_conn(self):
        """Return cached boto3 S3 client (built from connection)."""
        return self._client

    def get_key(self, key: str, bucket_name: str):
        """Return raw S3 ``get_object`` response."""
        client = self.get_conn()
        return OzoneS3Client.get_key(client, bucket_name=bucket_name, key=key)

    def head_object(self, key: str, bucket_name: str) -> dict | None:
        """Retrieve metadata of an object. Returns None if key does not exist."""
        client = self.get_conn()
        return OzoneS3Client.head_object(client, bucket_name, key)

    def check_for_key(self, key: str, bucket_name: str) -> bool:
        """Return True if key exists, False otherwise."""
        return self.head_object(key, bucket_name) is not None

    def list_keys(self, bucket_name: str, prefix: str = ""):
        """List object keys under prefix."""
        client = self.get_conn()
        return OzoneS3Client.list_keys(client, bucket_name=bucket_name, prefix=prefix)

    def get_file_metadata(self, prefix: str, bucket_name: str | None = None):
        """List metadata for objects under prefix (for sensor wildcard)."""
        if bucket_name is None:
            raise ValueError("bucket_name is required")
        client = self.get_conn()
        return OzoneS3Client.get_file_metadata(client, bucket_name=bucket_name, prefix=prefix)

    def create_bucket(self, bucket_name: str) -> None:
        """Create S3 bucket."""
        client = self.get_conn()
        OzoneS3Client.create_bucket(client, bucket_name=bucket_name)

    def load_file_obj(self, file_obj, key: str, bucket_name: str, replace: bool = False) -> None:
        """Upload file-like object to S3."""
        client = self.get_conn()
        OzoneS3Client.load_file_obj(
            client,
            file_obj=file_obj,
            key=key,
            bucket_name=bucket_name,
            replace=replace,
        )

    def load_string(self, string_data: str, key: str, bucket_name: str, replace: bool = False) -> None:
        """Upload string to S3."""
        client = self.get_conn()
        OzoneS3Client.load_string(
            client,
            string_data=string_data,
            key=key,
            bucket_name=bucket_name,
            replace=replace,
        )

    @CliRunner.retry_for(
        retry_condition=lambda exc: isinstance(exc, OzoneS3Error) and exc.retryable,
        logger=log,
    )
    def get_key_with_retry(self, key: str, bucket_name: str):
        """Get S3 key with retry logic for retryable network errors."""
        return self._run_retryable_s3_operation(
            action=f"get S3 key s3://{bucket_name}/{key}",
            operation=lambda: self.get_key(key, bucket_name),
            success_message=f"Successfully retrieved S3 key: s3://{bucket_name}/{key}",
        )

    @CliRunner.retry_for(
        retry_condition=lambda exc: isinstance(exc, OzoneS3Error) and exc.retryable,
        logger=log,
    )
    def load_file_obj_with_retry(self, file_obj, key: str, bucket_name: str, replace: bool = False):
        """Load file object to S3 with retry logic."""
        self._run_retryable_s3_operation(
            action=f"upload file object to s3://{bucket_name}/{key}",
            operation=lambda: self.load_file_obj(file_obj, key, bucket_name, replace=replace),
            success_message=f"Successfully loaded file object to S3: s3://{bucket_name}/{key}",
        )

    @CliRunner.retry_for(
        retry_condition=lambda exc: isinstance(exc, OzoneS3Error) and exc.retryable,
        logger=log,
    )
    def load_string_with_retry(self, string_data: str, key: str, bucket_name: str, replace: bool = False):
        """Load string to S3 with retry logic."""
        self._run_retryable_s3_operation(
            action=f"upload string to s3://{bucket_name}/{key}",
            operation=lambda: self.load_string(string_data, key, bucket_name, replace=replace),
            success_message=f"Successfully loaded string to S3: s3://{bucket_name}/{key}",
        )

    @CliRunner.retry_for(
        retry_condition=lambda exc: isinstance(exc, OzoneS3Error) and exc.retryable,
        logger=log,
    )
    def create_bucket_with_retry(self, bucket_name: str):
        """Create S3 bucket with retry; BucketAlreadyExists is not retried."""
        try:
            self.create_bucket(bucket_name=bucket_name)
            self.log.debug("Successfully created S3 bucket: %s", bucket_name)
        except ClientError as err:
            error_code = err.response.get("Error", {}).get("Code", "") if hasattr(err, "response") else ""
            if error_code in (
                OzoneS3Errors.BUCKET_ALREADY_EXISTS,
                OzoneS3Errors.BUCKET_ALREADY_OWNED_BY_YOU,
            ) or (error_code == "" and OzoneS3Errors.BUCKET_ALREADY_OWNED_BY_YOU in str(err)):
                self.log.info("Bucket already exists: %s", bucket_name)
                return
            self._raise_retryable_s3_error(err, f"create S3 bucket {bucket_name}")
        except BotoCoreError as err:
            self._raise_retryable_s3_error(err, f"create S3 bucket {bucket_name}")

    @CliRunner.retry_for(
        retry_condition=lambda exc: isinstance(exc, OzoneS3Error) and exc.retryable,
        logger=log,
    )
    def list_keys_with_retry(self, bucket_name: str, prefix: str = ""):
        """List S3 keys with retry logic."""
        keys = self._run_retryable_s3_operation(
            action=f"list S3 keys in bucket={bucket_name}, prefix={prefix}",
            operation=lambda: self.list_keys(bucket_name=bucket_name, prefix=prefix),
        )
        if keys is None:
            return []
        self.log.debug("Successfully listed %d key(s) from S3 bucket: %s", len(keys), bucket_name)
        return keys
