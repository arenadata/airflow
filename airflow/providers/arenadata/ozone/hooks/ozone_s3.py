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

from botocore.exceptions import BotoCoreError, ClientError
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.arenadata.ozone.utils import s3_client
from airflow.providers.arenadata.ozone.utils.s3_compat import S3ErrorCode, map_s3_error_to_ozone

# Get the logger for tenacity to use
log = logging.getLogger(__name__)


_s3_retryable = retry(
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((ClientError, BotoCoreError, AirflowException)),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)


class OzoneS3Hook(BaseHook):
    """Interact with Ozone via S3 Gateway using boto3."""

    hook_name = "Ozone S3"
    default_conn_name = "ozone_s3_default"
    conn_name_attr = "ozone_conn_id"
    conn_type = "ozone_s3"

    def __init__(self, ozone_conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ozone_conn_id = ozone_conn_id.strip()
        self._client = None
        self._verify = None
        self._security_details_logged = False

        self.log.debug("Initializing OzoneS3Hook with connection ID: %s", self._ozone_conn_id)
        try:
            conn = self.get_connection(self._ozone_conn_id)
            extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}
            self._verify = extra.get("verify")
        except Exception:
            self._verify = None

    def _log_connection_security_details(self, conn) -> None:
        """Log secrets backend and SSL details lazily after connection resolution."""
        if self._security_details_logged:
            return

        if conn.login and str(conn.login).startswith("secret://"):
            self.log.info("Using Secrets Backend for S3 access key (conn_id=%s)", self._ozone_conn_id)
        if conn.password and str(conn.password).startswith("secret://"):
            self.log.info("Using Secrets Backend for S3 secret key (conn_id=%s)", self._ozone_conn_id)

        extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}
        endpoint_url = str(extra.get("endpoint_url", "") or "")
        verify = extra.get("verify")
        self._verify = verify

        if endpoint_url.startswith("https://"):
            self.log.info("SSL/TLS enabled: Using HTTPS endpoint: %s", endpoint_url)
            if verify is False:
                self.log.warning(
                    "SSL certificate verification is disabled (verify=False). "
                    "This should only be used for development/testing."
                )
            elif isinstance(verify, str):
                self.log.info("Using custom CA certificate for SSL verification: %s", verify)
            else:
                self.log.debug("Using default SSL certificate verification")
        elif endpoint_url.startswith("http://"):
            self.log.warning("Using unencrypted HTTP connection. Consider using HTTPS for production.")

        self._security_details_logged = True

    def get_conn(self):
        """Return cached boto3 S3 client (built from connection)."""
        if self._client is None:
            conn = self.get_connection(self._ozone_conn_id)
            self._log_connection_security_details(conn)
            self._client = s3_client.get_s3_client(conn)
        return self._client

    def get_key(self, key: str, bucket_name: str):
        """Return object that supports .get()['Body'] for streaming."""
        client = self.get_conn()
        return s3_client.get_key(client, bucket_name=bucket_name, key=key)

    def head_object(self, key: str, bucket_name: str) -> dict | None:
        """Retrieve metadata of an object. Returns None if key does not exist."""
        client = self.get_conn()
        return s3_client.head_object(client, bucket_name, key)

    def check_for_key(self, key: str, bucket_name: str) -> bool:
        """Return True if key exists, False otherwise."""
        return self.head_object(key, bucket_name) is not None

    def list_keys(self, bucket_name: str, prefix: str = ""):
        """List object keys under prefix."""
        client = self.get_conn()
        return s3_client.list_keys(client, bucket_name=bucket_name, prefix=prefix)

    def get_file_metadata(self, prefix: str, bucket_name: str | None = None):
        """List metadata for objects under prefix (for sensor wildcard)."""
        if bucket_name is None:
            raise ValueError("bucket_name is required")
        client = self.get_conn()
        return s3_client.get_file_metadata(client, bucket_name=bucket_name, prefix=prefix)

    def create_bucket(self, bucket_name: str) -> None:
        """Create S3 bucket."""
        client = self.get_conn()
        s3_client.create_bucket(client, bucket_name=bucket_name)

    def load_file_obj(self, file_obj, key: str, bucket_name: str, replace: bool = False) -> None:
        """Upload file-like object to S3."""
        client = self.get_conn()
        s3_client.load_file_obj(
            client,
            file_obj=file_obj,
            key=key,
            bucket_name=bucket_name,
            replace=replace,
        )

    def load_string(self, string_data: str, key: str, bucket_name: str, replace: bool = False) -> None:
        """Upload string to S3."""
        client = self.get_conn()
        s3_client.load_string(
            client,
            string_data=string_data,
            key=key,
            bucket_name=bucket_name,
            replace=replace,
        )

    @_s3_retryable
    def get_key_with_retry(self, key: str, bucket_name: str):
        """Get S3 key with retry logic for transient network errors."""
        self.log.debug("Getting S3 key with retry: s3://%s/%s", bucket_name, key)
        try:
            key_obj = self.get_key(key, bucket_name)
            self.log.debug("Successfully retrieved S3 key: s3://%s/%s", bucket_name, key)
            return key_obj
        except ClientError as e:
            human_msg = map_s3_error_to_ozone(e)
            self.log.warning(
                "Failed to get S3 key (will retry): s3://%s/%s - %s", bucket_name, key, human_msg
            )
            raise AirflowException(human_msg)
        except BotoCoreError as e:
            self.log.warning(
                "Failed to get S3 key (will retry, low-level boto error): s3://%s/%s - %s",
                bucket_name,
                key,
                str(e),
            )
            raise AirflowException(f"Failed to get S3 key due to boto error: {str(e)}")

    @_s3_retryable
    def load_file_obj_with_retry(self, file_obj, key: str, bucket_name: str, replace: bool = False):
        """Load file object to S3 with retry logic."""
        self.log.debug("Loading file object to S3 with retry: s3://%s/%s", bucket_name, key)
        try:
            self.load_file_obj(file_obj, key, bucket_name, replace=replace)
            self.log.debug("Successfully loaded file object to S3: s3://%s/%s", bucket_name, key)
        except ClientError as e:
            human_msg = map_s3_error_to_ozone(e)
            self.log.warning(
                "Failed to load file object to S3 (will retry): s3://%s/%s - %s",
                bucket_name,
                key,
                human_msg,
            )
            raise AirflowException(human_msg)
        except BotoCoreError as e:
            self.log.warning(
                "Failed to load file object to S3 (will retry, boto error): s3://%s/%s - %s",
                bucket_name,
                key,
                str(e),
            )
            raise AirflowException(f"Failed to load file object to S3: {str(e)}")

    @_s3_retryable
    def load_string_with_retry(self, string_data: str, key: str, bucket_name: str, replace: bool = False):
        """Load string to S3 with retry logic."""
        self.log.debug("Loading string to S3 with retry: s3://%s/%s", bucket_name, key)
        try:
            self.load_string(string_data, key, bucket_name, replace=replace)
            self.log.debug("Successfully loaded string to S3: s3://%s/%s", bucket_name, key)
        except ClientError as e:
            human_msg = map_s3_error_to_ozone(e)
            self.log.warning(
                "Failed to load string to S3 (will retry): s3://%s/%s - %s", bucket_name, key, human_msg
            )
            raise AirflowException(human_msg)
        except BotoCoreError as e:
            self.log.warning(
                "Failed to load string to S3 (will retry, boto error): s3://%s/%s - %s",
                bucket_name,
                key,
                str(e),
            )
            raise AirflowException(f"Failed to load string to S3: {str(e)}")

    @_s3_retryable
    def create_bucket_with_retry(self, bucket_name: str):
        """Create S3 bucket with retry; BucketAlreadyExists is not retried."""
        self.log.debug("Creating S3 bucket with retry: %s", bucket_name)
        try:
            self.create_bucket(bucket_name=bucket_name)
            self.log.debug("Successfully created S3 bucket: %s", bucket_name)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "") if hasattr(e, "response") else ""
            if error_code in (S3ErrorCode.BUCKET_ALREADY_EXISTS, S3ErrorCode.BUCKET_ALREADY_OWNED_BY_YOU) or (
                error_code == "" and S3ErrorCode.BUCKET_ALREADY_OWNED_BY_YOU.value in str(e)
            ):
                self.log.info("Bucket already exists: %s", bucket_name)
                return
            human_msg = map_s3_error_to_ozone(e)
            self.log.warning("Failed to create S3 bucket (will retry): %s - %s", bucket_name, human_msg)
            raise AirflowException(human_msg)
        except BotoCoreError as e:
            self.log.warning(
                "Failed to create S3 bucket (will retry, boto error): %s - %s", bucket_name, str(e)
            )
            raise AirflowException(f"Failed to create S3 bucket due to boto error: {str(e)}")

    @_s3_retryable
    def list_keys_with_retry(self, bucket_name: str, prefix: str = ""):
        """List S3 keys with retry logic."""
        self.log.debug("Listing S3 keys with retry: bucket=%s, prefix=%s", bucket_name, prefix)
        try:
            keys = self.list_keys(bucket_name=bucket_name, prefix=prefix)
            self.log.debug("Successfully listed %d key(s) from S3 bucket: %s", len(keys), bucket_name)
            return keys
        except ClientError as e:
            human_msg = map_s3_error_to_ozone(e)
            self.log.warning(
                "Failed to list S3 keys (will retry): bucket=%s, prefix=%s - %s",
                bucket_name,
                prefix,
                human_msg,
            )
            raise AirflowException(human_msg)
        except BotoCoreError as e:
            self.log.warning(
                "Failed to list S3 keys (will retry, boto error): bucket=%s, prefix=%s - %s",
                bucket_name,
                prefix,
                str(e),
            )
            raise AirflowException(f"Failed to list S3 keys due to boto error: {str(e)}")
