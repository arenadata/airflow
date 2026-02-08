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
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Get the logger for tenacity to use
log = logging.getLogger(__name__)


class OzoneS3Hook(S3Hook):
    """
    Interact with Ozone via S3 Gateway. Wraps Boto3 S3Hook.

    This hook optimizes connection reuse through boto3's built-in HTTP connection pool.
    boto3 uses urllib3's HTTP connection pool under the hood, which automatically manages
    connection reuse, keep-alive, and connection limits for optimal performance in parallel operations.

    SSL/TLS Support:
    - SSL/TLS is automatically enabled when using HTTPS endpoints (https://)
    - Configure endpoint_url in connection Extra field: {"endpoint_url": "https://s3g:9879"}
    - For self-signed certificates, set verify=False in connection Extra:
      {"endpoint_url": "https://s3g:9879", "verify": false}
    - For custom CA certificates, set verify to certificate path:
      {"endpoint_url": "https://s3g:9879", "verify": "/path/to/ca-cert.pem"}

    Secrets Backend Support:
    - AWS credentials (login/password) can be stored in Airflow Secrets Backend
    - Use secret:// paths in connection Login/Password fields:
      - Login: "secret://vault/ozone/access-key"
      - Password: "secret://vault/ozone/secret-key"
    - Credentials resolution is delegated to Airflow/AWS provider mechanisms (so assume-role and other
      standard AWS auth features continue to work).
    """

    # Reuse the standard AWS connection type from S3Hook (conn_type='aws'),
    # but expose a clearer name in the UI.
    hook_name = "Ozone S3 (AWS)"
    default_conn_name = "ozone_s3_default"

    def __init__(self, ozone_conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__(aws_conn_id=ozone_conn_id, *args, **kwargs)
        # Avoid noisy logs during DAG parsing; use DEBUG here.
        self.log.debug("Initializing OzoneS3Hook with connection ID: %s", ozone_conn_id)
        self.log.debug("OzoneS3Hook initialized successfully (using AWS provider auth/session machinery)")

        # Check for Secrets Backend usage and SSL/TLS configuration
        # Note: Airflow's BaseHook.get_connection() automatically resolves secret:// paths
        # We check here for logging purposes
        try:
            conn = self.get_connection(ozone_conn_id)
            if conn.login and conn.login.startswith("secret://"):
                self.log.info("Using Secrets Backend for AWS Access Key: %s", conn.login)
            if conn.password and conn.password.startswith("secret://"):
                self.log.info(
                    "Using Secrets Backend for AWS Secret Key: %s",
                    conn.password.split("/")[-1] if "/" in conn.password else "***",
                )

            # Check if SSL/TLS is enabled via endpoint_url
            extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}
            endpoint_url = extra.get("endpoint_url", "")
            verify = extra.get("verify", None)
        except Exception as e:
            self.log.debug(
                "Could not check connection for Secrets Backend/SSL (may not exist yet): %s", str(e)
            )
            endpoint_url = ""
            verify = None

        # If verify wasn't explicitly provided to the hook, allow configuring it via connection Extra.
        # This preserves standard AWS provider behavior (assume role, session tokens, etc.) while
        # enabling custom certificate verification controls for the S3 Gateway endpoint.
        current_verify = getattr(self, "_verify", None)
        if current_verify is None and verify is not None:
            setattr(self, "_verify", verify)

        if endpoint_url.startswith("https://"):
            self.log.info("SSL/TLS enabled: Using HTTPS endpoint: %s", endpoint_url)
            verify_effective = getattr(self, "_verify", None)
            if verify_effective is False:
                self.log.warning(
                    "SSL certificate verification is disabled (verify=False). "
                    "This should only be used for development/testing."
                )
            elif isinstance(verify_effective, str):
                self.log.info("Using custom CA certificate for SSL verification: %s", verify_effective)
            else:
                self.log.debug("Using default SSL certificate verification")
        elif endpoint_url.startswith("http://"):
            self.log.warning("Using unencrypted HTTP connection. Consider using HTTPS for production.")

        self.log.debug(
            "Connection pooling: boto3 uses urllib3 connection pooling automatically. "
            "Connections are reused and managed efficiently."
        )

    @retry(
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((ClientError, BotoCoreError, AirflowException)),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )
    def get_key_with_retry(self, key: str, bucket_name: str):
        """
        Get S3 key with retry logic for transient network errors.

        :param key: S3 key to retrieve
        :param bucket_name: S3 bucket name
        :return: S3 key object
        """
        self.log.debug("Getting S3 key with retry: s3://%s/%s", bucket_name, key)
        try:
            key_obj = self.get_key(key, bucket_name)
            self.log.debug("Successfully retrieved S3 key: s3://%s/%s", bucket_name, key)
            return key_obj
        except (ClientError, BotoCoreError) as e:
            self.log.warning("Failed to get S3 key (will retry): s3://%s/%s - %s", bucket_name, key, str(e))
            raise AirflowException(f"Failed to get S3 key: {str(e)}")

    @retry(
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((ClientError, BotoCoreError, AirflowException)),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )
    def load_file_obj_with_retry(self, file_obj, key: str, bucket_name: str, replace: bool = False):
        """
        Load file object to S3 with retry logic for transient network errors.

        :param file_obj: File-like object to upload
        :param key: S3 key (path) to upload to
        :param bucket_name: S3 bucket name
        :param replace: Whether to replace existing key
        """
        self.log.debug("Loading file object to S3 with retry: s3://%s/%s", bucket_name, key)
        try:
            self.load_file_obj(file_obj, key, bucket_name, replace=replace)
            self.log.debug("Successfully loaded file object to S3: s3://%s/%s", bucket_name, key)
        except (ClientError, BotoCoreError) as e:
            self.log.warning(
                "Failed to load file object to S3 (will retry): s3://%s/%s - %s", bucket_name, key, str(e)
            )
            raise AirflowException(f"Failed to load file object to S3: {str(e)}")

    @retry(
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((ClientError, BotoCoreError, AirflowException)),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )
    def load_string_with_retry(self, string_data: str, key: str, bucket_name: str, replace: bool = False):
        """
        Load string data to S3 with retry logic for transient network errors.

        :param string_data: String data to upload
        :param key: S3 key (path) to upload to
        :param bucket_name: S3 bucket name
        :param replace: Whether to replace existing key
        """
        self.log.debug("Loading string to S3 with retry: s3://%s/%s", bucket_name, key)
        try:
            self.load_string(string_data, key, bucket_name, replace=replace)
            self.log.debug("Successfully loaded string to S3: s3://%s/%s", bucket_name, key)
        except (ClientError, BotoCoreError) as e:
            self.log.warning(
                "Failed to load string to S3 (will retry): s3://%s/%s - %s", bucket_name, key, str(e)
            )
            raise AirflowException(f"Failed to load string to S3: {str(e)}")

    @retry(
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((ClientError, BotoCoreError, AirflowException)),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )
    def create_bucket_with_retry(self, bucket_name: str):
        """
        Create S3 bucket with retry logic for transient network errors.

        :param bucket_name: Name of the bucket to create
        """
        self.log.debug("Creating S3 bucket with retry: %s", bucket_name)
        try:
            self.create_bucket(bucket_name=bucket_name)
            self.log.debug("Successfully created S3 bucket: %s", bucket_name)
        except (ClientError, BotoCoreError) as e:
            # Check if bucket already exists (not a retryable error)
            error_code = e.response.get("Error", {}).get("Code", "") if hasattr(e, "response") else ""
            if error_code == "BucketAlreadyExists" or "BucketAlreadyOwnedByYou" in str(e):
                self.log.info("Bucket already exists: %s", bucket_name)
                return
            self.log.warning("Failed to create S3 bucket (will retry): %s - %s", bucket_name, str(e))
            raise AirflowException(f"Failed to create S3 bucket: {str(e)}")

    @retry(
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((ClientError, BotoCoreError, AirflowException)),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )
    def list_keys_with_retry(self, bucket_name: str, prefix: str = ""):
        """
        List S3 keys with retry logic for transient network errors.

        :param bucket_name: S3 bucket name
        :param prefix: Prefix to filter keys
        :return: List of keys
        """
        self.log.debug("Listing S3 keys with retry: bucket=%s, prefix=%s", bucket_name, prefix)
        try:
            keys = self.list_keys(bucket_name=bucket_name, prefix=prefix)
            self.log.debug("Successfully listed %d key(s) from S3 bucket: %s", len(keys), bucket_name)
            return keys
        except (ClientError, BotoCoreError) as e:
            self.log.warning(
                "Failed to list S3 keys (will retry): bucket=%s, prefix=%s - %s", bucket_name, prefix, str(e)
            )
            raise AirflowException(f"Failed to list S3 keys: {str(e)}")
