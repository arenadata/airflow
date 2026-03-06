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
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from functools import cached_property, partial

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone_s3 import OzoneS3Hook
from airflow.providers.arenadata.ozone.utils.cli_runner import CliRunner
from airflow.providers.arenadata.ozone.utils.errors import OzoneS3Error, OzoneS3Errors
from airflow.providers.arenadata.ozone.utils.s3_client import OzoneS3Client
from airflow.utils.context import Context  # noqa: TCH001

log = logging.getLogger(__name__)
KEY_LOG_PREVIEW_LIMIT = 10
PROGRESS_LOG_STEPS = 10


class OzoneToS3Operator(BaseOperator):
    """
    Copy data from an Ozone bucket to an external S3 bucket.

    This operator is memory-efficient as it streams data and supports parallel transfers
    for bulk operations to optimize performance.
    """

    template_fields = (
        "ozone_bucket",
        "ozone_prefix",
        "s3_bucket",
        "s3_prefix",
    )

    def __init__(
        self,
        *,
        ozone_bucket: str,
        s3_bucket: str,
        ozone_prefix: str = "",
        s3_prefix: str = "",
        ozone_conn_id: str = OzoneS3Hook.default_conn_name,
        s3_conn_id: str = "aws_default",
        max_workers: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ozone_bucket = ozone_bucket
        self.s3_bucket = s3_bucket
        self.ozone_conn_id = ozone_conn_id
        self.s3_conn_id = s3_conn_id
        if max_workers <= 0:
            raise ValueError("max_workers parameter must be a positive integer")

        # Keep template fields assigned directly; use normalized copies for transfer logic.
        self.ozone_prefix = ozone_prefix
        self.s3_prefix = s3_prefix
        self._ozone_prefix_normalized = self._normalize_prefix(ozone_prefix)
        self._s3_prefix_normalized = self._normalize_prefix(s3_prefix)
        self.max_workers = max_workers

        self.log.debug(
            "Initializing OzoneToS3Operator - source: ozone://%s/%s, destination: s3://%s/%s, max_workers: %d",
            self.ozone_bucket,
            self.ozone_prefix,
            self.s3_bucket,
            self.s3_prefix,
            self.max_workers,
        )

    @staticmethod
    def _normalize_prefix(prefix: str) -> str:
        """Normalize prefix for stable key mapping (strip, remove leading slash, keep trailing slash)."""
        normalized = (prefix or "").strip().lstrip("/")
        if not normalized:
            return ""
        return normalized if normalized.endswith("/") else f"{normalized}/"

    def _raise_retryable_transfer_error(self, err: Exception, action: str) -> None:
        """Classify destination S3 transfer failure as retryable or terminal."""
        if isinstance(err, ClientError):
            error_code = str(err.response.get("Error", {}).get("Code", "")).strip()
            http_status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            human_msg = OzoneS3Errors.map_s3_error_to_ozone(err)
            if error_code in OzoneS3Errors.non_retryable_errors:
                raise AirflowException(human_msg)
            if OzoneS3Errors.is_retryable_failure(http_status, error_code):
                raise OzoneS3Error(human_msg, retryable=True)
            raise OzoneS3Error(human_msg, retryable=True)
        if isinstance(err, (BotoCoreError, OSError)):
            raise OzoneS3Error(f"Failed to {action}: {str(err)}", retryable=True)
        raise err

    @CliRunner.retry_for(
        retry_condition=lambda exc: isinstance(exc, OzoneS3Error) and exc.retryable,
        logger=log,
    )
    def _load_file_obj_with_retry(self, dest_client: object, body: object, dest_key: str) -> None:
        """Upload one object to destination S3 with retry policy."""
        try:
            OzoneS3Client.load_file_obj(
                dest_client,
                body,
                dest_key,
                self.s3_bucket,
                replace=True,
                transfer_config=self._parallel_transfer_config,
            )
        except (ClientError, BotoCoreError, OSError) as err:
            self._raise_retryable_transfer_error(err, f"upload object to s3://{self.s3_bucket}/{dest_key}")

    def _init_clients(self) -> tuple[OzoneS3Hook, object]:
        """Initialize source Ozone hook and destination S3 client."""
        self.log.debug("Initializing connection pools for Ozone and destination S3")
        ozone_hook = OzoneS3Hook(ozone_conn_id=self.ozone_conn_id)
        ozone_hook.get_conn()
        dest_conn = BaseHook.get_connection(self.s3_conn_id)
        dest_client = OzoneS3Client.get_s3_client(dest_conn)
        self.log.debug("Connection pools initialized and ready for parallel operations")
        return ozone_hook, dest_client

    @cached_property
    def _parallel_transfer_config(self) -> TransferConfig:
        """Disable boto3 internal transfer threading for external parallel mode."""
        return TransferConfig(use_threads=False)

    def _build_dest_key(self, key: str) -> str:
        """Build destination key by replacing source prefix only at the start."""
        if not self._ozone_prefix_normalized:
            return f"{self._s3_prefix_normalized}{key}"

        if not key.startswith(self._ozone_prefix_normalized):
            raise AirflowException(
                f"Source key '{key}' does not start with ozone_prefix '{self._ozone_prefix_normalized}'"
            )

        relative_key = key[len(self._ozone_prefix_normalized) :]
        return f"{self._s3_prefix_normalized}{relative_key}"

    def _copy_single_key(
        self,
        key: str,
        ozone_hook: OzoneS3Hook,
        dest_client: object,
    ) -> bool:
        """Copy one key and return True on success."""
        body = None
        try:
            dest_key = self._build_dest_key(key)
            source_path = f"ozone://{self.ozone_bucket}/{key}"
            dest_path = f"s3://{self.s3_bucket}/{dest_key}"
            self.log.debug("Copying key: %s -> %s", source_path, dest_path)
            source_obj = ozone_hook.get_key_with_retry(key, self.ozone_bucket)
            body = source_obj["Body"]
            self._load_file_obj_with_retry(dest_client, body, dest_key)
            self.log.debug("Successfully copied key: %s", key)
            return True
        except (AirflowException, ClientError, BotoCoreError, OSError, ValueError) as err:
            self.log.error(
                "Failed to copy key: %s (error_type=%s, error=%s)",
                key,
                type(err).__name__,
                str(err),
            )
            return False
        finally:
            close_fn = getattr(body, "close", None)
            if callable(close_fn):
                with suppress(Exception):
                    close_fn()

    def _transfer_keys_parallel(
        self,
        keys_to_copy: list[str],
        ozone_hook: OzoneS3Hook,
        dest_client: object,
    ) -> tuple[int, int]:
        """Transfer keys with optional parallel execution and return (transferred, failed)."""
        if len(keys_to_copy) == 1:
            self.log.debug("Single key transfer, using sequential mode")
            return (1, 0) if self._copy_single_key(keys_to_copy[0], ozone_hook, dest_client) else (0, 1)

        self.log.info(
            "Starting parallel bulk transfer of %d key(s) using %d worker(s)",
            len(keys_to_copy),
            self.max_workers,
        )
        transferred_count = 0
        failed_count = 0

        copy_key = partial(self._copy_single_key, ozone_hook=ozone_hook, dest_client=dest_client)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for completed, is_success in enumerate(executor.map(copy_key, keys_to_copy), start=1):
                if is_success:
                    transferred_count += 1
                else:
                    failed_count += 1

                if (
                    len(keys_to_copy) > KEY_LOG_PREVIEW_LIMIT
                    and completed % max(1, len(keys_to_copy) // PROGRESS_LOG_STEPS) == 0
                ):
                    progress = (completed / len(keys_to_copy)) * 100
                    self.log.info(
                        "Transfer progress: %.1f%% (%d/%d keys completed)",
                        progress,
                        completed,
                        len(keys_to_copy),
                    )

        return transferred_count, failed_count

    def execute(self, context: Context):
        self.log.info(
            "Starting Ozone->S3 transfer: ozone://%s/%s -> s3://%s/%s (workers=%d)",
            self.ozone_bucket,
            self.ozone_prefix,
            self.s3_bucket,
            self.s3_prefix,
            self.max_workers,
        )
        self.log.debug("Ozone connection: %s, S3 connection: %s", self.ozone_conn_id, self.s3_conn_id)

        ozone_hook, dest_client = self._init_clients()

        self.log.debug(
            "Listing source keys in Ozone bucket: %s (prefix: %s)", self.ozone_bucket, self.ozone_prefix
        )
        keys_to_copy = ozone_hook.list_keys_with_retry(
            self.ozone_bucket, prefix=self._ozone_prefix_normalized
        )

        if not keys_to_copy:
            self.log.info(
                "No keys found for Ozone->S3 transfer in bucket=%s with prefix=%s",
                self.ozone_bucket,
                self.ozone_prefix,
            )
            return {"transferred": 0, "failed": 0, "total": 0}

        self.log.info("Found %d key(s) to transfer from Ozone to S3", len(keys_to_copy))
        self.log.debug("Keys to transfer: %s", keys_to_copy[:KEY_LOG_PREVIEW_LIMIT])
        if len(keys_to_copy) > KEY_LOG_PREVIEW_LIMIT:
            self.log.debug("... and %d more keys", len(keys_to_copy) - KEY_LOG_PREVIEW_LIMIT)

        transferred_count, failed_count = self._transfer_keys_parallel(keys_to_copy, ozone_hook, dest_client)
        self.log.info(
            "Ozone->S3 transfer finished: transferred=%d failed=%d total=%d",
            transferred_count,
            failed_count,
            len(keys_to_copy),
        )

        if failed_count > 0:
            raise AirflowException(
                f"Failed to transfer {failed_count} of {len(keys_to_copy)} key(s). Check logs for details."
            )

        return {
            "transferred": transferred_count,
            "failed": failed_count,
            "total": len(keys_to_copy),
        }
