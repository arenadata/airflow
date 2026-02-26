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
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone_s3 import OzoneS3Hook
from airflow.providers.arenadata.ozone.utils import s3_client

if TYPE_CHECKING:
    from airflow.utils.context import Context

log = logging.getLogger(__name__)


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
        "ozone_conn_id",
        "s3_conn_id",
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
        if not ozone_bucket or not ozone_bucket.strip():
            raise ValueError("ozone_bucket parameter cannot be empty")
        if not s3_bucket or not s3_bucket.strip():
            raise ValueError("s3_bucket parameter cannot be empty")
        if ozone_prefix is None:
            raise ValueError("ozone_prefix parameter cannot be None (use empty string instead)")
        if s3_prefix is None:
            raise ValueError("s3_prefix parameter cannot be None (use empty string instead)")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        if not s3_conn_id or not s3_conn_id.strip():
            raise ValueError("s3_conn_id parameter cannot be empty")
        if not isinstance(max_workers, int) or max_workers <= 0:
            raise ValueError("max_workers parameter must be a positive integer")

        self.ozone_bucket = ozone_bucket
        self.s3_bucket = s3_bucket
        self.ozone_prefix = ozone_prefix
        self.s3_prefix = s3_prefix
        self.ozone_conn_id = ozone_conn_id
        self.s3_conn_id = s3_conn_id

        self.max_workers = max_workers

        self.log.debug(
            "Initializing OzoneToS3Operator - source: ozone://%s/%s, destination: s3://%s/%s, max_workers: %d",
            self.ozone_bucket,
            self.ozone_prefix,
            self.s3_bucket,
            self.s3_prefix,
            self.max_workers,
        )

    def execute(self, context: Context):
        self.log.info("Starting Ozone to S3 data transfer operation")
        self.log.info("Source: ozone://%s/%s", self.ozone_bucket, self.ozone_prefix)
        self.log.info("Destination: s3://%s/%s", self.s3_bucket, self.s3_prefix)
        self.log.debug("Ozone connection: %s, S3 connection: %s", self.ozone_conn_id, self.s3_conn_id)
        self.log.info("Using parallel transfer with %d worker(s) for bulk operations", self.max_workers)

        self.log.debug("Initializing connection pools for Ozone and destination S3")
        ozone_hook = OzoneS3Hook(ozone_conn_id=self.ozone_conn_id)
        dest_conn = BaseHook().get_connection(self.s3_conn_id)
        dest_client = s3_client.get_s3_client(dest_conn)

        ozone_hook.get_conn()
        self.log.debug("Connection pools initialized and ready for parallel operations")

        self.log.info("Listing keys in Ozone bucket: %s (prefix: %s)", self.ozone_bucket, self.ozone_prefix)
        keys_to_copy = ozone_hook.list_keys_with_retry(self.ozone_bucket, prefix=self.ozone_prefix)

        if not keys_to_copy:
            self.log.info(
                "No keys found to copy from Ozone bucket: %s (prefix: %s)",
                self.ozone_bucket,
                self.ozone_prefix,
            )
            return

        self.log.info("Found %d key(s) to transfer from Ozone to S3", len(keys_to_copy))
        self.log.debug("Keys to transfer: %s", keys_to_copy[:10])  # Log first 10 keys
        if len(keys_to_copy) > 10:
            self.log.debug("... and %d more keys", len(keys_to_copy) - 10)

        transferred_count = 0
        failed_count = 0
        count_lock = Lock()

        def copy_single_key(key: str) -> tuple[str, bool, str]:
            dest_key = key.replace(self.ozone_prefix, self.s3_prefix, 1)
            source_path = f"ozone://{self.ozone_bucket}/{key}"
            dest_path = f"s3://{self.s3_bucket}/{dest_key}"

            try:
                self.log.debug("Copying key: %s -> %s", source_path, dest_path)
                source_obj = ozone_hook.get_key_with_retry(key, self.ozone_bucket)
                s3_client.load_file_obj(
                    dest_client, source_obj.get()["Body"], dest_key, self.s3_bucket, replace=True
                )
                self.log.debug("Successfully copied key: %s", key)
                return (key, True, "")
            except Exception as e:
                error_msg = str(e)
                self.log.error("Failed to copy key: %s (error: %s)", key, error_msg)
                return (key, False, error_msg)

        if len(keys_to_copy) == 1:
            self.log.debug("Single key transfer, using sequential mode")
            _, success, _ = copy_single_key(keys_to_copy[0])
            if success:
                transferred_count += 1
            else:
                failed_count += 1
        else:
            self.log.info(
                "Starting parallel bulk transfer of %d key(s) using %d worker(s)",
                len(keys_to_copy),
                self.max_workers,
            )
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_key = {executor.submit(copy_single_key, key): key for key in keys_to_copy}

                completed = 0
                for future in as_completed(future_to_key):
                    completed += 1
                    _, success, _ = future.result()

                    with count_lock:
                        if success:
                            transferred_count += 1
                        else:
                            failed_count += 1

                    if len(keys_to_copy) > 10 and completed % max(1, len(keys_to_copy) // 10) == 0:
                        progress = (completed / len(keys_to_copy)) * 100
                        self.log.info(
                            "Transfer progress: %.1f%% (%d/%d keys completed)",
                            progress,
                            completed,
                            len(keys_to_copy),
                        )

            self.log.info("Transfer operation completed")
            self.log.info(
                "Successfully transferred: %d key(s), Failed: %d key(s)", transferred_count, failed_count
            )

        if failed_count > 0:
            self.log.warning("Some keys failed to transfer. Check logs above for details.")

        if failed_count > 0 and transferred_count == 0:
            raise AirflowException(f"All {failed_count} key(s) failed to transfer. Check logs for details.")

        return {
            "transferred": transferred_count,
            "failed": failed_count,
            "total": len(keys_to_copy),
        }
