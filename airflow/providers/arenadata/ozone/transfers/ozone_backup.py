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

import os
import subprocess
import time
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone_admin import OzoneAdminHook
from airflow.utils.log.secrets_masker import redact


class OzoneBackupOperator(BaseOperator):
    """Creates a snapshot of a bucket for backup and disaster recovery."""

    template_fields = ("volume", "bucket", "snapshot_name")

    def __init__(
        self,
        *,
        volume: str,
        bucket: str,
        snapshot_name: str,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not volume or not volume.strip():
            raise ValueError("volume parameter cannot be empty")
        if not bucket or not bucket.strip():
            raise ValueError("bucket parameter cannot be empty")
        if not snapshot_name or not snapshot_name.strip():
            raise ValueError("snapshot_name parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")

        self.volume = volume
        self.bucket = bucket
        self.snapshot_name = snapshot_name
        self.ozone_conn_id = ozone_conn_id.strip()

    def execute(self, context: Any):
        """
        Execute the snapshot creation command.

        Idempotent operation: if snapshot already exists, treats it as success.
        """
        vol = (self.volume or "").strip() or self.volume
        buck = (self.bucket or "").strip() or self.bucket
        path = f"/{vol}/{buck}"
        snapshot_name_use = (self.snapshot_name or "").strip() or self.snapshot_name

        self.log.info("Starting Ozone snapshot creation operation")
        self.log.info(
            "Volume: %s, Bucket: %s, Snapshot name: %s", self.volume, self.bucket, self.snapshot_name
        )
        self.log.info("Target path: %s", path)
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneAdminHook(ozone_conn_id=self.ozone_conn_id)
        cmd = ["ozone", "sh", "snapshot", "create", path, snapshot_name_use]

        # Use direct subprocess call instead of run_cli() to handle "already exists" errors
        # gracefully without triggering retry logic. The run_cli() method has retry decorator
        # that would retry on AirflowException, but "FILE_ALREADY_EXISTS" is expected and
        # should be treated as success (idempotent operation).
        # Get merged environment variables (SSL + Kerberos) if available
        if hasattr(hook, "_get_merged_env"):
            try:
                env = hook._get_merged_env()
            except Exception:
                env = os.environ.copy()
        else:
            env = os.environ.copy()
        start_time = time.time()
        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True, env=env, timeout=300)
            execution_time = time.time() - start_time
            self.log.info(
                "Successfully created snapshot: %s in %.2f seconds", self.snapshot_name, execution_time
            )
            self.log.info("Snapshot path: %s, Volume: %s, Bucket: %s", path, self.volume, self.bucket)
        except subprocess.CalledProcessError as e:
            error_message = e.stderr.strip() if e.stderr else "No error message provided"
            if "FILE_ALREADY_EXISTS" in error_message or "Snapshot already exists" in error_message:
                self.log.info(
                    "Snapshot %s already exists, treating as success (idempotent operation).",
                    self.snapshot_name,
                )
                return
            masked_error = redact(error_message)
            raise AirflowException(f"Ozone command failed (return code: {e.returncode}): {masked_error}")
