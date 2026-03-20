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

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone import (
    OzoneAdminHook,
)
from airflow.providers.arenadata.ozone.utils.errors import OzoneCliError
from airflow.providers.arenadata.ozone.utils.params import (
    RETRY_ATTEMPTS,
    SLOW_TIMEOUT_SECONDS,
)
from airflow.utils.context import Context  # noqa: TCH001


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
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.volume = volume
        self.bucket = bucket
        self.snapshot_name = snapshot_name
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context):
        """Create snapshot and treat existing snapshot as success."""
        path = f"/{self.volume}/{self.bucket}"
        cmd = ["ozone", "sh", "snapshot", "create", path, self.snapshot_name]

        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        try:
            hook.run_cli(cmd, timeout=self.timeout)
            self.log.info("Snapshot created: %s (path=%s)", self.snapshot_name, path)
        except OzoneCliError as e:
            error_stderr = (e.stderr or "").strip()
            stderr_upper = error_stderr.upper()
            if "FILE_ALREADY_EXISTS" in stderr_upper or "SNAPSHOT ALREADY EXISTS" in stderr_upper:
                self.log.info(
                    "Snapshot %s already exists, treating as success (idempotent operation).",
                    self.snapshot_name,
                )
                return
            raise AirflowException(str(e)) from e
