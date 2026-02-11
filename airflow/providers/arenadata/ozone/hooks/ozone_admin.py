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

import json
import subprocess
import time
from enum import Enum

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneHook
from airflow.utils.log.secrets_masker import redact


class OzoneResource(str, Enum):
    """Supported Ozone admin resource types."""

    VOLUME = "volume"
    BUCKET = "bucket"


class OzoneAdminHook(OzoneHook):
    """
    Interact with Ozone Admin CLI (ozone sh).

    Used for managing Volumes, Buckets, Quotas.
    """

    # NOTE: keep separate default name if you really want a separate Airflow connection.
    # If you want to reuse same conn as FS hooks -> set to "ozone_default".
    default_conn_name = "ozone_admin_default"
    hook_name = "Ozone Admin"

    # CHANGED: single place that prepares cmd/env and runs without retries
    def _exec(
        self,
        cmd: list[str],
        *,
        timeout: int = 300,
        input_text: str | None = None,
    ) -> tuple[str, str, int]:
        """
        Execute admin command once (no retries), returning (stdout, stderr, returncode).

        Uses OzoneHook internals:
        - _prepare_cli_command() adds --config for Kerberos if needed.
        - _build_env() sets OZONE_OM_ADDRESS and security env.
        - _run_command(check=False) returns rc without raising CalledProcessError.
        """
        prepared_cmd = self._prepare_cli_command(cmd)
        env = self._build_env()
        stdout, stderr, returncode = self._run_command(
            prepared_cmd,
            env,
            timeout=timeout,
            check=False,
            input_text=input_text,
        )
        return (stdout or ""), (stderr or ""), int(returncode)

    # CHANGED: simplified delete without manual CalledProcessError juggling
    def _delete_resource(
        self,
        *,
        resource_type: str,
        resource_path: str,
        cmd_base: list[str],
        not_found_keywords: list[str],
        recursive: bool = False,
        force: bool = False,
    ) -> None:
        """
        Delete volumes or buckets using shared logic.

        :param resource_type: Type of resource ("volume" or "bucket")
        :param resource_path: Full path to the resource (e.g., "/vol1" or "/vol1/bucket1")
        :param cmd_base: Base command list (e.g., ["ozone", "sh", "volume", "delete"])
        :param not_found_keywords: Keywords to check for "not found" errors
        :param recursive: If True, delete recursively
        :param force: If True, automatically confirm recursive deletion
        """
        self.log.info(
            "Deleting Ozone %s: %s (recursive=%s, force=%s)",
            resource_type,
            resource_path,
            recursive,
            force,
        )

        cmd = cmd_base.copy()
        if recursive:
            cmd.append("-r")
        cmd.append(resource_path)

        input_text = "yes\n" if (recursive and force) else None

        try:
            start = time.time()
            stdout, stderr, rc = self._exec(cmd, timeout=300, input_text=input_text)
            elapsed = time.time() - start

            if rc == 0:
                self.log.info(
                    "Successfully deleted %s: %s in %.2f seconds", resource_type, resource_path, elapsed
                )
                if stdout.strip():
                    self.log.debug("Command output: %s", redact(stdout.strip()))
                if stderr.strip():
                    self.log.debug("Command stderr: %s", redact(stderr.strip()))
                return

            err = (stderr or "").strip()
            err_l = err.lower()

            if any(k in err for k in not_found_keywords) or "does not exist" in err_l:
                self.log.info(
                    "%s %s does not exist, treating as success.", resource_type.capitalize(), resource_path
                )
                return

            masked_error = redact(err or "Unknown error")
            self.log.error("Failed to delete %s %s: %s", resource_type, resource_path, masked_error)
            raise AirflowException(f"Failed to delete {resource_type} {resource_path}: {masked_error}")
        except subprocess.TimeoutExpired:
            self.log.error("Timeout while deleting %s %s after 300 seconds", resource_type, resource_path)
            raise AirflowException(
                f"Timeout while deleting {resource_type} {resource_path}. The operation took longer than 5 minutes."
            )

    def create_volume(self, volume_name: str, quota: str | None = None) -> None:
        """
        Create a volume (e.g. ozone sh volume create /vol1).

        :param volume_name: Name of the volume to create (without leading slash)
        :param quota: Optional quota string (e.g., "10GB", "1TB")
        """
        volume_path = f"/{volume_name}"
        self.log.info("Creating Ozone volume: %s", volume_path)
        if quota:
            self.log.info("Volume quota will be set to: %s", quota)

        cmd = ["ozone", "sh", "volume", "create", volume_path]
        if quota:
            cmd.extend(["--quota", quota])

        try:
            start = time.time()
            stdout, stderr, rc = self._exec(cmd, timeout=300)
            elapsed = time.time() - start

            if rc == 0:
                self.log.info(
                    "Successfully created volume: %s (quota: %s) in %.2f seconds",
                    volume_path,
                    quota or "unlimited",
                    elapsed,
                )
                if stdout.strip():
                    self.log.debug("Command output: %s", redact(stdout.strip()))
                if stderr.strip():
                    self.log.debug("Command stderr: %s", redact(stderr.strip()))
                return

            err = (stderr or "").strip() or "No error message provided"
            if "VOLUME_ALREADY_EXISTS" in err:
                self.log.info("Volume %s already exists, treating as success.", volume_path)
                return

            masked_error = redact(err)
            raise AirflowException(f"Ozone command failed (return code: {rc}): {masked_error}")
        except subprocess.TimeoutExpired:
            self.log.error("Timeout while creating volume %s after 300 seconds", volume_path)
            raise AirflowException(
                f"Timeout while creating volume {volume_path}. The operation took longer than 5 minutes."
            )

    def create_bucket(self, volume_name: str, bucket_name: str, quota: str | None = None) -> None:
        """
        Create a bucket inside a volume.

        :param volume_name: Name of the parent volume
        :param bucket_name: Name of the bucket to create
        :param quota: Optional quota string for the bucket (e.g. "10GB").
                      In Ozone 2.0+, if a volume has a space quota,
                      bucket space quota must also be explicitly set.
        """
        bucket_path = f"/{volume_name}/{bucket_name}"
        self.log.info("Creating Ozone bucket: %s", bucket_path)
        self.log.debug(
            "Bucket details - volume: %s, bucket: %s, quota: %s",
            volume_name,
            bucket_name,
            quota or "none",
        )

        cmd = ["ozone", "sh", "bucket", "create", bucket_path]
        if quota:
            cmd.extend(["--quota", quota])

        try:
            start = time.time()
            stdout, stderr, rc = self._exec(cmd, timeout=300)
            elapsed = time.time() - start

            if rc == 0:
                self.log.info(
                    "Successfully created bucket: %s (quota: %s) in %.2f seconds",
                    bucket_path,
                    quota or "none",
                    elapsed,
                )
                if stdout.strip():
                    self.log.debug("Command output: %s", redact(stdout.strip()))
                if stderr.strip():
                    self.log.debug("Command stderr: %s", redact(stderr.strip()))
                return

            err = (stderr or "").strip() or "No error message provided"
            if "BUCKET_ALREADY_EXISTS" in err:
                self.log.info("Bucket %s already exists, treating as success.", bucket_path)
                return

            masked_error = redact(err)
            raise AirflowException(f"Ozone command failed (return code: {rc}): {masked_error}")
        except subprocess.TimeoutExpired:
            self.log.error("Timeout while creating bucket %s after 300 seconds", bucket_path)
            raise AirflowException(
                f"Timeout while creating bucket {bucket_path}. The operation took longer than 5 minutes."
            )

    def delete_volume(self, volume_name: str, recursive: bool = False, force: bool = False) -> None:
        """
        Delete a volume. Volume must be empty unless recursive=True.

        :param volume_name: Name of the volume to delete (without leading slash)
        :param recursive: If True, delete volume and all its contents (buckets, keys)
        :param force: If True, automatically confirm recursive deletion (requires recursive=True)
        """
        volume_path = f"/{volume_name}"
        self._delete_resource(
            resource_type=OzoneResource.VOLUME.value,
            resource_path=volume_path,
            cmd_base=["ozone", "sh", "volume", "delete"],
            not_found_keywords=["VOLUME_NOT_FOUND"],
            recursive=recursive,
            force=force,
        )

    def delete_bucket(
        self,
        volume_name: str,
        bucket_name: str,
        recursive: bool = False,
        force: bool = False,
    ) -> None:
        """
        Delete a bucket. Bucket must be empty unless recursive=True.

        :param volume_name: Name of the parent volume
        :param bucket_name: Name of the bucket to delete
        :param recursive: If True, delete bucket and all its contents (keys)
        :param force: If True, automatically confirm recursive deletion (requires recursive=True)
        """
        bucket_path = f"/{volume_name}/{bucket_name}"
        self._delete_resource(
            resource_type=OzoneResource.BUCKET.value,
            resource_path=bucket_path,
            cmd_base=["ozone", "sh", "bucket", "delete"],
            not_found_keywords=["BUCKET_NOT_FOUND"],
            recursive=recursive,
            force=force,
        )

    def get_container_report(self) -> dict:
        """
        Fetch low-level container reports from SCM.

        :return: Dictionary containing container report data
        """
        self.log.info("Fetching container report from Ozone SCM")
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        output = self.run_cli(["ozone", "admin", "container", "report", "--json"])

        try:
            report_data = json.loads(output)
            if not isinstance(report_data, (dict, list)):
                raise ValueError(
                    f"Container report is not a valid JSON object or array, got type: {type(report_data)}"
                )
            self.log.info("Successfully retrieved container report")
            self.log.debug(
                "Container report contains %d entries",
                len(report_data) if isinstance(report_data, list) else 1,
            )
            return report_data
        except json.JSONDecodeError as e:
            self.log.error("Failed to parse container report JSON: %s", str(e))
            masked_output = redact(output)
            self.log.debug("Raw output: %s", masked_output)
            raise AirflowException(f"Failed to parse container report JSON: {str(e)}") from e
        except ValueError as e:
            self.log.error("Invalid container report format: %s", str(e))
            masked_output = redact(output)
            self.log.debug("Raw output: %s", masked_output)
            raise AirflowException(f"Invalid container report format: {str(e)}") from e
