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

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneHook  # Import the base hook
from airflow.utils.log.secrets_masker import redact


class OzoneAdminHook(OzoneHook):  # Inherit from OzoneHook
    """
    Interact with Ozone Admin CLI (ozone sh).

    Used for managing Volumes, Buckets, Quotas.
    """

    # Use the same connection type as the base OzoneHook to avoid
    # duplicate connection type registrations. This hook represents
    # an administrative view on the same underlying Ozone connection.
    default_conn_name = "ozone_admin_default"
    hook_name = "Ozone Admin"

    def _get_merged_env(self) -> dict[str, str]:
        """
        Get merged environment variables for subprocess (OM address, SSL, Kerberos).

        Uses base hook's _build_env() so OZONE_OM_ADDRESS is set from the connection;
        without it the Ozone CLI would not know where to connect and could hang.
        """
        return self._build_env()

    def _delete_resource(
        self,
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
            "Deleting Ozone %s: %s (recursive: %s, force: %s)", resource_type, resource_path, recursive, force
        )

        cmd = cmd_base.copy()
        if recursive:
            cmd.append("-r")
        cmd.append(resource_path)
        cmd = self._prepare_cli_command(cmd)

        try:
            if recursive and force:
                env = self._get_merged_env()
                self.log.info("Performing recursive %s deletion with automatic confirmation", resource_type)
                stdout, stderr, returncode = self._run_command(
                    cmd, env, timeout=300, check=False, input_text="yes\n"
                )
                if returncode != 0:
                    raise subprocess.CalledProcessError(returncode, cmd, stdout, stderr)
                self.log.info("Successfully deleted %s (recursively): %s", resource_type, resource_path)
                if stdout:
                    self.log.debug("Command output: %s", redact(stdout.strip()))
            else:
                self.run_cli(cmd)
                self.log.info("Successfully deleted %s: %s", resource_type, resource_path)
        except AirflowException as e:
            msg = str(e)
            if any(keyword in msg for keyword in not_found_keywords) or "does not exist" in msg.lower():
                self.log.info(
                    "%s %s does not exist, treating as success.", resource_type.capitalize(), resource_path
                )
                return
            raise
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.strip() if e.stderr else "Unknown error"
            if (
                any(keyword in error_msg for keyword in not_found_keywords)
                or "does not exist" in error_msg.lower()
            ):
                self.log.info(
                    "%s %s does not exist, treating as success.", resource_type.capitalize(), resource_path
                )
                return
            masked_error = redact(error_msg)
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
        else:
            self.log.debug("No quota specified for volume: %s", volume_path)

        cmd = ["ozone", "sh", "volume", "create", volume_path]
        if quota:
            cmd.extend(["--quota", quota])

        # Prepare command with --config flag if Kerberos is enabled and configs exist
        # This explicitly tells Ozone Shell where to find security configuration files,
        # which is critical for Kerberos authentication (see HDDS-4602 bug workaround)
        cmd = self._prepare_cli_command(cmd)

        # Mask sensitive data in command before logging
        masked_cmd_str = redact(" ".join(cmd))
        self.log.debug("Volume creation command: %s", masked_cmd_str)
        try:
            # Use _run_command instead of run_cli() to handle "already exists"
            # without retry logic. Idempotent: VOLUME_ALREADY_EXISTS is treated as success.
            env = self._get_merged_env()
            debug_env = {
                key: env.get(key)
                for key in (
                    "HADOOP_SECURITY_AUTHENTICATION",
                    "HADOOP_OPTS",
                    "OZONE_OPTS",
                    "OZONE_SECURITY_ENABLED",
                    "OZONE_CONF_DIR",
                    "HADOOP_CONF_DIR",
                )
                if key in env
            }
            masked_debug_env = {k: (redact(str(v)) if v is not None else None) for k, v in debug_env.items()}
            self.log.debug("Ozone volume create Kerberos/SSL env: %s", masked_debug_env)
            start_time = time.time()
            stdout, stderr, returncode = self._run_command(cmd, env, timeout=300, check=False)
            execution_time = time.time() - start_time
            if returncode != 0:
                raise subprocess.CalledProcessError(returncode, cmd, stdout, stderr)
            self.log.info(
                "Successfully created volume: %s (quota: %s) in %.2f seconds",
                volume_path,
                quota or "unlimited",
                execution_time,
            )
        except subprocess.CalledProcessError as e:
            error_message = e.stderr.strip() if e.stderr else "No error message provided"
            if "VOLUME_ALREADY_EXISTS" in error_message:
                self.log.info("Volume %s already exists, treating as success.", volume_path)
                return
            masked_error = redact(error_message)
            raise AirflowException(f"Ozone command failed (return code: {e.returncode}): {masked_error}")

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
            "Bucket details - volume: %s, bucket: %s, quota: %s", volume_name, bucket_name, quota or "none"
        )

        cmd = ["ozone", "sh", "bucket", "create", bucket_path]
        if quota:
            cmd.extend(["--quota", quota])

        cmd = self._prepare_cli_command(cmd)

        try:
            env = self._get_merged_env()
            start_time = time.time()
            stdout, stderr, returncode = self._run_command(cmd, env, timeout=300, check=False)
            execution_time = time.time() - start_time
            if returncode != 0:
                raise subprocess.CalledProcessError(returncode, cmd, stdout, stderr)
            self.log.info(
                "Successfully created bucket: %s (quota: %s) in %.2f seconds",
                bucket_path,
                quota or "none",
                execution_time,
            )
        except subprocess.CalledProcessError as e:
            error_message = (e.stderr or "").strip() or "No error message provided"
            if "BUCKET_ALREADY_EXISTS" in error_message:
                self.log.info("Bucket %s already exists, treating as success.", bucket_path)
                return
            masked_error = redact(error_message)
            raise AirflowException(f"Ozone command failed (return code: {e.returncode}): {masked_error}")

    def delete_volume(self, volume_name: str, recursive: bool = False, force: bool = False) -> None:
        """
        Delete a volume. Volume must be empty unless recursive=True.

        :param volume_name: Name of the volume to delete (without leading slash)
        :param recursive: If True, delete volume and all its contents (buckets, keys)
        :param force: If True, automatically confirm recursive deletion (requires recursive=True)
        """
        volume_path = f"/{volume_name}"
        self._delete_resource(
            resource_type="volume",
            resource_path=volume_path,
            cmd_base=["ozone", "sh", "volume", "delete"],
            not_found_keywords=["VOLUME_NOT_FOUND"],
            recursive=recursive,
            force=force,
        )

    def delete_bucket(
        self, volume_name: str, bucket_name: str, recursive: bool = False, force: bool = False
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
            resource_type="bucket",
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
