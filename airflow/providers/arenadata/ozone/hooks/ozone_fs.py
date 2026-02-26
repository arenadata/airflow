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

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.hooks.ozone import (
    OzoneCliError,
    OzoneCliTransientError,
    OzoneHook,
)


class OzoneFsHook(OzoneHook):  # Inherit from OzoneHook
    """
    Interact with Ozone via CLI (o3fs:// or ofs:// schemes).

    Uses 'ozone fs' or 'hadoop fs' commands.
    """

    # The default_conn_name is inherited.
    hook_name = "Ozone FS"

    def mkdir(self, path: str) -> None:
        """
        Create directory in Ozone FS.

        :param path: Ozone path (e.g., ofs://vol1/bucket1/dir1)
        """
        self.log.info("Creating directory in Ozone FS: %s", path)
        self.log.debug(
            "Directory path details - scheme: %s", path.split("://")[0] if "://" in path else "unknown"
        )

        cmd = ["ozone", "fs", "-mkdir", "-p", path]
        self.run_cli(cmd)  # Use inherited run_cli

        self.log.info("Successfully created directory: %s", path)

    def copy_from_local(self, local_path: str, remote_path: str) -> None:
        """
        Upload file to Ozone FS.

        :param local_path: Local file path on the Airflow worker
        :param remote_path: Ozone destination path (e.g., ofs://vol1/bucket1/file.txt)
        """
        self.log.info(
            "Uploading file from local filesystem to Ozone (local: %s -> remote: %s)",
            local_path,
            remote_path,
        )

        if not os.path.exists(local_path):
            self.log.error("Local file does not exist: %s", local_path)
            raise AirflowException(f"Local file does not exist: {local_path}")

        file_size = os.path.getsize(local_path)
        self.log.debug("Local file exists, size: %d bytes", file_size)

        cmd = ["ozone", "fs", "-put", "-f", local_path, remote_path]
        self.run_cli(cmd)

        self.log.info("Successfully uploaded file to Ozone: %s", remote_path)

    def exists(self, path: str) -> bool:
        """
        Check if path exists in Ozone FS.

        :param path: Ozone path to check
        :return: True if path exists, False otherwise
        """
        self.log.debug("Checking existence of path in Ozone FS: %s", path)
        cmd = ["ozone", "fs", "-test", "-e", path]
        try:
            result = self.run_cli_check(cmd, timeout=30)
            if result.returncode == 0:
                self.log.debug("Path exists in Ozone: %s", path)
                return True

            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            combined = (stderr or stdout).lower()
            if any(
                x in combined for x in ("timed out", "timeout", "connection refused", "service unavailable")
            ):
                raise OzoneCliTransientError(f"Transient error while checking path existence: {path}")

            self.log.debug("Path does not exist in Ozone: %s", path)
            return False
        except OzoneCliTransientError:
            raise
        except FileNotFoundError as e:
            raise OzoneCliError("Ozone CLI not found in PATH") from e
        except Exception as e:
            self.log.error("Unexpected error while checking path existence: %s (error: %s)", path, str(e))
            raise

    def list_paths(self, path: str) -> list[str]:
        """
        List files in path.

        :param path: Ozone path to list (e.g., ofs://vol1/bucket1/data/)
        :return: List of file paths
        """
        self.log.info("Listing paths in Ozone FS: %s", path)
        cmd = ["ozone", "fs", "-ls", "-C", path]
        output = self.run_cli(cmd)

        paths = output.splitlines() if output else []
        self.log.info("Found %d path(s) in %s", len(paths), path)
        if paths:
            self.log.debug("Paths found: %s", paths[:10])  # Log first 10 paths
            if len(paths) > 10:
                self.log.debug("... and %d more paths", len(paths) - 10)

        return paths

    def set_key_property(self, path: str, replication_factor: int | None = None) -> None:
        """
        Set properties for a given key, such as replication factor.

        :param path: The full path to the key (e.g., ofs://vol1/bucket1/key1).
        :param replication_factor: The desired replication factor (e.g., 3).
        """
        if replication_factor:
            self.log.info("Setting replication factor to %d for path: %s", replication_factor, path)
            self.log.debug("Path details - scheme: %s", path.split("://")[0] if "://" in path else "unknown")

            cmd = ["ozone", "fs", "-setrep", str(replication_factor), path]
            self.run_cli(cmd)

            self.log.info("Successfully set replication factor to %d for path: %s", replication_factor, path)
        else:
            self.log.warning("No key properties were specified to be set for path: %s", path)
