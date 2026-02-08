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
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone_fs import OzoneFsHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LocalFilesystemToOzoneOperator(BaseOperator):
    """
    Upload a file from the local filesystem (Airflow Worker) to Ozone.

    Wraps 'ozone fs -put'.
    """

    template_fields = ("local_path", "remote_path")

    def __init__(
        self,
        local_path: str,
        remote_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        overwrite: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not local_path or not local_path.strip():
            raise ValueError("local_path parameter cannot be empty")
        if not remote_path or not remote_path.strip():
            raise ValueError("remote_path parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        self.local_path = local_path
        self.remote_path = remote_path
        self.ozone_conn_id = ozone_conn_id.strip()
        self.overwrite = overwrite

    def execute(self, context: Context):
        self.log.info("Starting local filesystem to Ozone upload operation")
        self.log.info("Local path: %s, Remote path: %s", self.local_path, self.remote_path)
        self.log.debug("Overwrite mode: %s", self.overwrite)
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneFsHook(ozone_conn_id=self.ozone_conn_id)

        local_path_use = self.local_path.strip() if self.local_path else self.local_path
        remote_path_use = self.remote_path.strip() if self.remote_path else self.remote_path
        if not os.path.exists(local_path_use):
            self.log.error("Local file not found: %s", local_path_use)
            raise AirflowException(f"Local file not found: {local_path_use}")

        local_file_size = os.path.getsize(local_path_use)
        self.log.debug("Local file exists, size: %d bytes", local_file_size)

        # Check if remote exists
        if hook.exists(remote_path_use):
            if not self.overwrite:
                self.log.error("Remote path already exists and overwrite is disabled: %s", remote_path_use)
                raise AirflowException(f"Remote path {remote_path_use} already exists and overwrite is False")
            else:
                self.log.warning("Remote path already exists, will overwrite: %s", remote_path_use)
        else:
            self.log.debug("Remote path does not exist, will create: %s", remote_path_use)

        self.log.info(
            "Uploading file to Ozone: %s -> %s (%d bytes)", local_path_use, remote_path_use, local_file_size
        )
        hook.copy_from_local(local_path_use, remote_path_use)

        self.log.info("Successfully uploaded file to Ozone: %s", remote_path_use)


class OzoneToOzoneOperator(BaseOperator):
    """
    Move or rename a key within the same Ozone cluster.

    This is a metadata-only operation on the Ozone side, making it extremely fast.
    """

    template_fields = ("source_path", "dest_path")

    def __init__(
        self,
        *,
        source_path: str,
        dest_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not source_path or not source_path.strip():
            raise ValueError("source_path parameter cannot be empty")
        if not dest_path or not dest_path.strip():
            raise ValueError("dest_path parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        self.source_path = source_path
        self.dest_path = dest_path
        self.ozone_conn_id = ozone_conn_id.strip()

    def execute(self, context):
        source_use = self.source_path.strip() if self.source_path else self.source_path
        dest_use = self.dest_path.strip() if self.dest_path else self.dest_path
        self.log.info("Starting Ozone to Ozone move/rename operation")
        self.log.info("Source path: %s, Destination path: %s", source_use, dest_use)
        self.log.debug("Using connection: %s", self.ozone_conn_id)
        self.log.debug("Note: This is a metadata-only operation, very fast")

        hook = OzoneFsHook(ozone_conn_id=self.ozone_conn_id)

        # Create destination directory if it doesn't exist (for directory moves)
        if "/" in dest_use.rstrip("/"):
            dest_dir = "/".join(dest_use.rstrip("/").split("/")[:-1])
            if dest_dir and not hook.exists(dest_dir):
                self.log.info("Creating destination directory: %s", dest_dir)
                hook.mkdir(dest_dir)

        # Handle wildcard patterns in source_path
        if "*" in source_use:
            # List files matching the pattern
            source_dir = source_use.rsplit("*", 1)[0].rstrip("/")
            if not source_dir:
                source_dir = "/"

            self.log.debug("Wildcard pattern detected, listing files in: %s", source_dir)
            try:
                files = hook.list_paths(source_dir)
                if not files:
                    self.log.info("No files found to move in %s, skipping move operation", source_dir)
                    return

                self.log.info("Found %d file(s) to move", len(files))
                # Move each file individually
                for file_path in files:
                    # Extract filename from full path
                    filename = file_path.split("/")[-1]
                    dest_file_path = f"{dest_use.rstrip('/')}/{filename}"
                    self.log.debug("Moving: %s -> %s", file_path, dest_file_path)
                    hook.run_cli(["ozone", "fs", "-mv", file_path, dest_file_path])

                self.log.info("Successfully moved %d file(s) to %s", len(files), dest_use)
                return
            except Exception as e:
                self.log.warning("Could not list files in %s: %s", source_dir, str(e))
                # Fall through to single file move

        # Single file/directory move (no wildcard)
        # Check if source exists before moving
        if hook.exists(source_use):
            self.log.debug("Source path exists, proceeding with move operation")
            hook.run_cli(["ozone", "fs", "-mv", source_use, dest_use])
            self.log.info("Successfully completed move operation")
            self.log.info("Moved: %s -> %s", source_use, dest_use)
        else:
            self.log.warning("Source path does not exist: %s, skipping move operation", source_use)
            # Treat as success (idempotent operation - nothing to move)
