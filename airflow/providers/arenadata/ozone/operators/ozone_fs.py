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
import tempfile
from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone_fs import OzoneFsHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OzoneFsMkdirOperator(BaseOperator):
    """Create directory via FS interface."""

    template_fields = ("path",)

    def __init__(self, path: str, ozone_conn_id: str = OzoneFsHook.default_conn_name, **kwargs):
        super().__init__(**kwargs)
        if not path or not path.strip():
            raise ValueError("path parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        self.path = path
        self.ozone_conn_id = ozone_conn_id.strip()
        self.log.debug(
            "Initializing OzoneFsMkdirOperator - path: %s, connection: %s", self.path, self.ozone_conn_id
        )

    def execute(self, context: Context):
        self.log.info("Starting directory creation operation")
        self.log.debug("Directory path: %s", self.path)
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneFsHook(ozone_conn_id=self.ozone_conn_id)
        hook.mkdir(self.path.strip() if self.path else self.path)

        self.log.info("Successfully created directory: %s", self.path)


class OzoneFsPutOperator(BaseOperator):
    """Put content string to a file via FS interface."""

    def __init__(
        self, content: str, remote_path: str, ozone_conn_id: str = OzoneFsHook.default_conn_name, **kwargs
    ):
        super().__init__(**kwargs)
        if content is None:
            raise ValueError("content parameter cannot be None")
        if not remote_path or not remote_path.strip():
            raise ValueError("remote_path parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        self.content = content
        self.remote_path = remote_path.strip()
        self.ozone_conn_id = ozone_conn_id.strip()

        content_length = len(content) if content else 0
        self.log.debug(
            "Initializing OzoneFsPutOperator - remote_path: %s, content_length: %d bytes, connection: %s",
            self.remote_path,
            content_length,
            self.ozone_conn_id,
        )

    def execute(self, context: Context):
        self.log.info("Starting file upload operation to Ozone FS")
        self.log.debug("Remote path: %s", self.remote_path)
        self.log.debug("Content size: %d bytes", len(self.content) if self.content else 0)

        hook = OzoneFsHook(ozone_conn_id=self.ozone_conn_id)

        self.log.debug("Creating temporary file for content")
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
                tmp.write(self.content)
                tmp_path = tmp.name
                tmp_size = os.path.getsize(tmp_path)
                self.log.debug("Temporary file created: %s (%d bytes)", tmp_path, tmp_size)

            hook.copy_from_local(tmp_path, self.remote_path)
            self.log.info("Successfully uploaded content to Ozone: %s (%d bytes)", self.remote_path, tmp_size)
        finally:
            # Clean up temporary file if it was created
            if tmp_path and os.path.exists(tmp_path):
                self.log.debug("Cleaning up temporary file: %s", tmp_path)
                try:
                    os.remove(tmp_path)
                    self.log.debug("Temporary file removed")
                except OSError as e:
                    self.log.warning("Failed to remove temporary file %s: %s", tmp_path, str(e))


class OzoneDeleteKeyOperator(BaseOperator):
    """Deletes a key (file) from Ozone FS."""

    template_fields = ("path",)

    def __init__(self, path: str, ozone_conn_id: str = OzoneFsHook.default_conn_name, **kwargs):
        super().__init__(**kwargs)
        if not path or not path.strip():
            raise ValueError("path parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        self.path = path
        self.ozone_conn_id = ozone_conn_id.strip()
        self.log.debug(
            "Initializing OzoneDeleteKeyOperator - path: %s, connection: %s", self.path, self.ozone_conn_id
        )

    def execute(self, context):
        path_use = self.path.strip() if self.path else self.path
        self.log.info("Starting key deletion operation")
        self.log.debug("Key path to delete: %s", path_use)
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneFsHook(ozone_conn_id=self.ozone_conn_id)

        # Handle wildcard patterns in path
        if "*" in path_use:
            # List files matching the pattern
            source_dir = path_use.rsplit("*", 1)[0].rstrip("/")
            if not source_dir:
                source_dir = "/"

            self.log.debug("Wildcard pattern detected, listing files in: %s", source_dir)
            try:
                files = hook.list_paths(source_dir)
                if not files:
                    self.log.info("No files found to delete in %s, skipping deletion operation", source_dir)
                    return  # Idempotent: no files to delete is success

                self.log.info("Found %d file(s) to delete", len(files))
                # Delete each file individually
                for file_path in files:
                    self.log.debug("Deleting: %s", file_path)
                    hook.run_cli(["ozone", "fs", "-rm", file_path])

                self.log.info("Successfully deleted %d file(s) from %s", len(files), source_dir)
                return
            except Exception as e:
                self.log.warning("Could not list files in %s: %s", source_dir, str(e))
                # Fall through to single file deletion

        # Single file/directory deletion (no wildcard)
        # Check if path exists before deleting (idempotent operation)
        if hook.exists(path_use):
            self.log.debug("Path exists, proceeding with deletion")
            hook.run_cli(["ozone", "fs", "-rm", path_use])
            self.log.info("Successfully deleted key: %s", path_use)
        else:
            self.log.info("Path does not exist: %s, skipping deletion (idempotent operation)", path_use)
            # Treat as success (idempotent operation - nothing to delete)


class OzoneListOperator(BaseOperator):
    """
    Lists keys/prefixes in an Ozone path and returns them via XCom.

    :param path: The Ozone path to list (e.g., ofs://vol1/bucket1/data/).
    :param ozone_conn_id: The Airflow connection ID to use.
    """

    template_fields = ("path",)
    ui_color = "#C5E1A5"  # A nice green color for UI

    def __init__(self, *, path: str, ozone_conn_id: str = OzoneFsHook.default_conn_name, **kwargs):
        super().__init__(**kwargs)
        if not path or not path.strip():
            raise ValueError("path parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        self.path = path
        self.ozone_conn_id = ozone_conn_id.strip()
        self.log.debug(
            "Initializing OzoneListOperator - path: %s, connection: %s", self.path, self.ozone_conn_id
        )

    def execute(self, context: Any) -> list[str]:
        """
        Run the operator and push the result to XCom.

        This method is called by Airflow and the return value is pushed to XComs.
        """
        self.log.info("Starting path listing operation")
        self.log.debug("Listing path: %s", self.path)
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneFsHook(ozone_conn_id=self.ozone_conn_id)
        keys = hook.list_paths(self.path.strip() if self.path else self.path)

        self.log.info("Path listing completed - found %d key(s)", len(keys))
        if keys:
            self.log.debug("First few keys: %s", keys[:5])
            if len(keys) > 5:
                self.log.debug("... and %d more keys", len(keys) - 5)

        self.log.debug("Returning keys list to XCom for downstream tasks")
        return keys
