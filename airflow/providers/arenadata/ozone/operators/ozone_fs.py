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

import fnmatch
import os
import posixpath
import tempfile
from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone_fs import OzoneFsHook
from airflow.providers.arenadata.ozone.utils.ozone_path import (
    build_ozone_uri,
    contains_wildcards,
    parse_ozone_uri,
)

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

        # Wildcard-aware delete: list parent dir + fnmatch filter
        if contains_wildcards(path_use):
            scheme, netloc, uri_path = parse_ozone_uri(path_use)
            pattern = posixpath.basename(uri_path)
            source_dir_path = posixpath.dirname(uri_path) or ("/" if scheme else "")
            source_dir = build_ozone_uri(scheme, netloc, source_dir_path or "/")

            self.log.debug(
                "Wildcard pattern detected, listing files in: %s (pattern: %s)", source_dir, pattern
            )
            try:
                files = hook.list_paths(source_dir)
            except Exception as e:
                self.log.warning("Could not list files in %s: %s", source_dir, str(e))
                # Fallback to CLI delete with pattern (avoid silent skip)
                self.log.info("Falling back to CLI deletion for pattern: %s", path_use)
                hook.run_cli(["ozone", "fs", "-rm", path_use])
                self.log.info("Successfully completed deletion operation (wildcard fallback)")
                return
            else:
                if not files:
                    self.log.info("No files found to delete in %s, skipping deletion operation", source_dir)
                    return  # Idempotent success

                matched = [p for p in files if fnmatch.fnmatch(posixpath.basename(p), pattern)]
                if not matched:
                    self.log.info(
                        "No files matching pattern %s in %s, skipping deletion operation", pattern, source_dir
                    )
                    return  # Idempotent success

                self.log.info("Found %d file(s) to delete (pattern: %s)", len(matched), pattern)
                for file_path in matched:
                    self.log.debug("Deleting: %s", file_path)
                    hook.run_cli(["ozone", "fs", "-rm", file_path])

                self.log.info("Successfully deleted %d file(s) from %s", len(matched), source_dir)
                return

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

    Supports wildcard patterns in the last path segment (e.g., ``ofs://om/landing/raw/*.csv``,
    ``ofs://vol1/bucket1/data/file_??.parquet``, ``ofs://vol1/bucket1/data/[ab]*.txt``).

    Note: Wildcard patterns in the middle of the path (e.g., ``ofs://vol1/bucket1/*/part-*.parquet``)
    are not supported. This would require recursive glob traversal, which is not implemented.

    :param path: The Ozone path to list (e.g., ``ofs://vol1/bucket1/data/`` or ``ofs://om/landing/raw/*.csv``).
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
        path_use = self.path.strip() if self.path else self.path

        # Wildcard-aware list: list parent dir + fnmatch filter on basename
        if contains_wildcards(path_use):
            scheme, netloc, uri_path = parse_ozone_uri(path_use)
            pattern = posixpath.basename(uri_path)
            source_dir_path = posixpath.dirname(uri_path) or ("/" if scheme else "")
            source_dir = build_ozone_uri(scheme, netloc, source_dir_path or "/")

            self.log.debug("Wildcard pattern detected, listing in: %s (pattern: %s)", source_dir, pattern)

            try:
                keys = hook.list_paths(source_dir)
            except Exception as e:
                # Fallback to raw list if listing parent dir failed
                self.log.warning(
                    "Could not list paths in %s: %s. Falling back to direct list(%s)", source_dir, e, path_use
                )
                keys = hook.list_paths(path_use)
            else:
                keys = [k for k in keys if fnmatch.fnmatch(posixpath.basename(k), pattern)]

            self.log.info("Wildcard listing completed - found %d key(s)", len(keys))
            if keys:
                self.log.debug("First few keys: %s", keys[:5])
                if len(keys) > 5:
                    self.log.debug("... and %d more keys", len(keys) - 5)

            self.log.debug("Returning keys list to XCom for downstream tasks")
            return keys

        # Default listing (no wildcard)
        keys = hook.list_paths(path_use)

        self.log.info("Path listing completed - found %d key(s)", len(keys))
        if keys:
            self.log.debug("First few keys: %s", keys[:5])
            if len(keys) > 5:
                self.log.debug("... and %d more keys", len(keys) - 5)

        self.log.debug("Returning keys list to XCom for downstream tasks")
        return keys
