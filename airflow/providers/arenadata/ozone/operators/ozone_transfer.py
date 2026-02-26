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
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone_fs import OzoneFsHook
from airflow.providers.arenadata.ozone.utils.ozone_path import (
    build_ozone_uri,
    contains_wildcards,
    parse_ozone_uri,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LocalFilesystemToOzoneOperator(BaseOperator):
    """
    Upload a file from the local filesystem (Airflow Worker) to Ozone.

    Wraps 'ozone fs -put'.
    """

    template_fields = ("local_path", "remote_path", "ozone_conn_id")

    def __init__(
        self,
        local_path: str,
        remote_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        overwrite: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        # Template fields must be assigned directly from __init__ args
        self.local_path = local_path
        self.remote_path = remote_path
        self.ozone_conn_id = ozone_conn_id
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

    template_fields = ("source_path", "dest_path", "ozone_conn_id")

    def __init__(
        self,
        *,
        source_path: str,
        dest_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        # Template fields must be assigned directly from __init__ args
        self.source_path = source_path
        self.dest_path = dest_path
        self.ozone_conn_id = ozone_conn_id

    def execute(self, context):
        source_use = self.source_path.strip() if self.source_path else self.source_path
        dest_use = self.dest_path.strip() if self.dest_path else self.dest_path
        self.log.info("Starting Ozone to Ozone move/rename operation")
        self.log.info("Source path: %s, Destination path: %s", source_use, dest_use)
        self.log.debug("Using connection: %s", self.ozone_conn_id)
        self.log.debug("Note: This is a metadata-only operation, very fast")

        hook = OzoneFsHook(ozone_conn_id=self.ozone_conn_id)

        if contains_wildcards(source_use):
            src_scheme, src_netloc, src_path = parse_ozone_uri(source_use)
            pattern = posixpath.basename(src_path)
            src_dir_path = posixpath.dirname(src_path) or ("/" if src_scheme else "")
            src_dir = build_ozone_uri(src_scheme, src_netloc, src_dir_path or "/")

            self.log.debug("Wildcard pattern detected, listing files in: %s (pattern: %s)", src_dir, pattern)
            try:
                files = hook.list_paths(src_dir)
            except Exception as e:
                self.log.warning("Could not list files in %s: %s", src_dir, str(e))
                dest_dir = dest_use.rstrip("/")
                if dest_dir and not hook.exists(dest_dir):
                    self.log.info("Creating destination directory: %s", dest_dir)
                    hook.mkdir(dest_dir)
                hook.run_cli(["ozone", "fs", "-mv", source_use, dest_use])
                self.log.info("Successfully completed move operation (wildcard fallback)")
                return

            if not files:
                self.log.info("No files found to move in %s, skipping move operation", src_dir)
                return

            matched = [p for p in files if fnmatch.fnmatch(posixpath.basename(p), pattern)]
            if not matched:
                self.log.info("No files matching pattern %s in %s, skipping move operation", pattern, src_dir)
                return

            dest_dir = dest_use.rstrip("/")
            if dest_dir and not hook.exists(dest_dir):
                self.log.info("Creating destination directory: %s", dest_dir)
                hook.mkdir(dest_dir)

            dst_scheme, dst_netloc, dst_path = parse_ozone_uri(dest_dir.rstrip("/"))
            dst_base = dst_path.rstrip("/") or ("/" if dst_scheme else "")

            self.log.info("Found %d file(s) matching %s to move", len(matched), pattern)
            for file_path in matched:
                filename = posixpath.basename(file_path.rstrip("/"))
                dest_file_path = build_ozone_uri(dst_scheme, dst_netloc, posixpath.join(dst_base, filename))
                self.log.debug("Moving: %s -> %s", file_path, dest_file_path)
                hook.run_cli(["ozone", "fs", "-mv", file_path, dest_file_path])

            self.log.info("Successfully moved %d file(s) to %s", len(matched), dest_use)
            return

        dst_scheme, dst_netloc, dst_path = parse_ozone_uri(dest_use.rstrip("/"))
        dst_parent_path = posixpath.dirname(dst_path) if dst_path else ""
        if dst_scheme and dst_parent_path in ("", "/"):
            dst_parent_path = ""  # do not mkdir root
        dst_parent = build_ozone_uri(dst_scheme, dst_netloc, dst_parent_path) if dst_parent_path else ""

        if dst_parent and not hook.exists(dst_parent):
            self.log.info("Creating destination directory: %s", dst_parent)
            hook.mkdir(dst_parent)

        if hook.exists(source_use):
            self.log.debug("Source path exists, proceeding with move operation")
            hook.run_cli(["ozone", "fs", "-mv", source_use, dest_use])
            self.log.info("Successfully completed move operation")
            self.log.info("Moved: %s -> %s", source_use, dest_use)
        else:
            self.log.warning("Source path does not exist: %s, skipping move operation", source_use)
