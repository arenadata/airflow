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

import io
import json
import posixpath
import tempfile
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone import (
    FAST_TIMEOUT_SECONDS,
    RETRY_ATTEMPTS,
    SLOW_TIMEOUT_SECONDS,
    OzoneAdminHook,
    OzoneFsHook,
)
from airflow.providers.arenadata.ozone.hooks.ozone_s3 import OzoneS3Hook
from airflow.providers.arenadata.ozone.utils.errors import OzoneCliError
from airflow.providers.arenadata.ozone.utils.helpers import (
    TypeNormalizationHelper,
    URIHelper,
)
from airflow.utils.context import Context  # noqa: TCH001


class OzoneCreateVolumeOperator(BaseOperator):
    """Create an Ozone Volume using Native Admin CLI."""

    def __init__(
        self,
        volume_name: str,
        quota: str | None = None,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.volume_name = volume_name
        self.quota = TypeNormalizationHelper.require_optional_non_empty(
            quota, "quota parameter cannot be an empty string (use None for unlimited)"
        )
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context):
        """Create the target volume if it does not exist."""
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.create_volume(self.volume_name, self.quota, timeout=self.timeout)


class OzoneCreateBucketOperator(BaseOperator):
    """Create an Ozone Bucket using Native Admin CLI."""

    def __init__(
        self,
        volume_name: str,
        bucket_name: str,
        quota: str | None = None,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.volume_name = volume_name
        self.bucket_name = bucket_name
        self.quota = TypeNormalizationHelper.require_optional_non_empty(
            quota, "quota parameter cannot be an empty string (use None for unlimited)"
        )
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context):
        """Create the target bucket if it does not exist."""
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.create_bucket(
            self.volume_name,
            self.bucket_name,
            self.quota,
            timeout=self.timeout,
        )


class OzoneSetQuotaOperator(BaseOperator):
    """Dynamically adjust volume or bucket quotas."""

    def __init__(
        self,
        volume: str,
        quota: str,
        bucket: str | None = None,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = FAST_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.volume = volume
        self.quota = quota
        self.bucket = TypeNormalizationHelper.require_optional_non_empty(
            bucket, "bucket parameter cannot be an empty string (use None for volume quota)"
        )
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context):
        """Apply a quota to a volume or bucket."""
        target = f"/{self.volume}" if not self.bucket else f"/{self.volume}/{self.bucket}"
        cmd_type = "volume" if not self.bucket else "bucket"
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.run_cli(
            ["ozone", "sh", cmd_type, "setquota", target, "--quota", self.quota],
            timeout=self.timeout,
        )


class OzoneDeleteVolumeOperator(BaseOperator):
    """Delete an Ozone Volume using Native Admin CLI."""

    def __init__(
        self,
        volume_name: str,
        recursive: bool = False,
        force: bool = False,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.volume_name = volume_name
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        if force and not recursive:
            raise ValueError("force=True requires recursive=True")
        self.recursive = recursive
        self.force = force

    def execute(self, context: Context):
        """Delete a volume with optional recursive mode."""
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.delete_volume(
            self.volume_name,
            self.recursive,
            self.force,
            timeout=self.timeout,
        )


class OzoneDeleteBucketOperator(BaseOperator):
    """Delete an Ozone Bucket using Native Admin CLI."""

    def __init__(
        self,
        volume_name: str,
        bucket_name: str,
        recursive: bool = False,
        force: bool = False,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.volume_name = volume_name
        self.bucket_name = bucket_name
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        if force and not recursive:
            raise ValueError("force=True requires recursive=True")
        self.recursive = recursive
        self.force = force

    def execute(self, context: Context):
        """Delete a bucket with optional recursive mode."""
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.delete_bucket(
            self.volume_name,
            self.bucket_name,
            self.recursive,
            self.force,
            timeout=self.timeout,
        )


class OzoneFsMkdirOperator(BaseOperator):
    """Create directory via FS interface."""

    template_fields = ("path",)

    def __init__(
        self,
        path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = FAST_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.path = path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context):
        """Create a directory path in Ozone FS."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.mkdir(self.path, timeout=self.timeout)


class OzoneFsPutOperator(BaseOperator):
    """Put content string to a file via FS interface."""

    def __init__(
        self,
        content: str,
        remote_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.remote_path = remote_path
        self.ozone_conn_id = ozone_conn_id
        self.content = content
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context):
        """Write string content to a temporary file and upload it."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        with tempfile.TemporaryDirectory(prefix="ozone_fs_put_") as tmp_dir:
            tmp_path = Path(tmp_dir) / "payload.txt"
            tmp_path.write_text(self.content, encoding="utf-8")
            hook.copy_from_local(
                str(tmp_path),
                self.remote_path,
                timeout=self.timeout,
            )


class OzoneDeleteKeyOperator(BaseOperator):
    """Deletes a key (file) from Ozone FS."""

    template_fields = ("path",)

    def __init__(
        self,
        path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.path = path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def _list_wildcard_matches(self, hook: OzoneFsHook, path_use: str) -> list[str] | None:
        """Return wildcard matches or None when caller should fallback to direct CLI delete."""
        try:
            source_dir, matched = URIHelper.resolve_wildcard_matches(
                path_use,
                lambda p: hook.list_paths(p, timeout=self.timeout),
            )
        except OzoneCliError as err:
            if not err.retryable:
                raise
            source_dir, _ = URIHelper.split_ozone_wildcard_path(path_use)
            self.log.warning(
                "Could not list wildcard source directory %s (%s); falling back to direct delete for %s",
                source_dir,
                type(err).__name__,
                path_use,
            )
            return None

        return matched

    def execute(self, context: Context):
        """Delete a single key or a wildcard-matched group of keys."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )

        if not URIHelper.contains_wildcards(self.path):
            if hook.exists(self.path, timeout=self.timeout):
                hook.run_cli(["ozone", "fs", "-rm", self.path], timeout=self.timeout)
            return

        matched = self._list_wildcard_matches(hook, self.path)
        if matched is None:
            hook.run_cli(["ozone", "fs", "-rm", self.path], timeout=self.timeout)
            return

        for file_path in matched:
            hook.run_cli(
                ["ozone", "fs", "-rm", file_path],
                timeout=self.timeout,
            )


class OzoneListOperator(BaseOperator):
    """Lists keys/prefixes in an Ozone path and returns them via XCom."""

    template_fields = ("path",)

    def __init__(
        self,
        *,
        path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = FAST_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.path = path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def _list_with_wildcard(self, hook: OzoneFsHook, path_use: str) -> list[str]:
        """List and filter keys by wildcard pattern with CLI fallback."""
        try:
            source_dir, matched = URIHelper.resolve_wildcard_matches(
                path_use,
                lambda p: hook.list_paths(p, timeout=self.timeout),
            )
        except OzoneCliError as err:
            if not err.retryable:
                raise
            source_dir, _ = URIHelper.split_ozone_wildcard_path(path_use)
            self.log.warning(
                "Could not list wildcard source directory %s (%s); falling back to listing %s",
                source_dir,
                type(err).__name__,
                path_use,
            )
            return hook.list_paths(path_use, timeout=self.timeout)

        return matched

    def execute(self, context: Context) -> list[str]:
        """Return a list of keys, optionally filtered by wildcard pattern."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )

        if URIHelper.contains_wildcards(self.path):
            return self._list_with_wildcard(hook, self.path)

        return hook.list_paths(self.path, timeout=self.timeout)


class OzoneS3CreateBucketOperator(BaseOperator):
    """Create a bucket via S3 Gateway."""

    template_fields = ("bucket_name",)

    def __init__(self, bucket_name: str, ozone_conn_id: str = OzoneS3Hook.default_conn_name, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.ozone_conn_id = ozone_conn_id

    def execute(self, context: Context):
        """Create a bucket through the S3 gateway hook."""
        hook = OzoneS3Hook(ozone_conn_id=self.ozone_conn_id)
        hook.create_bucket_with_retry(bucket_name=self.bucket_name)


class OzoneS3PutObjectOperator(BaseOperator):
    """
    Put object via S3 Gateway.

    ``data`` supports ``str``, ``bytes``, and JSON-serializable objects.
    Strings are uploaded as-is, bytes are uploaded as binary streams, and
    other objects are JSON-serialized before upload.
    """

    template_fields = ("bucket_name", "key", "data")

    def __init__(
        self,
        bucket_name: str,
        key: str,
        data: object,
        ozone_conn_id: str = OzoneS3Hook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.key = key
        self.ozone_conn_id = ozone_conn_id
        if data is None:
            raise ValueError("data parameter cannot be None")
        self.data = data

    def execute(self, context: Context):
        """Upload object content through the S3 gateway hook."""
        hook = OzoneS3Hook(ozone_conn_id=self.ozone_conn_id)
        if isinstance(self.data, bytes):
            data_stream = io.BytesIO(self.data)
            hook.load_file_obj_with_retry(
                file_obj=data_stream, key=self.key, bucket_name=self.bucket_name, replace=True
            )
            return

        if isinstance(self.data, str):
            payload = self.data
        else:
            try:
                payload = json.dumps(self.data)
            except (TypeError, ValueError) as e:
                raise AirflowException(
                    "data must be str, bytes, or a JSON-serializable object "
                    f"(got {type(self.data).__name__})"
                ) from e

        hook.load_string_with_retry(
            string_data=payload, key=self.key, bucket_name=self.bucket_name, replace=True
        )


class LocalFilesystemToOzoneOperator(BaseOperator):
    """Upload a file from the local filesystem to Ozone."""

    template_fields = ("local_path", "remote_path")

    def __init__(
        self,
        local_path: str,
        remote_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        overwrite: bool = False,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.local_path = local_path
        self.remote_path = remote_path
        self.ozone_conn_id = ozone_conn_id
        self.overwrite = overwrite
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context):
        """Upload a local file to Ozone, with optional overwrite."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        local_path_obj = Path(self.local_path)
        if not local_path_obj.exists():
            raise AirflowException(f"Local file not found: {self.local_path}")

        if hook.exists(self.remote_path, timeout=self.timeout) and not self.overwrite:
            raise AirflowException(f"Remote path {self.remote_path} already exists and overwrite is False")

        hook.copy_from_local(
            str(local_path_obj),
            self.remote_path,
            timeout=self.timeout,
        )


class OzoneToOzoneOperator(BaseOperator):
    """Move or rename a key within the same Ozone cluster."""

    template_fields = ("source_path", "dest_path")

    def __init__(
        self,
        *,
        source_path: str,
        dest_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_path = source_path
        self.dest_path = dest_path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def _ensure_dest_dir_exists(self, hook: OzoneFsHook, path: str) -> None:
        dest_dir = path.rstrip("/")
        if dest_dir and not hook.exists(dest_dir, timeout=self.timeout):
            hook.mkdir(dest_dir, timeout=self.timeout)

    def _move_with_wildcard(self, hook: OzoneFsHook, source_use: str, dest_use: str) -> None:
        try:
            src_dir, matched = URIHelper.resolve_wildcard_matches(
                source_use,
                lambda p: hook.list_paths(p, timeout=self.timeout),
            )
        except OzoneCliError as err:
            if not err.retryable:
                raise
            src_dir, _ = URIHelper.split_ozone_wildcard_path(source_use)
            self.log.warning(
                "Could not list wildcard source directory %s (%s); falling back to direct move %s -> %s",
                src_dir,
                type(err).__name__,
                source_use,
                dest_use,
            )
            self._ensure_dest_dir_exists(hook, dest_use)
            hook.run_cli(
                ["ozone", "fs", "-mv", source_use, dest_use],
                timeout=self.timeout,
            )
            return

        if not matched:
            return

        self._ensure_dest_dir_exists(hook, dest_use)

        dest_dir = dest_use.rstrip("/")
        dst_scheme, dst_netloc, dst_path = URIHelper.parse_ozone_uri(dest_dir.rstrip("/"))
        dst_base = dst_path.rstrip("/") or ("/" if dst_scheme else "")
        for file_path in matched:
            filename = posixpath.basename(file_path.rstrip("/"))
            dest_file_path = URIHelper.build_ozone_uri(
                dst_scheme, dst_netloc, posixpath.join(dst_base, filename)
            )
            hook.run_cli(
                ["ozone", "fs", "-mv", file_path, dest_file_path],
                timeout=self.timeout,
            )

    def execute(self, context: Context):
        """Move one key or a wildcard-selected batch within Ozone."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )

        if URIHelper.contains_wildcards(self.source_path):
            self._move_with_wildcard(hook, self.source_path, self.dest_path)
            return

        dst_scheme, dst_netloc, dst_path = URIHelper.parse_ozone_uri(self.dest_path.rstrip("/"))
        dst_parent_path = posixpath.dirname(dst_path) if dst_path else ""
        if dst_scheme and dst_parent_path in ("", "/"):
            dst_parent_path = ""
        dst_parent = (
            URIHelper.build_ozone_uri(dst_scheme, dst_netloc, dst_parent_path) if dst_parent_path else ""
        )

        if dst_parent and not hook.exists(dst_parent, timeout=self.timeout):
            hook.mkdir(dst_parent, timeout=self.timeout)

        if hook.exists(self.source_path, timeout=self.timeout):
            hook.run_cli(
                ["ozone", "fs", "-mv", self.source_path, self.dest_path],
                timeout=self.timeout,
            )
