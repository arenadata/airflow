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
from airflow.providers.arenadata.ozone.utils.helpers import (
    TypeNormalizationHelper,
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
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.set_quota(
            volume=self.volume,
            quota=self.quota,
            bucket=self.bucket,
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


class OzoneCreatePathOperator(BaseOperator):
    """Create directory path via FS interface."""

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
        hook.create_path(self.path, timeout=self.timeout)


class OzoneUploadContentOperator(BaseOperator):
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
            hook.upload_key(
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

    def execute(self, context: Context):
        """Delete a single key or a wildcard-matched group of keys."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.delete_key(self.path, timeout=self.timeout)


class OzoneDeletePathOperator(BaseOperator):
    """Delete file/directory path from Ozone FS."""

    template_fields = ("path",)

    def __init__(
        self,
        path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        recursive: bool = True,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.path = path
        self.ozone_conn_id = ozone_conn_id
        self.recursive = recursive
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context):
        """Delete file/directory path with optional recursive mode."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.delete_path(self.path, recursive=self.recursive, timeout=self.timeout)


class OzonePathExistsOperator(BaseOperator):
    """Check whether Ozone path exists and return boolean via XCom."""

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

    def execute(self, context: Context) -> bool:
        """Return True when path exists, False otherwise."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        return hook.path_exists(self.path, timeout=self.timeout)


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

    def execute(self, context: Context) -> list[str]:
        """Return a list of keys, optionally filtered by wildcard pattern."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        return hook.list_keys(self.path, timeout=self.timeout)


class OzoneUploadFileOperator(BaseOperator):
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

        hook.upload_key(
            str(local_path_obj),
            self.remote_path,
            timeout=self.timeout,
        )


class OzoneMoveOperator(BaseOperator):
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

    def execute(self, context: Context):
        """Move one key or a wildcard-selected batch within Ozone."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.move(self.source_path, self.dest_path, timeout=self.timeout)


class OzoneCopyOperator(BaseOperator):
    """Copy one key or wildcard-selected keys within Ozone."""

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

    def execute(self, context: Context):
        """Copy one key or a wildcard-selected batch within Ozone."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.copy_path(self.source_path, self.dest_path, timeout=self.timeout)


class OzoneDownloadFileOperator(BaseOperator):
    """Download a file from Ozone to local filesystem."""

    template_fields = ("remote_path", "local_path")

    def __init__(
        self,
        *,
        remote_path: str,
        local_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        overwrite: bool = False,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.remote_path = remote_path
        self.local_path = local_path
        self.ozone_conn_id = ozone_conn_id
        self.overwrite = overwrite
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context):
        """Download remote file to local path."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.download_key(
            self.remote_path,
            self.local_path,
            overwrite=self.overwrite,
            timeout=self.timeout,
        )


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
