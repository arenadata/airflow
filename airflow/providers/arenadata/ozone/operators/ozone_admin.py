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

from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone_admin import OzoneAdminHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OzoneCreateVolumeOperator(BaseOperator):
    """Create an Ozone Volume using Native Admin CLI."""

    def __init__(
        self,
        volume_name: str,
        quota: str | None = None,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not volume_name or not volume_name.strip():
            raise ValueError("volume_name parameter cannot be empty")
        if quota is not None and (not quota.strip() if isinstance(quota, str) else False):
            raise ValueError("quota parameter cannot be an empty string (use None for unlimited)")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        # Template fields must be assigned directly from __init__ args
        self.volume_name = volume_name
        self.quota = quota.strip() if quota and isinstance(quota, str) else quota
        self.ozone_conn_id = ozone_conn_id

        self.log.debug(
            "Initializing OzoneCreateVolumeOperator - volume: %s, quota: %s, connection: %s",
            self.volume_name,
            self.quota or "unlimited",
            self.ozone_conn_id,
        )

    def execute(self, context: Context):
        self.log.info("Starting volume creation operation")
        self.log.debug("Volume name: %s", self.volume_name)
        self.log.debug("Quota: %s", self.quota or "unlimited")
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneAdminHook(ozone_conn_id=self.ozone_conn_id)
        hook.create_volume(self.volume_name, self.quota)

        self.log.info(
            "Successfully created/verified volume: %s (quota: %s)",
            self.volume_name,
            self.quota or "unlimited",
        )


class OzoneCreateBucketOperator(BaseOperator):
    """Create an Ozone Bucket using Native Admin CLI."""

    def __init__(
        self,
        volume_name: str,
        bucket_name: str,
        quota: str | None = None,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not volume_name or not volume_name.strip():
            raise ValueError("volume_name parameter cannot be empty")
        if not bucket_name or not bucket_name.strip():
            raise ValueError("bucket_name parameter cannot be empty")
        if quota is not None and (not quota.strip() if isinstance(quota, str) else False):
            raise ValueError("quota parameter cannot be an empty string (use None for unlimited)")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        # Template fields must be assigned directly from __init__ args
        self.volume_name = volume_name
        self.bucket_name = bucket_name
        self.quota = quota.strip() if quota and isinstance(quota, str) else quota
        self.ozone_conn_id = ozone_conn_id

        self.log.debug(
            "Initializing OzoneCreateBucketOperator - volume: %s, bucket: %s, quota: %s, connection: %s",
            self.volume_name,
            self.bucket_name,
            self.quota or "none",
            self.ozone_conn_id,
        )

    def execute(self, context: Context):
        self.log.info("Starting bucket creation operation")
        self.log.debug(
            "Volume: %s, Bucket: %s, Quota: %s", self.volume_name, self.bucket_name, self.quota or "none"
        )
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneAdminHook(ozone_conn_id=self.ozone_conn_id)
        hook.create_bucket(self.volume_name, self.bucket_name, self.quota)

        self.log.info(
            "Successfully created bucket: %s in volume: %s (quota: %s)",
            self.bucket_name,
            self.volume_name,
            self.quota or "none",
        )


class OzoneSetQuotaOperator(BaseOperator):
    """Dynamically adjust volume or bucket quotas."""

    def __init__(
        self,
        volume: str,
        quota: str,
        bucket: str | None = None,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not volume or not volume.strip():
            raise ValueError("volume parameter cannot be empty")
        if not quota or not quota.strip():
            raise ValueError("quota parameter cannot be empty")
        if bucket is not None and (not bucket.strip() if isinstance(bucket, str) else False):
            raise ValueError("bucket parameter cannot be an empty string (use None for volume quota)")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        # Template fields must be assigned directly from __init__ args
        self.volume = volume
        self.bucket = bucket.strip() if bucket and isinstance(bucket, str) else bucket
        self.quota = quota.strip()
        self.ozone_conn_id = ozone_conn_id

        target_type = "bucket" if self.bucket else "volume"
        target_name = f"{self.volume}/{self.bucket}" if self.bucket else self.volume
        self.log.debug(
            "Initializing OzoneSetQuotaOperator - target: %s (%s), quota: %s, connection: %s",
            target_name,
            target_type,
            self.quota,
            self.ozone_conn_id,
        )

    def execute(self, context):
        target = f"/{self.volume}" if not self.bucket else f"/{self.volume}/{self.bucket}"
        cmd_type = "volume" if not self.bucket else "bucket"

        self.log.info("Starting quota update operation")
        self.log.debug("Target: %s (%s)", target, cmd_type)
        self.log.debug("New quota: %s", self.quota)
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneAdminHook(ozone_conn_id=self.ozone_conn_id)
        hook.run_cli(["ozone", "sh", cmd_type, "setquota", target, "--quota", self.quota])

        self.log.info("Successfully updated quota for %s %s to %s", cmd_type, target, self.quota)


class OzoneDeleteVolumeOperator(BaseOperator):
    """Delete an Ozone Volume using Native Admin CLI."""

    def __init__(
        self,
        volume_name: str,
        recursive: bool = False,
        force: bool = False,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not volume_name or not volume_name.strip():
            raise ValueError("volume_name parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        if force and not recursive:
            raise ValueError("force=True requires recursive=True")
        # Template fields must be assigned directly from __init__ args
        self.volume_name = volume_name
        self.recursive = recursive
        self.force = force
        self.ozone_conn_id = ozone_conn_id

        self.log.debug(
            "Initializing OzoneDeleteVolumeOperator - volume: %s, recursive: %s, force: %s, connection: %s",
            self.volume_name,
            self.recursive,
            self.force,
            self.ozone_conn_id,
        )

    def execute(self, context: Context):
        self.log.info("Starting volume deletion operation")
        self.log.debug("Volume name: %s", self.volume_name)
        self.log.debug("Recursive: %s, Force: %s", self.recursive, self.force)
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneAdminHook(ozone_conn_id=self.ozone_conn_id)
        hook.delete_volume(self.volume_name, self.recursive, self.force)

        self.log.info("Successfully deleted/verified volume deletion: %s", self.volume_name)


class OzoneDeleteBucketOperator(BaseOperator):
    """Delete an Ozone Bucket using Native Admin CLI."""

    def __init__(
        self,
        volume_name: str,
        bucket_name: str,
        recursive: bool = False,
        force: bool = False,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not volume_name or not volume_name.strip():
            raise ValueError("volume_name parameter cannot be empty")
        if not bucket_name or not bucket_name.strip():
            raise ValueError("bucket_name parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        if force and not recursive:
            raise ValueError("force=True requires recursive=True")
        # Template fields must be assigned directly from __init__ args
        self.volume_name = volume_name
        self.bucket_name = bucket_name
        self.recursive = recursive
        self.force = force
        self.ozone_conn_id = ozone_conn_id

        self.log.debug(
            "Initializing OzoneDeleteBucketOperator - volume: %s, bucket: %s, recursive: %s, force: %s, connection: %s",
            self.volume_name,
            self.bucket_name,
            self.recursive,
            self.force,
            self.ozone_conn_id,
        )

    def execute(self, context: Context):
        self.log.info("Starting bucket deletion operation")
        self.log.debug("Volume: %s, Bucket: %s", self.volume_name, self.bucket_name)
        self.log.debug("Recursive: %s, Force: %s", self.recursive, self.force)
        self.log.debug("Using connection: %s", self.ozone_conn_id)

        hook = OzoneAdminHook(ozone_conn_id=self.ozone_conn_id)
        hook.delete_bucket(self.volume_name, self.bucket_name, self.recursive, self.force)

        self.log.info(
            "Successfully deleted/verified bucket deletion: %s in volume: %s",
            self.bucket_name,
            self.volume_name,
        )
