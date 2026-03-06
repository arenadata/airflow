#
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
import re
from typing import Sequence
from urllib.parse import urlsplit

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.hooks.ozone import (
    FAST_TIMEOUT_SECONDS,
    RETRY_ATTEMPTS,
    OzoneFsHook,
)
from airflow.providers.arenadata.ozone.hooks.ozone_s3 import OzoneS3Hook
from airflow.providers.arenadata.ozone.utils.errors import OzoneCliError
from airflow.providers.arenadata.ozone.utils.helpers import URIHelper
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context  # noqa: TCH001


class OzoneKeySensor(BaseSensorOperator):
    """
    Wait for a file or directory (key) to exist on Ozone FS (ofs:// or o3fs://).

    Uses OzoneFsHook; retryable CLI errors are treated as "not yet" and trigger retry.
    """

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
        self.log.debug("OzoneKeySensor initialized (path=%s, conn_id=%s)", self.path, self.ozone_conn_id)

    def poke(self, context: Context) -> bool:
        """Return True when the target path appears in Ozone."""
        self.log.debug("Checking if key exists in Ozone: %s", self.path)
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        try:
            if hook.exists(self.path, timeout=self.timeout):
                self.log.info("Key found in Ozone: %s", self.path)
                return True
            return False
        except OzoneCliError as e:
            if e.retryable:
                self.log.warning("Retryable error while checking key existence (will retry): %s", str(e))
                return False
            raise


class OzoneS3KeySensor(BaseSensorOperator):
    """Waits for a key (or keys) to be present in an Ozone S3 bucket."""

    template_fields: Sequence[str] = ("bucket_key", "bucket_name")

    def __init__(
        self,
        *,
        bucket_key: str | list[str],
        bucket_name: str | None = None,
        ozone_conn_id: str = OzoneS3Hook.default_conn_name,
        wildcard_match: bool = False,
        use_regex: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.ozone_conn_id = ozone_conn_id
        self.wildcard_match = wildcard_match
        self.use_regex = use_regex

    def _parse_bucket_and_key(self, key: str) -> tuple[str, str]:
        """Split full s3:// URL or combine plain key with bucket_name."""
        bucket_name = self.bucket_name
        key_use = key

        if not bucket_name:
            parsed = urlsplit(key)
            if parsed.scheme != "s3":
                raise AirflowException(
                    "bucket_name is required when bucket_key is not a full s3://bucket/key URL."
                )
            bucket_name = parsed.netloc
            key_use = parsed.path.lstrip("/")
            if not bucket_name or not key_use:
                raise AirflowException(
                    f"Invalid s3 URL for OzoneS3KeySensor: {key!r}. Expected s3://bucket/key."
                )

        return bucket_name, key_use

    def _check_key(self, key: str, hook: OzoneS3Hook) -> bool:
        """Check key existence with exact, wildcard, or regex matching."""
        bucket_name, key_use = self._parse_bucket_and_key(key)

        if not self.wildcard_match and not self.use_regex and URIHelper.contains_wildcards(key_use):
            self.log.warning(
                "bucket_key %r contains wildcard characters, but wildcard_match=False and use_regex=False. "
                "Key will be treated as literal. If you expect glob matching, set wildcard_match=True.",
                key_use,
            )

        if self.wildcard_match:
            prefix = re.split(r"[\[*?]", key_use, 1)[0]
            files = hook.get_file_metadata(prefix, bucket_name)
            return bool(fnmatch.filter([f["Key"] for f in files], key_use))

        if self.use_regex:
            meta_split = re.split(r"[\\\[\]\^\$\*\+\?\|\(\)]", key_use, 1)
            prefix = meta_split[0] if meta_split else ""
            files = hook.get_file_metadata(prefix, bucket_name)
            try:
                pattern = re.compile(key_use)
            except re.error as e:
                raise AirflowException(f"Invalid regex pattern for bucket_key: {key_use!r} ({e})")
            return any(pattern.match(f["Key"]) for f in files)

        return hook.check_for_key(key_use, bucket_name)

    def poke(self, context: Context) -> bool:
        """Return True when all requested S3 keys are found."""
        hook = OzoneS3Hook(ozone_conn_id=self.ozone_conn_id)
        if isinstance(self.bucket_key, str):
            result = self._check_key(self.bucket_key, hook)
        else:
            result = all(self._check_key(k, hook) for k in self.bucket_key)
        if result:
            self.log.info("S3 key(s) found in Ozone: %s", self.bucket_key)
        else:
            self.log.debug("S3 key(s) not found yet in Ozone: %s", self.bucket_key)
        return result
