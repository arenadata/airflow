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
from typing import TYPE_CHECKING, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.hooks.ozone_s3 import OzoneS3Hook
from airflow.providers.arenadata.ozone.utils.ozone_path import contains_wildcards
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OzoneS3KeySensor(BaseSensorOperator):
    """Waits for a key (or keys) to be present in an Ozone S3 bucket."""

    template_fields: Sequence[str] = ("bucket_key", "bucket_name", "ozone_conn_id")

    def __init__(
        self,
        *,
        bucket_key: str | list[str],
        bucket_name: str | None = None,
        ozone_conn_id: str = OzoneS3Hook.default_conn_name,
        wildcard_match: bool = False,
        use_regex: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.ozone_conn_id = ozone_conn_id
        self.wildcard_match = wildcard_match
        self.use_regex = use_regex
        self.deferrable = deferrable

        self.log.debug(
            "Initializing OzoneS3KeySensor - bucket: %s, key: %s, connection: %s",
            bucket_name,
            bucket_key,
            ozone_conn_id,
        )

    def _parse_bucket_and_key(self, key: str) -> tuple[str, str]:
        """
        Derive bucket and key from inputs.

        If bucket_name is not set, allow full s3://bucket/key syntax in bucket_key.
        Otherwise, require bucket_name to be provided explicitly (mirrors S3KeySensor).
        """
        bucket_name = self.bucket_name
        key_use = key

        if not bucket_name:
            if key.startswith("s3://"):
                # Parse s3://bucket/key
                without_scheme = key[len("s3://") :]
                parts = without_scheme.split("/", 1)
                if len(parts) != 2 or not parts[0] or not parts[1]:
                    raise AirflowException(
                        f"Invalid s3 URL for OzoneS3KeySensor: {key!r}. Expected s3://bucket/key."
                    )
                bucket_name, key_use = parts[0], parts[1]
            else:
                raise AirflowException(
                    "bucket_name is required when bucket_key is not a full s3://bucket/key URL."
                )

        return bucket_name, key_use

    def _check_key(self, key: str, context: Context) -> bool:
        bucket_name, key_use = self._parse_bucket_and_key(key)
        self.log.info("Poking for key: s3://%s/%s", bucket_name, key_use)
        hook = OzoneS3Hook(ozone_conn_id=self.ozone_conn_id)

        # Warn if user passed wildcard-like key but wildcard_match/use_regex is off
        if not self.wildcard_match and not self.use_regex and contains_wildcards(key_use):
            self.log.warning(
                "bucket_key %r contains wildcard characters, but wildcard_match=False and use_regex=False. "
                "Key will be treated as literal. If you expect glob matching, set wildcard_match=True.",
                key_use,
            )

        if self.wildcard_match:
            prefix = re.split(r"[\[*?]", key_use, 1)[0]
            files = hook.get_file_metadata(prefix, bucket_name)
            return any(fnmatch.fnmatch(f["Key"], key_use) for f in files)

        if self.use_regex:
            # Use a prefix up to the first regex meta-character to limit listing scope.
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
        if isinstance(self.bucket_key, str):
            result = self._check_key(self.bucket_key, context)
        else:
            result = all(self._check_key(k, context) for k in self.bucket_key)
        if result:
            self.log.info("S3 key(s) found in Ozone: %s", self.bucket_key)
        else:
            self.log.debug("S3 key(s) not found yet in Ozone: %s", self.bucket_key)
        return result
