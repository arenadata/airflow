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

from airflow.providers.arenadata.ozone.hooks.ozone import OzoneCliTransientError
from airflow.providers.arenadata.ozone.hooks.ozone_fs import OzoneFsHook
from airflow.sensors.base import BaseSensorOperator


class OzoneKeySensor(BaseSensorOperator):
    """
    Wait for a file or directory (key) to exist on Ozone FS (ofs:// or o3fs://).

    Uses OzoneFsHook; transient CLI errors are treated as "not yet" and trigger retry.
    """

    template_fields = ("path",)

    def __init__(self, path: str, ozone_conn_id: str = OzoneFsHook.default_conn_name, **kwargs):
        super().__init__(**kwargs)
        if not path or not path.strip():
            raise ValueError("path parameter cannot be empty")
        if not ozone_conn_id or not ozone_conn_id.strip():
            raise ValueError("ozone_conn_id parameter cannot be empty")
        self.path = path.strip()
        self.ozone_conn_id = ozone_conn_id.strip()
        self.log.debug("OzoneKeySensor initialized (path=%s, conn_id=%s)", self.path, self.ozone_conn_id)

    def poke(self, context):
        self.log.debug("Checking if key exists in Ozone: %s", self.path)
        hook = OzoneFsHook(self.ozone_conn_id)
        try:
            if hook.exists(self.path):
                self.log.info("Key found in Ozone: %s", self.path)
                return True
            return False
        except OzoneCliTransientError as e:
            self.log.warning("Transient error while checking key existence (will retry): %s", str(e))
            return False
        except Exception as e:
            # Unexpected exceptions (e.g., FileNotFoundError for missing ozone CLI)
            # should be re-raised as they indicate configuration issues
            self.log.error("Unexpected error while checking key existence: %s (error: %s)", self.path, str(e))
            raise
