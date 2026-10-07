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

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

from fsspec.implementations.webhdfs import WebHDFS
from fsspec.implementations.webhdfs import WebHDFile
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

schemes = ["webhdfs"]


class _OzoneWebHDFile:
    """
    Overrides WebHDFile upload methods to work with Ozone HttpFS

    Standard WebHDFS upload is a two-step process:
      1. _initiate_upload: POST to create the file, server responds with a redirect URL
      2. _upload_chunk: PUT to the redirect URL with the actual data

    Ozone HttpFS does not support this redirect-based protocol
    Instead, it requires a single PUT request with op=CREATE&data=true
    directly to the file URL, with the data in the request body
    https://ozone.apache.org/docs/2.1.2/user-guide/client-interfaces/httpfs#upload-a-file
    """

    def _initiate_upload(self):
        # Ozone does not use a two-step upload, skip initiation
        pass

    def _upload_chunk(self, final=False):
        # Single PUT with data=true as required by Ozone HttpFS
        log.info("_OzoneWebHDFile._upload_chunk: writing to %s", self.path)
        data = self.buffer.getvalue()
        params = {"op": "CREATE", "data": "true", "overwrite": "true"}
        params.update(self.fs.pars)
        out = self.fs.session.put(
            self.fs.url + self.path,
            params=params,
            data=data,
            headers={"content-type": "application/octet-stream"},
        )
        out.raise_for_status()
        return True


def _make_ozone_webhdfs(base_fs):
    # Patch WebHDFile class directly so all instances get Ozone-compatible upload behaviour

    WebHDFile._initiate_upload = _OzoneWebHDFile._initiate_upload
    WebHDFile._upload_chunk = _OzoneWebHDFile._upload_chunk
    return base_fs


def get_fs(conn_id: str | None, storage_options: dict[str, str] | None = None) -> AbstractFileSystem:

    if conn_id is None:
        return WebHDFS()

    conn = BaseHook.get_connection(conn_id)
    options = {
        "host": conn.host,
        "port": conn.port,
        "user": conn.login or None,
        **(conn.extra_dejson or {}),
    }
    options.update(storage_options or {})

    return _make_ozone_webhdfs(WebHDFS(**options))
