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

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

schemes = ["webhdfs"]


class _OzoneWebHDFile:
    """Patched WebHDFile that uses a single PUT with op=CREATE&data=true as per Ozone HttpFS docs."""

    def _initiate_upload(self):
        pass

    def _upload_chunk(self, final=False):
        import logging
        log = logging.getLogger(__name__)
        data = self.buffer.getvalue()
        log.warning("[ozone] _upload_chunk data_len=%s", len(data))
        params = {"op": "CREATE", "data": "true", "overwrite": "true"}
        params.update(self.fs.pars)
        out = self.fs.session.put(
            self.fs.url + self.path,
            params=params,
            data=data,
            headers={"content-type": "application/octet-stream"},
        )
        log.warning("[ozone] _upload_chunk response: status=%s body=%s", out.status_code, out.text)
        out.raise_for_status()
        return True


def _make_ozone_webhdfs(base_fs):
    """Patch WebHDFS instance to use _OzoneWebHDFile."""
    original_open = base_fs._open

    def _open(path, mode="rb", **kwargs):
        f = original_open(path, mode=mode, **kwargs)
        if "w" in mode:
            f.__class__ = type("OzoneWebHDFile", (_OzoneWebHDFile, type(f)), {})
        return f

    import types
    base_fs._open = types.MethodType(lambda self, path, mode="rb", **kw: _open(path, mode, **kw), base_fs)
    return base_fs


def get_fs(conn_id: str | None, storage_options: dict[str, str] | None = None) -> AbstractFileSystem:
    from fsspec.implementations.webhdfs import WebHDFS

    if conn_id is None:
        return WebHDFS()

    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)
    options = {
        "host": conn.host,
        "port": conn.port,
        "user": conn.login or None,
        **(conn.extra_dejson or {}),
    }
    options.update(storage_options or {})

    return _make_ozone_webhdfs(WebHDFS(**options))
