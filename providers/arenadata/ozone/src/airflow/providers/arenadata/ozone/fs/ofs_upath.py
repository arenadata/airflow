from __future__ import annotations

import fsspec
from upath.implementations.cloud import CloudPath
from upath.registry import register_implementation
from urllib.parse import urlsplit

from airflow.providers.arenadata.ozone.fs.ofs import OzoneFsFileSystem

schemes = ["ozoneofs"]

fsspec.register_implementation("ozoneofs", OzoneFsFileSystem, clobber=True)


class _OzoneOfsPath(CloudPath):
    __slots__ = ()

    @property
    def path(self) -> str:
        # str(self) = ozoneofs://[conn_id@]volume/bucket/key
        # We need /volume/bucket/key for ozone fs commands.
        url = urlsplit(str(self))
        _, _, host = url.netloc.rpartition("@")
        return f"/{host}{url.path}" if host else url.path or "/"


register_implementation("ozoneofs", _OzoneOfsPath, clobber=True)


def get_fs(conn_id: str | None, storage_options: dict | None = None) -> OzoneFsFileSystem:
    return OzoneFsFileSystem(conn_id=conn_id, **(storage_options or {}))
