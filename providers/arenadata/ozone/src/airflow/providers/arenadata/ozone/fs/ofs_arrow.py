from __future__ import annotations

from upath.implementations.cloud import CloudPath
from upath.registry import register_implementation
from urllib.parse import urlsplit

import pyarrow.fs as pafs
from fsspec.implementations.arrow import ArrowFSWrapper

schemes = ["ozonearrow"]


class OzoneArrowFileSystem(ArrowFSWrapper):
    def __init__(self, conn_id: str | None = None, **kwargs):
        kwargs.pop("conn_id", None)
        super().__init__(fs=pafs.HadoopFileSystem("default"), **kwargs)

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, str):
            if path.startswith("ozonearrow://"):
                path = path[len("ozonearrow://"):]
            if "@" in path.split("/")[0]:
                path = path.split("@", 1)[1]
        return path.lstrip("/")



class _OzoneArrowPath(CloudPath):
    __slots__ = ()

    @property
    def path(self) -> str:
        url = urlsplit(str(self))
        _, _, host = url.netloc.rpartition("@")
        path = f"{host}{url.path}" if host else url.path or ""
        return path.lstrip("/")


register_implementation("ozonearrow", _OzoneArrowPath, clobber=True)


def get_fs(conn_id: str | None, storage_options: dict | None = None) -> OzoneArrowFileSystem:
    return OzoneArrowFileSystem(conn_id=conn_id)
