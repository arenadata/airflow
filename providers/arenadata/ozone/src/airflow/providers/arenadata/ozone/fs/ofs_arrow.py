from __future__ import annotations

import fsspec
from fsspec import AbstractFileSystem
from upath.implementations.cloud import CloudPath
from upath.registry import register_implementation
from urllib.parse import urlsplit

schemes = ["ozonearrow"]


class OzoneArrowFileSystem(AbstractFileSystem):
    protocol = "ozonearrow"

    def __init__(self, conn_id: str | None = None, **kwargs):
        kwargs.pop("conn_id", None)
        super().__init__(**kwargs)
        import pyarrow.fs as pafs
        from fsspec.implementations.arrow import ArrowFSWrapper
        self._arrow_fs = ArrowFSWrapper(pafs.HadoopFileSystem("default"))

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, str):
            if path.startswith("ozonearrow://"):
                path = path[len("ozonearrow://"):]
            if "@" in path.split("/")[0]:
                path = path.split("@", 1)[1]
        return path.lstrip("/")

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        return self._arrow_fs.ls(path, detail=detail, **kwargs)

    def mkdir(self, path, create_parents=True, **kwargs):
        path = self._strip_protocol(path)
        return self._arrow_fs.mkdir(path, create_parents=create_parents, **kwargs)

    def makedirs(self, path, exist_ok=False):
        path = self._strip_protocol(path)
        return self._arrow_fs.makedirs(path, exist_ok=exist_ok)

    def exists(self, path, **kwargs):
        path = self._strip_protocol(path)
        return self._arrow_fs.exists(path, **kwargs)

    def rm(self, path, recursive=False, **kwargs):
        path = self._strip_protocol(path)
        return self._arrow_fs.rm(path, recursive=recursive, **kwargs)

    def open(self, path, mode="rb", **kwargs):
        path = self._strip_protocol(path)
        return self._arrow_fs.open(path, mode=mode, **kwargs)

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        return self._arrow_fs.info(path, **kwargs)


fsspec.register_implementation("ozonearrow", OzoneArrowFileSystem, clobber=True)


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
