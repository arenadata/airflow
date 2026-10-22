from __future__ import annotations

import fsspec
from fsspec import AbstractFileSystem
from upath.implementations.cloud import CloudPath
from upath.registry import register_implementation
from urllib.parse import urlsplit

import pyarrow.fs as pafs
from fsspec.implementations.arrow import ArrowFSWrapper
from airflow.hooks.base import BaseHook

schemes = ["hdfsarrow"]


class HdfsArrowFileSystem(AbstractFileSystem):
    def __init__(self, host: str = "default", port: int = 8020, **kwargs):
        kwargs.pop("conn_id", None)
        super().__init__(**kwargs)
        self._arrow_fs = ArrowFSWrapper(pafs.HadoopFileSystem(host, port=port))

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, str):
            if path.startswith("hdfsarrow://"):
                path = path[len("hdfsarrow://"):]
            if "@" in path.split("/")[0]:
                path = path.split("@", 1)[1]
        return "/" + path.lstrip("/")

    def ls(self, path, detail=True, **kwargs):
        return self._arrow_fs.ls(self._strip_protocol(path), detail=detail, **kwargs)

    def mkdir(self, path, create_parents=True, **kwargs):
        return self._arrow_fs.mkdir(self._strip_protocol(path), create_parents=create_parents, **kwargs)

    def makedirs(self, path, exist_ok=False):
        return self._arrow_fs.makedirs(self._strip_protocol(path), exist_ok=exist_ok)

    def exists(self, path, **kwargs):
        return self._arrow_fs.exists(self._strip_protocol(path), **kwargs)

    def rm(self, path, recursive=False, **kwargs):
        return self._arrow_fs.rm(self._strip_protocol(path), recursive=recursive, **kwargs)

    def open(self, path, mode="rb", **kwargs):
        return self._arrow_fs.open(self._strip_protocol(path), mode=mode, **kwargs)

    def info(self, path, **kwargs):
        return self._arrow_fs.info(self._strip_protocol(path), **kwargs)


fsspec.register_implementation("hdfsarrow", HdfsArrowFileSystem, clobber=True)


class _HdfsArrowPath(CloudPath):
    __slots__ = ()

    @property
    def path(self) -> str:
        url = urlsplit(str(self))
        return url.path or "/"


register_implementation("hdfsarrow", _HdfsArrowPath, clobber=True)


def get_fs(conn_id: str | None, storage_options: dict | None = None) -> HdfsArrowFileSystem:
    conn = BaseHook.get_connection(conn_id)
    return HdfsArrowFileSystem(host=conn.host, port=conn.port or 8020)
