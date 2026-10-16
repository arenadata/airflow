from __future__ import annotations

from typing import TYPE_CHECKING

import fsspec
import s3fs
from upath.implementations.cloud import CloudPath
from upath.registry import register_implementation

from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

schemes = ["ozone"]

# Register s3fs under the "ozone" scheme in fsspec
fsspec.register_implementation("ozone", s3fs.S3FileSystem, clobber=True)

# Register in upath so that ObjectStoragePath("ozone://...") parses bucket/key correctly
class _OzonePath(CloudPath):
    __slots__ = ()

    @property
    def path(self) -> str:
        # Strip "ozone://" prefix so s3fs receives "bucket/key" not "ozone://bucket/key"
        p = super().path
        prefix = f"{self.protocol}://"
        return p[len(prefix):] if p.startswith(prefix) else p

register_implementation("ozone", _OzonePath, clobber=True)


def get_fs(conn_id: str | None, storage_options: dict | None = None) -> AbstractFileSystem:
    opts = storage_options or {}

    if conn_id is not None:
        conn = BaseHook.get_connection(conn_id)
        host = conn.host or "ozone"
        port = conn.port or 9878
        opts.setdefault("endpoint_url", f"http://{host}:{port}")
        if conn.login:
            opts.setdefault("key", conn.login)
        if conn.password:
            opts.setdefault("secret", conn.password)

    opts.setdefault("endpoint_url", "http://ozone:9878")
    opts.setdefault("key", "anykey")
    opts.setdefault("secret", "anysecret")
    opts.setdefault("use_ssl", False)

    return s3fs.S3FileSystem(**opts)
