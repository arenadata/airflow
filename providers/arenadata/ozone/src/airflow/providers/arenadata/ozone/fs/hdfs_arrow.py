from __future__ import annotations

from fsspec.implementations.arrow import HadoopFileSystem

from airflow.hooks.base import BaseHook

schemes = ["hdfs"]


def get_fs(conn_id: str | None, storage_options: dict | None = None) -> HadoopFileSystem:
    conn = BaseHook.get_connection(conn_id)
    return HadoopFileSystem(host=conn.host, port=conn.port or 8020)
