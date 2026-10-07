from __future__ import annotations

import re

from airflow.providers.common.io.xcom.backend import XComObjectStorageBackend

_UNSAFE = re.compile(r"[^\w.\-]")


def _safe(value: str | None) -> str | None:
    return _UNSAFE.sub("_", value) if value is not None else None


class XComHdfsBackend(XComObjectStorageBackend):
    @staticmethod
    def serialize_value(value, *, key=None, task_id=None, dag_id=None, run_id=None, map_index=None):
        return XComObjectStorageBackend.serialize_value(
            value,
            key=key,
            task_id=task_id,
            dag_id=dag_id,
            run_id=_safe(run_id),
            map_index=map_index,
        )
