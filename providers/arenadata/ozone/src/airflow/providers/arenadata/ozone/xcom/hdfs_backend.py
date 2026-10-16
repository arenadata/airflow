from __future__ import annotations

import json
import logging
import re

log = logging.getLogger(__name__)

import airflow.providers.arenadata.ozone.fs.ozone_s3  # noqa: F401 — registers ozone:// scheme
import airflow.providers.arenadata.ozone.fs.ofs_upath  # noqa: F401 — registers ozoneofs:// scheme
import airflow.providers.arenadata.ozone.fs.ofs_arrow  # noqa: F401 — registers ozonearrow:// scheme
import airflow.providers.arenadata.ozone.fs.hdfs_arrow  # noqa: F401 — registers hdfsarrow:// scheme
from airflow.providers.common.io.xcom.backend import XComObjectStorageBackend
from airflow.utils.json import XComDecoder
from airflow.providers.common.io.xcom.backend import _get_base_path
from airflow.sdk import ObjectStoragePath

_UNSAFE = re.compile(r"[^\w.\-]")


def _safe(value: str | None) -> str | None:
    return _UNSAFE.sub("_", value) if value is not None else None


class XComHdfsBackend(XComObjectStorageBackend):
    @staticmethod
    def serialize_value(value, *, key=None, task_id=None, dag_id=None, run_id=None, map_index=None):
        base_path = _get_base_path()
        log.info("XCom serialize: backend=%s dag=%s task=%s key=%s", base_path.protocol, dag_id, task_id, key)
        return XComObjectStorageBackend.serialize_value(
            value,
            key=key,
            task_id=task_id,
            dag_id=dag_id,
            run_id=_safe(run_id),
            map_index=map_index,
        )

    @staticmethod
    def _normalize_uri(raw: str) -> str:
        if "://" not in raw:
            raw = raw.replace(":/", "://", 1)
        # normalize double slashes in path part (after scheme://authority)
        scheme, rest = raw.split("://", 1)
        authority, _, path = rest.partition("/")
        path = re.sub(r"/+", "/", "/" + path)
        return f"{scheme}://{authority}{path}"

    @staticmethod
    def deserialize_value(result) -> object:
        raw = result.value if hasattr(result, "value") else result
        if not isinstance(raw, str):
            return raw
        normalized = XComHdfsBackend._normalize_uri(raw)
        base_normalized = XComHdfsBackend._normalize_uri(str(_get_base_path()))
        if not normalized.startswith(base_normalized):
            raise ValueError(f"XCom path {normalized!r} is not under base {base_normalized!r}")
        path = ObjectStoragePath(normalized)
        with path.open(mode="rb", compression="infer") as f:
            return json.load(f, cls=XComDecoder)

    @staticmethod
    def purge(xcom, session=None) -> None:
        raw = xcom.value if hasattr(xcom, "value") else xcom
        if not isinstance(raw, str):
            return
        normalized = XComHdfsBackend._normalize_uri(raw)
        ObjectStoragePath(normalized).unlink(missing_ok=True)
