from __future__ import annotations

import json
import logging
import re
import uuid
from typing import Any, TypeVar

from airflow.providers.common.io.xcom.backend import XComObjectStorageBackend
from airflow.providers.common.io.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.json import XComDecoder

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.bases.xcom import BaseXCom
else:
    from airflow.models.xcom import BaseXCom  # type: ignore[no-redef]

log = logging.getLogger(__name__)

T = TypeVar("T")

_CONN_ID = "ozone_webhdfs_default"
_BASE_PATH = "/vol1/bucket-legacy/xcom"
_XCOM_PATH_PREFIX = f"webhdfs://{_CONN_ID}@{_BASE_PATH}"

_UNSAFE = re.compile(r"[^\w.\-]")


def _safe(value: str | None) -> str | None:
    return _UNSAFE.sub("_", value) if value is not None else None


def _get_fs():
    from airflow.providers.arenadata.ozone.fs.webhdfs import get_fs
    return get_fs(_CONN_ID)


class XComOzoneBackend(XComObjectStorageBackend):
    @staticmethod
    def serialize_value(
        value: T,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> bytes | str:
        s_val_encoded = json.dumps(value).encode("utf-8")

        path = "/".join([
            _BASE_PATH,
            _safe(dag_id) or "NO_DAG_ID",
            _safe(run_id) or "NO_RUN_ID",
            _safe(task_id) or "NO_TASK_ID",
            str(uuid.uuid4()),
        ])

        log.info("XComOzoneBackend: writing to %s", path)
        fs = _get_fs()
        with fs.open(path, mode="wb") as f:
            f.write(s_val_encoded)

        return BaseXCom.serialize_value(f"{_XCOM_PATH_PREFIX}/{path[len(_BASE_PATH):].lstrip('/')}")

    @staticmethod
    def deserialize_value(result) -> Any:
        base_xcom_deser_result = BaseXCom.deserialize_value(result)
        if not isinstance(base_xcom_deser_result, str) or not base_xcom_deser_result.startswith("webhdfs://"):
            return base_xcom_deser_result
        # Extract HDFS path from webhdfs://conn_id@/path
        try:
            from urllib.parse import urlsplit
            url = urlsplit(base_xcom_deser_result)
            hdfs_path = url.path
            fs = _get_fs()
            with fs.open(hdfs_path, mode="rb") as f:
                return json.load(f, cls=XComDecoder)
        except Exception:
            log.exception("XComOzoneBackend: failed to deserialize %s", base_xcom_deser_result)
            return base_xcom_deser_result
