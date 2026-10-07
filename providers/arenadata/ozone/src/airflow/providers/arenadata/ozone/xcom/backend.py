from __future__ import annotations

import json
import logging
import re
import uuid
from typing import Any, TypeVar

log = logging.getLogger(__name__)

from airflow.utils.json import XComDecoder, XComEncoder
from airflow.providers.common.io.xcom.backend import (
    XComObjectStorageBackend,
    _get_base_path,
    _get_compression,
    _get_compression_suffix,
    _get_threshold,
)
from airflow.providers.common.io.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.bases.xcom import BaseXCom
else:
    from airflow.models.xcom import BaseXCom  # type: ignore[no-redef]

T = TypeVar("T")

_UNSAFE = re.compile(r"[^\w.\-]")


def _safe(value: str | None) -> str | None:
    return _UNSAFE.sub("_", value) if value is not None else None


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
        s_val = json.dumps(value, cls=XComEncoder)
        s_val_encoded = s_val.encode("utf-8")

        if compression := _get_compression():
            suffix = f".{_get_compression_suffix(compression)}"
        else:
            suffix = ""

        threshold = _get_threshold()
        if threshold < 0 or len(s_val_encoded) < threshold:
            if AIRFLOW_V_3_0_PLUS:
                return BaseXCom.serialize_value(value)
            return s_val_encoded

        base_path = _get_base_path()
        log.info("XComOzoneBackend v2: writing to %s", base_path)
        while True:
            p = base_path.joinpath(
                _safe(dag_id) or "NO_DAG_ID",
                _safe(run_id) or "NO_RUN_ID",
                _safe(task_id) or "NO_TASK_ID",
                f"{uuid.uuid4()}{suffix}",
            )
            if not p.exists():
                break
        p.parent.mkdir(parents=True, exist_ok=True)

        with p.open(mode="wb", compression=compression) as f:
            f.write(s_val_encoded)
        return BaseXCom.serialize_value(str(p))
