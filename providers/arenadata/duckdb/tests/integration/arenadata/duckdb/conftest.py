# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from airflow.models import Connection

# CLI installed by scripts/ci/docker-compose/integration-duckdb.yml into host-mounted /files
DUCKDB_BINARY = "/files/bin/duckdb"

DuckDbConnFactory = Callable[..., tuple[str, Path]]


@pytest.fixture
def duckdb_conn_factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DuckDbConnFactory:
    """Build a unique AIRFLOW_CONN_* Connection; optional extra overrides (timeout, lock_retry, …)."""

    def _factory(**extra_overrides: Any) -> tuple[str, Path]:
        conn_id = f"duckdb_it_{uuid.uuid4().hex[:12]}"
        db_path = tmp_path / f"{conn_id}.duckdb"
        extra: dict[str, Any] = {
            "duckdb_binary": DUCKDB_BINARY,
            "timeout": 60,
            **extra_overrides,
        }
        conn = Connection(
            conn_id=conn_id,
            conn_type="duckdb",
            host=str(db_path),
            extra=extra,
        )
        monkeypatch.setenv(f"AIRFLOW_CONN_{conn_id.upper()}", conn.as_json())
        return conn_id, db_path

    return _factory


@pytest.fixture
def duckdb_conn_id(duckdb_conn_factory: DuckDbConnFactory) -> str:
    """Default connection (timeout=60) for simple roundtrip tests."""
    conn_id, _ = duckdb_conn_factory()
    return conn_id
