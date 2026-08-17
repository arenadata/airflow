#
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
"""Helpers for DuckDB SQL script preparation."""

from __future__ import annotations

import math
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

SQL_QUERY_FILENAME = "query.sql"

PLACEHOLDER_PATTERN = re.compile(r"%\((\w+)\)s")


def serialize_sql_value(value: Any) -> str:
    """Convert a Python value into a DuckDB SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Non-finite float parameter: {value!r}")
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        literal = value.isoformat(sep=" ")
        sql_type = "TIMESTAMPTZ" if value.tzinfo is not None else "TIMESTAMP"
        return f"{sql_type} '{literal}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(
        f"Unsupported parameter type {type(value)!r}; "
        "expected str, int, float, Decimal, bool, None, date, or datetime"
    )


def bind_sql_parameters(sql: str, parameters: dict[str, Any] | None) -> str:
    """
    Replace ``%(name)s`` placeholders with serialized SQL literals.

    Substitution is textual: patterns that look like placeholders inside SQL
    string literals (for example ``LIKE '%(abc)s'``) are still replaced when
    *parameters* is non-empty.

    :raises ValueError: when a placeholder has no matching key in *parameters*.
    """
    if parameters is None:
        return sql

    def _replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in parameters:
            raise ValueError(f"Missing SQL parameter: {name!r}")
        return serialize_sql_value(parameters[name])

    return PLACEHOLDER_PATTERN.sub(_replace, sql)


@contextmanager
def write_sql_script(sql: str) -> Iterator[Path]:
    """
    Write *sql* to a private temp directory and yield the file path.

    The directory is removed on exit regardless of success or failure.
    """
    with TemporaryDirectory(prefix="airflow_duckdb_") as tmp_dir:
        script_path = Path(tmp_dir) / SQL_QUERY_FILENAME
        script_path.write_text(
            sql if sql.endswith("\n") else sql + "\n",
            encoding="utf-8",
        )
        yield script_path
