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
"""Tests for DuckDB SQL script utilities."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from airflow.providers.arenadata.duckdb.utils.sql_script import (
    bind_sql_parameters,
    serialize_sql_value,
    write_sql_script,
)


class TestSerializeSqlValue:
    """Test SQL literal serialization."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, "NULL"),
            (True, "TRUE"),
            (False, "FALSE"),
            (42, "42"),
            (3.14, "3.14"),
            ("hello", "'hello'"),
            ("it's fine", "'it''s fine'"),
            (date(2024, 1, 15), "DATE '2024-01-15'"),
            (datetime(2024, 1, 15, 10, 30, 0), "TIMESTAMP '2024-01-15 10:30:00'"),
            (
                datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
                "TIMESTAMPTZ '2024-01-15 10:30:00+00:00'",
            ),
            (Decimal("123.45"), "123.45"),
        ],
    )
    def test_serialize(self, value: object, expected: str) -> None:
        """Supported Python values map to DuckDB SQL literals."""
        assert serialize_sql_value(value) == expected

    def test_non_finite_float_raises(self) -> None:
        """Non-finite float parameters raise ValueError."""
        with pytest.raises(ValueError, match="Non-finite float parameter"):
            serialize_sql_value(float("inf"))
        with pytest.raises(ValueError, match="Non-finite float parameter"):
            serialize_sql_value(float("nan"))

    def test_unsupported_type_raises(self) -> None:
        """Unsupported parameter types raise TypeError."""
        with pytest.raises(TypeError, match="Unsupported parameter type"):
            serialize_sql_value({"bad": "dict"})


class TestBindSqlParameters:
    """Test %(name)s placeholder binding."""

    def test_no_parameters_returns_sql_unchanged(self) -> None:
        """None parameters leave SQL unchanged."""
        sql = "SELECT %(x)s"
        assert bind_sql_parameters(sql, None) == sql

    def test_substitutes_named_placeholders(self) -> None:
        """Placeholders are replaced with serialized literals."""
        sql = "SELECT * FROM t WHERE id = %(id)s AND name = %(name)s"
        result = bind_sql_parameters(sql, {"id": 1, "name": "alpha"})
        assert result == "SELECT * FROM t WHERE id = 1 AND name = 'alpha'"

    def test_injection_attempt_is_escaped(self) -> None:
        """String values are escaped to prevent SQL injection."""
        sql = "SELECT %(payload)s"
        result = bind_sql_parameters(sql, {"payload": "'; DROP TABLE t; --"})
        assert result == "SELECT '''; DROP TABLE t; --'"

    def test_missing_placeholder_raises(self) -> None:
        """Missing parameter keys raise ValueError."""
        with pytest.raises(ValueError, match="Missing SQL parameter: 'missing'"):
            bind_sql_parameters("SELECT %(missing)s", {})


class TestWriteSqlScript:
    """Test temporary SQL script file creation."""

    def test_yields_file_with_content(self) -> None:
        """SQL is written to a temp file with UTF-8 encoding."""
        with write_sql_script("SELECT 'кириллица'") as path:
            assert path.name == "query.sql"
            assert path.read_text(encoding="utf-8") == "SELECT 'кириллица'\n"
        assert not path.exists()

    def test_cleans_up_on_exception(self) -> None:
        """Temp directory is removed when an exception is raised."""
        saved: Path | None = None

        def _raise_inside_context() -> None:
            nonlocal saved
            with write_sql_script("SELECT 1") as path:
                saved = path
                raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            _raise_inside_context()
        assert saved is not None
        assert not saved.exists()
