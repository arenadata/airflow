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
"""Tests for DuckDB CLI JSON output parsing / salvage."""

from __future__ import annotations

import pytest

from airflow.providers.arenadata.duckdb.utils.errors import DuckDbOutputError
from airflow.providers.arenadata.duckdb.utils.json_output import parse_json_output


class TestParseJsonOutput:
    """parse_json_output: strict loads, salvage, list-only contract."""

    def test_plain_json_array(self) -> None:
        """Valid JSON array of row objects is returned as-is."""
        assert parse_json_output('[{"n":1},{"n":2}]') == [{"n": 1}, {"n": 2}]

    def test_empty_array(self) -> None:
        """Empty JSON array is a valid empty result set."""
        assert parse_json_output("[]") == []

    def test_salvage_after_log_prefix(self, caplog: pytest.LogCaptureFixture) -> None:
        """ADO/wrapper noise before JSON is discarded; array is salvaged."""
        raw = 'log: starting duckdb\n[{"ready": true}]'
        with caplog.at_level("INFO"):
            assert parse_json_output(raw) == [{"ready": True}]
        assert "Discarded non-JSON prefix" in caplog.text

    def test_salvage_object_root_rejected(self) -> None:
        """Non-list JSON root fails with DuckDbOutputError (sensor contract)."""
        with pytest.raises(DuckDbOutputError, match="must be a list"):
            parse_json_output('{"ready": true}')

    def test_empty_output_raises(self) -> None:
        """Empty stdout is not silently treated as rows (sensor handles empty)."""
        with pytest.raises(DuckDbOutputError, match="Empty JSON output"):
            parse_json_output("")
        with pytest.raises(DuckDbOutputError, match="Empty JSON output"):
            parse_json_output("   \n")

    def test_unparseable_raises(self) -> None:
        """Garbage without JSON payload raises DuckDbOutputError."""
        with pytest.raises(DuckDbOutputError, match="Failed to parse JSON"):
            parse_json_output("not json at all")
