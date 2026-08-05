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
"""Tests for DuckDB provider error types and lock classifier."""

from __future__ import annotations

import pytest

from airflow.providers.arenadata.duckdb.utils.errors import (
    DuckDbCliError,
    DuckDbCliErrors,
    DuckDbConfigurationError,
    DuckDbOutputError,
    DuckDbProviderError,
)
from airflow.providers.arenadata.duckdb.version_compat import AirflowException

LOCK_STDERR = (
    'Could not set lock on file "/tmp/test.duckdb": Conflicting lock is held'
)


class TestDuckDbErrorHierarchy:
    """Typed errors inherit from DuckDbProviderError - AirflowException."""

    def test_hierarchy(self) -> None:
        """Configuration, CLI, and output errors share the provider base."""
        assert issubclass(DuckDbProviderError, AirflowException)
        assert issubclass(DuckDbConfigurationError, DuckDbProviderError)
        assert issubclass(DuckDbCliError, DuckDbProviderError)
        assert issubclass(DuckDbOutputError, DuckDbProviderError)


class TestDuckDbCliErrorsLockClassifier:
    """Allowlist lock classifier (is_lock_failure)."""

    def test_lock_stderr_is_lock_failure(self) -> None:
        """Lock stderr with allowlist markers is classified as lock failure."""
        assert DuckDbCliErrors.is_lock_failure(LOCK_STDERR) is True

    @pytest.mark.parametrize(
        "stderr",
        [
            "Could not set lock on file",
            "Conflicting lock is held",
            "COULD NOT SET LOCK ON FILE",
        ],
    )
    def test_lock_markers_case_insensitive(self, stderr: str) -> None:
        """Marker match is case-insensitive."""
        assert DuckDbCliErrors.is_lock_failure(stderr) is True

    @pytest.mark.parametrize(
        "stderr",
        [
            None,
            "",
            "Catalog Error: Table does not exist",
            "Parser Error: syntax error",
            "database is locked",  # intentionally not in allowlist
            "Transaction conflict: cannot update a table that has been altered!",
        ],
    )
    def test_non_lock_stderr_is_not_lock_failure(self, stderr: str | None) -> None:
        """Non-lock CLI errors must not be classified as lock failures."""
        assert DuckDbCliErrors.is_lock_failure(stderr) is False


class TestDuckDbCliErrorLockConflict:
    """Clear lock-conflict message factory."""

    def test_for_lock_conflict_message(self) -> None:
        """Lock error includes conn_id, db_path, and remediation hints."""
        err = DuckDbCliError.for_lock_conflict(
            duckdb_conn_id="duckdb_team_a",
            db_path="/data/app.duckdb",
            stderr=LOCK_STDERR,
            returncode=1,
        )
        assert isinstance(err, DuckDbCliError)
        assert err.retryable is True
        assert err.returncode == 1
        assert err.stderr == LOCK_STDERR
        message = str(err)
        assert "duckdb_conn_id=duckdb_team_a" in message
        assert "db_path=/data/app.duckdb" in message
        assert "lock_retry_attempts" in message
        assert "Airflow pool" in message
