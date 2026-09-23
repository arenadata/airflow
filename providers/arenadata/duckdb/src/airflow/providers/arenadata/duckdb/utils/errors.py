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
"""Typed exceptions and CLI error classification for the DuckDB provider."""

from __future__ import annotations

from airflow.providers.arenadata.duckdb.version_compat import AirflowException

# File-lock markers from DuckDB CLI stderr (allowlist)
LOCK_FAILURE_MARKERS = (
    "could not set lock on file",
    "conflicting lock is held",
)


class DuckDbProviderError(AirflowException):
    """Base exception for DuckDB provider runtime errors."""


class DuckDbConfigurationError(DuckDbProviderError):
    """Invalid connection/operator configuration (preflight, ban-list, bind, coercion)."""


class DuckDbCliError(DuckDbProviderError):
    """DuckDB CLI execution error."""

    def __init__(
        self,
        message: str,
        *,
        command: list[str] | None = None,
        stderr: str | None = None,
        returncode: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.command = command
        self.stderr = stderr
        self.returncode = returncode
        self.retryable = retryable

    @classmethod
    def for_lock_conflict(
        cls,
        *,
        duckdb_conn_id: str,
        db_path: str,
        command: list[str] | None = None,
        stderr: str | None = None,
        returncode: int | None = None,
    ) -> DuckDbCliError:
        """Build a clear lock-conflict error (retryable marker; hook decides whether to retry)."""
        message = (
            f"DuckDB database file is locked by another process "
            f"(duckdb_conn_id={duckdb_conn_id}, db_path={db_path}). "
            "Use an Airflow pool with one slot for this file, separate .duckdb files, "
            "or set lock_retry_attempts > 0."
        )
        return cls(
            message,
            command=command,
            stderr=stderr,
            returncode=returncode,
            retryable=True,
        )


class DuckDbOutputError(DuckDbProviderError):
    """DuckDB CLI returned output that could not be parsed as expected JSON."""


class DuckDbCliErrors:
    """Allowlist classifier for DuckDB file-lock failures."""

    @classmethod
    def is_lock_failure(cls, stderr: str | None) -> bool:
        """Return True when stderr matches known DuckDB file-lock markers."""
        normalized = (stderr or "").lower()
        return any(marker in normalized for marker in LOCK_FAILURE_MARKERS)
