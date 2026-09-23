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
"""Parse DuckDB CLI ``-json`` output, including salvage after log noise."""

from __future__ import annotations

import json
import logging
from typing import Any

from airflow.providers.arenadata.duckdb.utils.errors import DuckDbOutputError
from airflow.providers.arenadata.duckdb.version_compat import redact

log = logging.getLogger(__name__)


def parse_json_output(output: str) -> list[Any]:
    """
    Parse CLI ``-json`` output; salvage JSON after optional log prefix. Must be a list.

    Intended for a single statement (one JSON array). Trailing data after the first
    decoded value is discarded with a warning.
    """
    raw_output = (output or "").strip()
    if not raw_output:
        raise DuckDbOutputError("Empty JSON output.")

    parsed: Any
    try:
        parsed = json.loads(raw_output)
    except json.JSONDecodeError:
        parsed = _salvage_json(raw_output)

    if not isinstance(parsed, list):
        raise DuckDbOutputError(
            f"DuckDB JSON output must be a list of rows, got {type(parsed).__name__}: {redact(raw_output)}"
        )
    return parsed


def _salvage_json(raw_output: str) -> Any:
    """Scan for the first JSON array/object and decode it; log discarded prefix."""
    decoder = json.JSONDecoder()
    for index, char in enumerate(raw_output):
        if char not in "[{":
            continue
        try:
            parsed, end = decoder.raw_decode(raw_output, idx=index)
        except json.JSONDecodeError:
            continue
        if index > 0:
            log.info(
                "Discarded non-JSON prefix from DuckDB output: %s",
                redact(raw_output[:index].strip()),
            )
        remainder = raw_output[end:].strip()
        if remainder:
            log.warning(
                "Discarded trailing data after first JSON value in DuckDB output: %s",
                redact(remainder),
            )
        return parsed
    raise DuckDbOutputError(f"Failed to parse JSON output: {redact(raw_output)}")
