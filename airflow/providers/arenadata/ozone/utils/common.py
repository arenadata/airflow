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

"""
Common helpers for the Ozone provider.

This module intentionally contains only small generic helpers used across
hooks/operators/sensors to avoid ad-hoc duplication.
"""

from __future__ import annotations

import fnmatch
import os
import subprocess
from collections.abc import Sequence
from urllib.parse import urlsplit, urlunsplit

WILDCARD_CHARS = ("*", "?", "[")


def contains_wildcards(value: str) -> bool:
    """Return True if path contains glob/wildcard characters."""
    return bool(value) and any(ch in value for ch in WILDCARD_CHARS)


def parse_ozone_uri(value: str) -> tuple[str, str, str]:
    """
    Parse Ozone-style URI.

    Returns (scheme, netloc, path_or_raw):
    - ofs://om/a/b -> ('ofs', 'om', '/a/b')
    - /a/b         -> ('', '', '/a/b')
    - a/b          -> ('', '', 'a/b')
    """
    parsed = urlsplit(value or "")
    if parsed.scheme:
        path = parsed.path or "/"
        if not path.startswith("/"):
            path = "/" + path
        return parsed.scheme, parsed.netloc or "", path
    return "", "", value or ""


def build_ozone_uri(scheme: str, netloc: str, path_or_raw: str) -> str:
    """Build Ozone-style URI from components returned by ``parse_ozone_uri``."""
    if not scheme:
        return path_or_raw
    path = path_or_raw or "/"
    if not path.startswith("/"):
        path = "/" + path
    return urlunsplit((scheme, netloc, path, "", ""))


def split_ozone_wildcard_path(path: str) -> tuple[str, str]:
    """
    Split a wildcard Ozone path into source directory URI and basename pattern.

    This helper supports only last-segment wildcard patterns.
    Example:
    - ``ofs://om/vol/bucket/*.csv`` -> (``ofs://om/vol/bucket``, ``*.csv``)
    """
    scheme, netloc, uri_path = parse_ozone_uri(path)
    pattern = os.path.basename(uri_path)
    source_dir_path = os.path.dirname(uri_path) or ("/" if scheme else "")
    source_dir = build_ozone_uri(scheme, netloc, source_dir_path or "/")
    return source_dir, pattern


def filter_paths_by_basename_pattern(paths: Sequence[str], pattern: str) -> list[str]:
    """Filter path list by basename using fnmatch semantics."""
    return [path for path in paths if fnmatch.fnmatch(os.path.basename(path), pattern)]


def require_optional_non_empty(value: object | None, message: str) -> str | None:
    """
    Validate and normalize an optional string parameter.

    Required string parameters are handled by type hints and semantic checks in call sites.
    This helper remains for optional/union inputs where runtime type validation is needed.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(message)
    normalized = value.strip()
    if not normalized:
        raise ValueError(message)
    return normalized


def merge_env_overrides(overrides: dict[str, object] | None) -> dict[str, str]:
    """
    Merge environment overrides into a copy of current process env.

    Values are stringified to keep subprocess env compatible.
    """
    env = os.environ.copy()
    if not overrides:
        return env
    env.update({str(k): str(v) for k, v in overrides.items()})
    return env


def is_true_flag(extra: dict[str, object], *keys: str) -> bool:
    """Return True if any provided key is present and equals 'true' (case-insensitive)."""
    for key in keys:
        value = extra.get(key)
        if isinstance(value, bool) and value:
            return True
        if isinstance(value, str) and value.lower() == "true":
            return True
    return False


def get_connection_extra(conn: object) -> dict[str, object]:
    """Return connection extra as dict in a consistent way."""
    return conn.extra_dejson if hasattr(conn, "extra_dejson") else {}


def run_subprocess(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    env_overrides: dict[str, object] | None = None,
    input_text: str | None = None,
    timeout: int | None = None,
    cwd: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """
    Run a subprocess with consistent env merge and text output.

    This helper centralizes a common pattern used across provider modules.
    """
    if env is not None and env_overrides is not None:
        raise ValueError("Provide either env or env_overrides, not both.")

    effective_env = env if env is not None else merge_env_overrides(env_overrides)

    return subprocess.run(
        cmd,
        env=effective_env,
        check=check,
        capture_output=True,
        text=True,
        input=input_text,
        timeout=timeout,
        cwd=cwd,
    )
