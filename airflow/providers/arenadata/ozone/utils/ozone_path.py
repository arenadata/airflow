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
Small utilities for working with Ozone-style URIs.

CHANGED: extracted from operators so all URI handling stays consistent.
"""

from __future__ import annotations

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
