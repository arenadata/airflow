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

import fnmatch
import json
import os
import posixpath
import re
from collections.abc import Callable, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

from airflow.exceptions import AirflowException
from airflow.utils.log.secrets_masker import mask_secret, redact


class URIHelper:
    """URI and wildcard helpers for Ozone paths."""

    WILDCARD_CHARS = ("*", "?", "[")

    @classmethod
    def contains_wildcards(cls, value: str) -> bool:
        """Return True if path contains glob or wildcard characters."""
        return bool(value) and any(ch in value for ch in cls.WILDCARD_CHARS)

    @staticmethod
    def parse_ozone_uri(value: str) -> tuple[str, str, str]:
        """Parse Ozone URI and return (scheme, netloc, path_or_raw)."""
        parsed = urlsplit(value or "")
        if parsed.scheme:
            path = parsed.path or "/"
            if not path.startswith("/"):
                path = "/" + path
            return parsed.scheme, parsed.netloc or "", path
        return "", "", value or ""

    @staticmethod
    def build_ozone_uri(scheme: str, netloc: str, path_or_raw: str) -> str:
        """Build Ozone-style URI from components returned by parse_ozone_uri."""
        if not scheme:
            return path_or_raw
        path = path_or_raw or "/"
        if not path.startswith("/"):
            path = "/" + path
        return urlunsplit((scheme, netloc, path, "", ""))

    @classmethod
    def split_ozone_path(cls, value: str) -> tuple[str, str]:
        """
        Split Ozone path into parent directory and basename.

        Returns:
            tuple[parent_path, name]
        """
        target_value = (value or "").rstrip("/")
        if not target_value:
            return "", ""

        scheme, netloc, parsed_path = cls.parse_ozone_uri(target_value)
        parent_raw, name = posixpath.split(parsed_path)

        if not parent_raw:
            return "", name

        if scheme and parent_raw == "/":
            return "", name

        return cls.build_ozone_uri(scheme, netloc, parent_raw), name

    @classmethod
    def join_ozone_path(cls, dir_path: str, name: str) -> str:
        """Join Ozone directory path and child name preserving scheme and netloc."""
        target_dir = (dir_path or "").rstrip("/")
        child_name = (name or "").strip("/")

        if not target_dir:
            return child_name
        if not child_name:
            return target_dir

        scheme, netloc, parsed_path = cls.parse_ozone_uri(target_dir)
        base_path = parsed_path.rstrip("/") or ("/" if scheme else "")
        full_path = posixpath.join(base_path, child_name)
        return cls.build_ozone_uri(scheme, netloc, full_path)

    @staticmethod
    def parse_s3_uri(value: str) -> tuple[str, str]:
        """Parse s3://bucket/key URI and return (bucket_name, key)."""
        parsed = urlsplit(value or "")
        if parsed.scheme != "s3":
            raise AirflowException(f"Invalid S3 URI: {value!r}. Expected s3://bucket/key.")
        bucket_name = parsed.netloc or ""
        key = parsed.path.lstrip("/")
        if not bucket_name or not key:
            raise AirflowException(f"Invalid S3 URI: {value!r}. Expected s3://bucket/key.")
        return bucket_name, key

    @classmethod
    def resolve_s3_bucket_and_key(cls, key: str, bucket_name: str | None = None) -> tuple[str, str]:
        """Return (bucket_name, key), accepting either plain key + bucket_name or full s3:// URI."""
        if bucket_name:
            return bucket_name, key
        return cls.parse_s3_uri(key)

    @classmethod
    def split_ozone_wildcard_path(cls, path: str) -> tuple[str, str]:
        """Split wildcard path into source directory URI and basename pattern."""
        scheme, netloc, uri_path = cls.parse_ozone_uri(path)
        source_dir_path, pattern = posixpath.split(uri_path)
        source_dir_path = source_dir_path or ("/" if scheme else "")
        source_dir = cls.build_ozone_uri(scheme, netloc, source_dir_path or "/")
        return source_dir, pattern

    @staticmethod
    def filter_paths_by_basename_pattern(paths: Sequence[str], pattern: str) -> list[str]:
        """Filter path list by basename using fnmatch semantics."""
        return [path for path in paths if fnmatch.fnmatch(posixpath.basename(path), pattern)]

    @classmethod
    def resolve_wildcard_matches(
        cls,
        wildcard_path: str,
        list_paths_func: Callable[[str], Sequence[str]],
    ) -> tuple[str, list[str]]:
        """Resolve wildcard path into source directory and matching file paths."""
        source_dir, pattern = cls.split_ozone_wildcard_path(wildcard_path)
        listed_paths = list_paths_func(source_dir)
        return source_dir, cls.filter_paths_by_basename_pattern(listed_paths, pattern)


class PatternHelper:
    """Helpers for glob and regex pattern handling."""

    @staticmethod
    def literal_prefix_before_glob(pattern: str) -> str:
        """Return literal prefix before first glob metacharacter."""
        parts = re.split(r"[\[*?]", pattern or "", 1)
        return parts[0] if parts else ""

    @staticmethod
    def literal_prefix_before_regex(pattern: str) -> str:
        """Return literal prefix before first regex metacharacter."""
        parts = re.split(r"[\\\[\]\^\$\*\+\?\|\(\)]", pattern or "", 1)
        return parts[0] if parts else ""


class EnvHelper:
    """Helpers for environment mapping and env variable reads."""

    @staticmethod
    def build_mapped_env(
        extra: Mapping[str, object],
        mapping: Sequence[tuple[str, str, bool]],
        *,
        resolve_secret: Callable[[object], object] | None = None,
    ) -> dict[str, str]:
        """Build environment variables from extra according to a mapping."""
        env: dict[str, str] = {}
        for extra_key, env_key, is_secret in mapping:
            if extra_key not in extra:
                continue
            value: object = extra[extra_key]
            if is_secret:
                if resolve_secret is None:
                    raise ValueError("resolve_secret must be provided for secret-mapped values")
                value = resolve_secret(value)
            env[env_key] = str(value)
        return env

    @staticmethod
    def get_env_str(name: str, default: str | None = None) -> str | None:
        """Get environment variable as string or return default if not set."""
        return TypeNormalizationHelper.normalize_optional_str(os.getenv(name)) or default


class SecretHelper:
    """Helpers for secret-safe handling and connection extra extraction."""

    @staticmethod
    def resolve_secret_masked(value: object, resolve_secret: Callable[[object], object]) -> object:
        """Resolve a secret-like value and register it in Airflow secret masker."""
        resolved = resolve_secret(value)
        mask_secret(resolved)
        return resolved

    @staticmethod
    def get_connection_extra(conn: object) -> dict[str, object]:
        """Return connection extra as dict in a consistent way."""
        return conn.extra_dejson if hasattr(conn, "extra_dejson") else {}


class TypeNormalizationHelper:
    """Normalization and lightweight validation helpers for typed inputs."""

    @staticmethod
    def normalize_optional_str(value: object | None) -> str | None:
        """Return stripped string value or None for empty or None input."""
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @classmethod
    def require_optional_non_empty(cls, value: object | None, message: str) -> str | None:
        """Validate and normalize optional string input."""
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(message)
        normalized = cls.normalize_optional_str(value)
        if not normalized:
            raise ValueError(message)
        return normalized

    @staticmethod
    def parse_int_or_default(value: object, default: int) -> int:
        """Parse int value or return default when parsing fails."""
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def normalize_bool_or_passthrough(value: object, default: object) -> object:
        """Normalize bool-like values; passthrough non-bool strings or objects."""
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "yes", "1"}:
                return True
            if normalized in {"false", "no", "0"}:
                return False
            return value
        return value

    @classmethod
    def is_true_flag(cls, extra: dict[str, object], *keys: str) -> bool:
        """Return True if any provided key is present and equals true."""
        for key in keys:
            value = extra.get(key)
            if (
                isinstance(value, (bool, str))
                and cls.normalize_bool_or_passthrough(value, default=False) is True
            ):
                return True
        return False

    @staticmethod
    def parse_json_output(output: str) -> object:
        """Parse CLI output that may contain plain JSON or logs followed by JSON."""
        raw_output = (output or "").strip()
        if not raw_output:
            raise AirflowException("Empty JSON output.")

        try:
            return json.loads(raw_output)
        except json.JSONDecodeError:
            pass

        decoder = json.JSONDecoder()
        for index, char in enumerate(raw_output):
            if char not in "[{":
                continue
            try:
                parsed, _ = decoder.raw_decode(raw_output[index:])
                return parsed
            except json.JSONDecodeError:
                continue

        raise AirflowException(f"Failed to parse JSON output: {redact(raw_output)}")
