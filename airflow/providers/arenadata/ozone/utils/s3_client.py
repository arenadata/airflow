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
Minimal S3 client helpers built from an Airflow Connection.

Used by:
- Ozone S3 Gateway interactions (custom endpoint_url)
- OzoneToS3Operator destination S3

Design goals:
- No dependency on amazon provider.
- Keep the API small and explicit.
- Support custom endpoints (Ozone/minio) via endpoint_url and path-style addressing.
- Support SSL verification via verify (bool or path to CA bundle).
- Support secret:// values for login/password via provider security utils.
"""

from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from airflow.providers.arenadata.ozone.utils.security import get_secret_value

if TYPE_CHECKING:
    from airflow.models.connection import Connection


class S3KeyWrapper:
    """
    Wrapper compatible with S3 Object .get()["Body"] usage.

    Some code paths (ported from airflow/providers/amazon) may expect this pattern.
    """

    __slots__ = ("_body",)

    def __init__(self, body):
        self._body = body

    def get(self) -> dict[str, object]:
        return {"Body": self._body}


def _as_bool_or_str(value: object, default: object) -> object:
    """
    Normalize values that may come from Connection extra as strings.

    - "true"/"false" -> bool
    - None -> default
    - otherwise -> value as-is
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        # Not typical for Airflow extra, but keep predictable behavior.
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "yes", "1"}:
            return True
        if v in {"false", "no", "0"}:
            return False
        return value  # e.g. "/path/to/ca.pem"
    return value


def _resolve_secret_maybe(value: str) -> str:
    """
    Resolve secret://... values via provider's secret resolver.

    If resolution fails, return the original string (do not swallow non-string values).
    """
    if isinstance(value, str) and value.startswith("secret://"):
        try:
            return str(get_secret_value(value))
        except Exception:
            return value
    return value


def get_s3_client(conn: Connection, region_name: str | None = None):
    """
    Build a boto3 S3 client from an Airflow connection.

    Connection:
    - login: access key (or secret://...)
    - password: secret key (or secret://...)

    Extra options:
    - endpoint_url: e.g. "http://ozone-s3g:9878"
    - verify: true/false or "/path/to/ca.pem"
    - addressing_style: "path" | "virtual" | "auto"  (default: "auto")
    - max_attempts: int (default: 3)
    - retries_mode: "standard" | "adaptive" | "legacy" (default: "standard")
    """
    extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}

    endpoint_url = extra.get("endpoint_url") or None
    verify = _as_bool_or_str(extra.get("verify"), default=True)

    addressing_style = (extra.get("addressing_style") or "auto").strip().lower()
    if addressing_style not in {"auto", "path", "virtual"}:
        addressing_style = "auto"

    retries_mode = (extra.get("retries_mode") or "standard").strip().lower()
    max_attempts = extra.get("max_attempts") or 3
    try:
        max_attempts = int(max_attempts)
    except Exception:
        max_attempts = 3

    config = Config(
        retries={"mode": retries_mode, "max_attempts": max_attempts},
        s3={"addressing_style": addressing_style},
    )

    access_key = _resolve_secret_maybe(conn.login or "")
    secret_key = _resolve_secret_maybe(conn.password or "")

    # For custom endpoints region is usually irrelevant, but boto3 likes having one.
    region = region_name or (extra.get("region_name") or "us-east-1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        verify=verify,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=config,
    )


def get_key(client, bucket_name: str, key: str):
    """Return an object that supports .get()["Body"] for streaming."""
    response = client.get_object(Bucket=bucket_name, Key=key)
    return S3KeyWrapper(response["Body"])


def head_object(client, bucket_name: str, key: str) -> dict | None:
    """Retrieve metadata of an object. Returns None if key does not exist (404)."""
    try:
        return client.head_object(Bucket=bucket_name, Key=key)
    except ClientError as e:
        if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
            return None
        raise


def check_key_exists(client, bucket_name: str, key: str) -> bool:
    return head_object(client, bucket_name, key) is not None


def list_keys(client, bucket_name: str, prefix: str = "") -> list[str]:
    """List object keys under prefix."""
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents") or []:
            keys.append(obj["Key"])
    return keys


def get_file_metadata(client, bucket_name: str, prefix: str = "") -> list[dict]:
    """List metadata for objects under prefix (raw Contents dicts)."""
    paginator = client.get_paginator("list_objects_v2")
    files: list[dict] = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        files.extend(page.get("Contents") or [])
    return files


def create_bucket(client, bucket_name: str) -> None:
    """Create a bucket. For custom endpoints (Ozone/minio) no LocationConstraint is used."""
    client.create_bucket(Bucket=bucket_name)


def load_file_obj(
    client,
    file_obj,
    key: str,
    bucket_name: str,
    replace: bool = False,
) -> None:
    """Upload a file-like object to S3."""
    if not replace and check_key_exists(client, bucket_name, key):
        raise ValueError(f"The key {key} already exists.")
    client.upload_fileobj(file_obj, bucket_name, key)


def load_string(
    client,
    string_data: str,
    key: str,
    bucket_name: str,
    replace: bool = False,
) -> None:
    """Upload string data to S3."""
    if not replace and check_key_exists(client, bucket_name, key):
        raise ValueError(f"The key {key} already exists.")
    buf = BytesIO(string_data.encode("utf-8"))
    client.upload_fileobj(buf, bucket_name, key)
