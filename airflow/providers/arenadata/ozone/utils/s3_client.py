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

import logging
from dataclasses import dataclass
from enum import Enum
from io import BytesIO

import boto3
from boto3.s3.transfer import TransferConfig  # noqa: TCH002
from botocore.config import Config
from botocore.exceptions import ClientError

from airflow.models.connection import Connection  # noqa: TCH001
from airflow.providers.arenadata.ozone.utils.common import get_connection_extra
from airflow.providers.arenadata.ozone.utils.security import get_secret_value, is_secret_ref

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class S3ConnectionConfig:
    """Normalized S3 connection settings derived from Airflow connection extra."""

    endpoint_url: str | None
    verify: bool | str
    addressing_style: str
    retries_mode: str
    max_attempts: int
    region_name: str


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


class S3ErrorCode(str, Enum):
    """Known S3 error codes used by the Ozone provider."""

    NO_SUCH_BUCKET = "NoSuchBucket"
    NO_SUCH_KEY = "NoSuchKey"
    ACCESS_DENIED = "AccessDenied"
    BUCKET_ALREADY_EXISTS = "BucketAlreadyExists"
    BUCKET_ALREADY_OWNED_BY_YOU = "BucketAlreadyOwnedByYou"


def map_s3_error_to_ozone(error: ClientError) -> str:
    """Translate a boto3 ClientError into an Ozone-specific, readable message."""
    error_code = error.response.get("Error", {}).get("Code", "Unknown")

    if error_code == S3ErrorCode.NO_SUCH_BUCKET:
        return "Ozone S3 Gateway Error: The specified bucket does not exist."
    if error_code == S3ErrorCode.NO_SUCH_KEY:
        return "Ozone S3 Gateway Error: The specified key does not exist."
    if error_code == S3ErrorCode.ACCESS_DENIED:
        return "Ozone S3 Gateway Error: Access Denied. Check Ozone ACLs or Ranger policies."

    return f"Ozone S3 Gateway reported an unhandled error: {error_code}"


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
    if is_secret_ref(value):
        try:
            return str(get_secret_value(value))
        except ValueError as err:
            log.warning("Could not resolve secret reference %s (%s); using original value", value, err)
            return value
    return value


def parse_s3_connection_config(conn: Connection, region_name: str | None = None) -> S3ConnectionConfig:
    """Build normalized S3 client configuration from Airflow connection and overrides."""
    extra = get_connection_extra(conn)
    endpoint_url_raw = extra.get("endpoint_url")
    endpoint_url = str(endpoint_url_raw).strip() if endpoint_url_raw else None
    verify = _as_bool_or_str(extra.get("verify"), default=True)

    addressing_style_raw = str(extra.get("addressing_style") or "auto").strip().lower()
    addressing_style = addressing_style_raw if addressing_style_raw in {"auto", "path", "virtual"} else "auto"

    retries_mode = str(extra.get("retries_mode") or "standard").strip().lower()
    max_attempts_raw = extra.get("max_attempts") or 3
    try:
        max_attempts = int(max_attempts_raw)
    except (TypeError, ValueError):
        max_attempts = 3

    normalized_region = str(region_name or (extra.get("region_name") or "us-east-1")).strip() or "us-east-1"

    return S3ConnectionConfig(
        endpoint_url=endpoint_url,
        verify=verify,
        addressing_style=addressing_style,
        retries_mode=retries_mode,
        max_attempts=max_attempts,
        region_name=normalized_region,
    )


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
    config_data = parse_s3_connection_config(conn, region_name=region_name)

    config = Config(
        retries={"mode": config_data.retries_mode, "max_attempts": config_data.max_attempts},
        s3={"addressing_style": config_data.addressing_style},
    )

    access_key = _resolve_secret_maybe(conn.login or "")
    secret_key = _resolve_secret_maybe(conn.password or "")

    return boto3.client(
        "s3",
        endpoint_url=config_data.endpoint_url,
        verify=config_data.verify,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=config_data.region_name,
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
    return [obj["Key"] for obj in _iter_s3_objects(client, bucket_name, prefix)]


def get_file_metadata(client, bucket_name: str, prefix: str = "") -> list[dict]:
    """List metadata for objects under prefix (raw Contents dicts)."""
    return list(_iter_s3_objects(client, bucket_name, prefix))


def _iter_s3_objects(client, bucket_name: str, prefix: str = ""):
    """Yield raw objects from list_objects_v2 paginator."""
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    for page in pages:
        yield from page.get("Contents") or []


def create_bucket(client, bucket_name: str) -> None:
    """Create a bucket. For custom endpoints (Ozone/minio) no LocationConstraint is used."""
    client.create_bucket(Bucket=bucket_name)


def load_file_obj(
    client,
    file_obj,
    key: str,
    bucket_name: str,
    replace: bool = False,
    transfer_config: TransferConfig | None = None,
) -> None:
    """Upload a file-like object to S3."""
    if not replace and check_key_exists(client, bucket_name, key):
        raise ValueError(f"The key {key} already exists.")
    client.upload_fileobj(file_obj, bucket_name, key, Config=transfer_config)


def load_string(
    client,
    string_data: str,
    key: str,
    bucket_name: str,
    replace: bool = False,
    transfer_config: TransferConfig | None = None,
) -> None:
    """Upload string data to S3."""
    buf = BytesIO(string_data.encode("utf-8"))
    load_file_obj(client, buf, key, bucket_name, replace=replace, transfer_config=transfer_config)
