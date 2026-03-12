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

"""S3 client helpers built from an Airflow connection."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

import boto3
from boto3.s3.transfer import TransferConfig  # noqa: TCH002
from botocore.config import Config
from botocore.exceptions import ClientError

from airflow.models.connection import Connection  # noqa: TCH001
from airflow.providers.arenadata.ozone.utils.helpers import SecretHelper, TypeNormalizationHelper
from airflow.providers.arenadata.ozone.utils.security import SecretResolver


@dataclass(frozen=True)
class OzoneS3Client:
    """S3 client settings and helpers for Ozone S3 Gateway interactions."""

    endpoint_url: str | None
    verify: bool | str
    addressing_style: str
    retries_mode: str
    max_attempts: int
    region_name: str | None

    @staticmethod
    def _parse_s3_connection_config(conn: Connection, region_name: str | None = None) -> OzoneS3Client:
        """Build normalized S3 client configuration from Airflow connection and overrides."""
        extra = SecretHelper.get_connection_extra(conn)
        endpoint_url = TypeNormalizationHelper.normalize_optional_str(extra.get("endpoint_url"))
        verify = TypeNormalizationHelper.normalize_bool_or_passthrough(extra.get("verify"), default=True)

        addressing_style_raw = str(extra.get("addressing_style") or "auto").strip().lower()
        addressing_style = (
            addressing_style_raw if addressing_style_raw in {"auto", "path", "virtual"} else "auto"
        )

        retries_mode = str(extra.get("retries_mode") or "standard").strip().lower()
        max_attempts = TypeNormalizationHelper.parse_int_or_default(extra.get("max_attempts") or 3, default=3)

        region_from_inputs = region_name if region_name is not None else extra.get("region_name")
        normalized_region = TypeNormalizationHelper.normalize_optional_str(region_from_inputs)

        return OzoneS3Client(
            endpoint_url=endpoint_url,
            verify=verify,
            addressing_style=addressing_style,
            retries_mode=retries_mode,
            max_attempts=max_attempts,
            region_name=normalized_region,
        )

    @staticmethod
    def _iter_s3_objects(client, bucket_name: str, prefix: str = ""):
        """Yield raw objects from list_objects_v2 paginator."""
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        for page in pages:
            yield from page.get("Contents") or []

    @classmethod
    def parse_s3_connection_config(cls, conn: Connection, region_name: str | None = None) -> OzoneS3Client:
        """Public config parser for hooks needing connection diagnostics."""
        return cls._parse_s3_connection_config(conn, region_name=region_name)

    @classmethod
    def get_s3_client(cls, conn: Connection, region_name: str | None = None):
        """Build boto3 S3 client from Airflow connection."""
        config_data = cls._parse_s3_connection_config(conn, region_name=region_name)

        config = Config(
            retries={"mode": config_data.retries_mode, "max_attempts": config_data.max_attempts},
            s3={"addressing_style": config_data.addressing_style},
        )

        conn_id = getattr(conn, "conn_id", None)
        access_key = TypeNormalizationHelper.normalize_optional_str(
            SecretResolver.resolve_secret(conn.login or "", conn_id=conn_id)
        )
        secret_key = TypeNormalizationHelper.normalize_optional_str(
            SecretResolver.resolve_secret(conn.password or "", conn_id=conn_id)
        )

        client_kwargs: dict[str, object] = {
            "endpoint_url": config_data.endpoint_url,
            "verify": config_data.verify,
            "region_name": config_data.region_name,
            "config": config,
        }
        if access_key is not None:
            client_kwargs["aws_access_key_id"] = access_key
        if secret_key is not None:
            client_kwargs["aws_secret_access_key"] = secret_key

        return boto3.client(
            "s3",
            **client_kwargs,
        )

    @classmethod
    def get_key(cls, client, bucket_name: str, key: str) -> dict[str, object]:
        """Return raw ``get_object`` response (contains ``Body`` stream)."""
        return client.get_object(Bucket=bucket_name, Key=key)

    @classmethod
    def head_object(cls, client, bucket_name: str, key: str) -> dict | None:
        """Retrieve metadata of an object. Returns None if key does not exist (404)."""
        try:
            return client.head_object(Bucket=bucket_name, Key=key)
        except ClientError as e:
            if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return None
            raise

    @classmethod
    def check_key_exists(cls, client, bucket_name: str, key: str) -> bool:
        """Return True if key exists."""
        return cls.head_object(client, bucket_name, key) is not None

    @classmethod
    def list_keys(cls, client, bucket_name: str, prefix: str = "") -> list[str]:
        """List object keys under prefix."""
        return [obj["Key"] for obj in cls._iter_s3_objects(client, bucket_name, prefix)]

    @classmethod
    def get_file_metadata(cls, client, bucket_name: str, prefix: str = "") -> list[dict]:
        """List metadata for objects under prefix (raw Contents dicts)."""
        return list(cls._iter_s3_objects(client, bucket_name, prefix))

    @classmethod
    def create_bucket(cls, client, bucket_name: str) -> None:
        """Create a bucket. For custom endpoints (Ozone/minio) no LocationConstraint is used."""
        client.create_bucket(Bucket=bucket_name)

    @classmethod
    def load_file_obj(
        cls,
        client,
        file_obj,
        key: str,
        bucket_name: str,
        replace: bool = False,
        transfer_config: TransferConfig | None = None,
    ) -> None:
        """Upload a file-like object to S3."""
        if not replace and cls.check_key_exists(client, bucket_name, key):
            raise ValueError(f"The key {key} already exists.")
        client.upload_fileobj(file_obj, bucket_name, key, Config=transfer_config)

    @classmethod
    def load_string(
        cls,
        client,
        string_data: str,
        key: str,
        bucket_name: str,
        replace: bool = False,
        transfer_config: TransferConfig | None = None,
    ) -> None:
        """Upload string data to S3."""
        buf = BytesIO(string_data.encode("utf-8"))
        cls.load_file_obj(client, buf, key, bucket_name, replace=replace, transfer_config=transfer_config)
