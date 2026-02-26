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

from dataclasses import dataclass, field

from airflow.providers.arenadata.ozone.utils.security.secret_resolver import get_secret_value
from airflow.utils.log.secrets_masker import mask_secret


@dataclass(frozen=True)
class SSLConfig:
    """Structured SSL/TLS configuration split by subsystem."""

    ozone: dict[str, str] = field(default_factory=dict)
    hive: dict[str, str] = field(default_factory=dict)
    hdfs: dict[str, str] = field(default_factory=dict)

    @property
    def env(self) -> dict[str, str]:
        """Merge all subsystem env vars into a single mapping."""
        merged: dict[str, str] = {}
        merged.update(self.ozone)
        merged.update(self.hive)
        merged.update(self.hdfs)
        return merged


def _build_ssl_env(
    extra: dict[str, object],
    conn_id: str | None,
    mapping: tuple[tuple[str, str, bool], ...],
) -> dict[str, str]:
    """Build SSL env vars from an extra->env mapping."""
    env: dict[str, str] = {}
    for extra_key, env_key, is_secret in mapping:
        if extra_key not in extra:
            continue
        value = extra[extra_key]
        if is_secret:
            resolved = get_secret_value(value, conn_id)
            mask_secret(resolved)
            env[env_key] = str(resolved)
        else:
            env[env_key] = str(value)
    return env


def _get_ozone_ssl_env(extra: dict[str, object], conn_id: str | None) -> dict[str, str]:
    """Build SSL env vars for Ozone Native CLI (ozone-site.xml mapping)."""
    env: dict[str, str] = {}

    if extra.get("ozone_security_enabled") == "true" or extra.get("ozone.security.enabled") == "true":
        env["OZONE_SECURITY_ENABLED"] = "true"
        env.update(
            _build_ssl_env(
                extra,
                conn_id,
                (
                    ("ozone_om_https_port", "OZONE_OM_HTTPS_PORT", False),
                    ("ozone.scm.https.port", "OZONE_SCM_HTTPS_PORT", False),
                    ("ozone_ssl_keystore_location", "OZONE_SSL_KEYSTORE_LOCATION", False),
                    ("ozone_ssl_keystore_password", "OZONE_SSL_KEYSTORE_PASSWORD", True),
                    ("ozone_ssl_keystore_type", "OZONE_SSL_KEYSTORE_TYPE", False),
                    ("ozone_ssl_truststore_location", "OZONE_SSL_TRUSTSTORE_LOCATION", False),
                    ("ozone_ssl_truststore_password", "OZONE_SSL_TRUSTSTORE_PASSWORD", True),
                    ("ozone_ssl_truststore_type", "OZONE_SSL_TRUSTSTORE_TYPE", False),
                ),
            )
        )

    return env


def _get_hive_ssl_env(extra: dict[str, object], conn_id: str | None) -> dict[str, str]:
    """Build SSL env vars for Hive CLI (hive-site.xml mapping)."""
    env: dict[str, str] = {}

    if extra.get("hive_ssl_enabled") == "true" or extra.get("hive.ssl.enabled") == "true":
        env["HIVE_SSL_ENABLED"] = "true"
        env.update(
            _build_ssl_env(
                extra,
                conn_id,
                (
                    ("hive_ssl_keystore_path", "HIVE_SSL_KEYSTORE_PATH", False),
                    ("hive_ssl_keystore_password", "HIVE_SSL_KEYSTORE_PASSWORD", True),
                    ("hive_ssl_truststore_path", "HIVE_SSL_TRUSTSTORE_PATH", False),
                    ("hive_ssl_truststore_password", "HIVE_SSL_TRUSTSTORE_PASSWORD", True),
                ),
            )
        )

    return env


def _get_hdfs_ssl_env(extra: dict[str, object], conn_id: str | None) -> dict[str, str]:
    """Build SSL env vars for HDFS clients (core-site.xml / hdfs-site.xml mapping)."""
    env: dict[str, str] = {}

    if extra.get("hdfs_ssl_enabled") == "true" or extra.get("dfs.encrypt.data.transfer") == "true":
        env["HDFS_SSL_ENABLED"] = "true"
        env.update(
            _build_ssl_env(
                extra,
                conn_id,
                (
                    ("dfs_encrypt_data_transfer", "DFS_ENCRYPT_DATA_TRANSFER", False),
                    ("dfs.encrypt.data.transfer", "DFS_ENCRYPT_DATA_TRANSFER", False),
                    ("hdfs_ssl_keystore_location", "HDFS_SSL_KEYSTORE_LOCATION", False),
                    ("hdfs_ssl_keystore_password", "HDFS_SSL_KEYSTORE_PASSWORD", True),
                    ("hdfs_ssl_keystore_type", "HDFS_SSL_KEYSTORE_TYPE", False),
                    ("hdfs_ssl_truststore_location", "HDFS_SSL_TRUSTSTORE_LOCATION", False),
                    ("hdfs_ssl_truststore_password", "HDFS_SSL_TRUSTSTORE_PASSWORD", True),
                    ("hdfs_ssl_truststore_type", "HDFS_SSL_TRUSTSTORE_TYPE", False),
                ),
            )
        )

    return env


def get_ssl_env_vars(extra: dict, conn_id: str | None = None) -> dict[str, str]:
    """
    Extract SSL/TLS configuration from connection Extra and return environment variables.

    Supports:
    - Ozone Native CLI SSL configuration
    - Hive SSL configuration
    - HDFS SSL configuration
    - Airflow Secrets Backend integration (secret:// paths)
    """
    config = SSLConfig(
        ozone=_get_ozone_ssl_env(extra, conn_id),
        hive=_get_hive_ssl_env(extra, conn_id),
        hdfs=_get_hdfs_ssl_env(extra, conn_id),
    )
    return config.env


def apply_ssl_env_vars(
    env_vars: dict[str, str], existing_env: dict[str, str] | None = None
) -> dict[str, str]:
    """
    Apply SSL environment variables to existing environment.

    If existing_env is None -> return overrides only (delta).
    """
    if existing_env is None:
        return env_vars.copy()

    env = existing_env.copy()
    env.update(env_vars)
    return env
