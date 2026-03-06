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

import logging
from dataclasses import dataclass, field

from airflow.providers.arenadata.ozone.utils.helpers import EnvSecretHelper, TypeNormalizationHelper
from airflow.providers.arenadata.ozone.utils.security.secret_resolver import SecretResolver


@dataclass(frozen=True)
class SSLConfig:
    """Structured SSL/TLS configuration split by subsystem."""

    ozone: dict[str, str] = field(default_factory=dict)
    hive: dict[str, str] = field(default_factory=dict)
    hdfs: dict[str, str] = field(default_factory=dict)
    conn_id: str | None = None

    def as_env(self) -> dict[str, str]:
        """Merge all subsystem env vars into a single mapping."""
        merged: dict[str, str] = {}
        merged.update(self.ozone)
        merged.update(self.hive)
        merged.update(self.hdfs)
        return merged

    @classmethod
    def from_extra(cls, extra: dict[str, object], conn_id: str | None = None) -> SSLConfig:
        """Build SSL configuration from connection Extra fields."""
        return cls(
            ozone=cls._build_ozone_env(extra, conn_id),
            hive=cls._build_hive_env(extra, conn_id),
            hdfs=cls._build_hdfs_env(extra, conn_id),
            conn_id=conn_id,
        )

    @classmethod
    def _build_ozone_env(cls, extra: dict[str, object], conn_id: str | None) -> dict[str, str]:
        """Build SSL env vars for Ozone Native CLI (ozone-site.xml mapping)."""
        env: dict[str, str] = {}
        if TypeNormalizationHelper.is_true_flag(extra, "ozone_security_enabled", "ozone.security.enabled"):
            env["OZONE_SECURITY_ENABLED"] = "true"
            env.update(
                EnvSecretHelper.build_mapped_env(
                    extra,
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
                    resolve_secret=lambda value: EnvSecretHelper.resolve_secret_masked(
                        value, lambda v: SecretResolver.get_secret_value(v, conn_id)
                    ),
                )
            )
        return env

    @classmethod
    def _build_hive_env(cls, extra: dict[str, object], conn_id: str | None) -> dict[str, str]:
        """Build SSL env vars for Hive CLI (hive-site.xml mapping)."""
        env: dict[str, str] = {}
        if TypeNormalizationHelper.is_true_flag(extra, "hive_ssl_enabled", "hive.ssl.enabled"):
            env["HIVE_SSL_ENABLED"] = "true"
            env.update(
                EnvSecretHelper.build_mapped_env(
                    extra,
                    (
                        ("hive_ssl_keystore_path", "HIVE_SSL_KEYSTORE_PATH", False),
                        ("hive_ssl_keystore_password", "HIVE_SSL_KEYSTORE_PASSWORD", True),
                        ("hive_ssl_truststore_path", "HIVE_SSL_TRUSTSTORE_PATH", False),
                        ("hive_ssl_truststore_password", "HIVE_SSL_TRUSTSTORE_PASSWORD", True),
                    ),
                    resolve_secret=lambda value: EnvSecretHelper.resolve_secret_masked(
                        value, lambda v: SecretResolver.get_secret_value(v, conn_id)
                    ),
                )
            )
        return env

    @classmethod
    def _build_hdfs_env(cls, extra: dict[str, object], conn_id: str | None) -> dict[str, str]:
        """Build SSL env vars for HDFS clients (core-site.xml / hdfs-site.xml mapping)."""
        env: dict[str, str] = {}
        if TypeNormalizationHelper.is_true_flag(extra, "hdfs_ssl_enabled", "dfs.encrypt.data.transfer"):
            env["HDFS_SSL_ENABLED"] = "true"
            env.update(
                EnvSecretHelper.build_mapped_env(
                    extra,
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
                    resolve_secret=lambda value: EnvSecretHelper.resolve_secret_masked(
                        value, lambda v: SecretResolver.get_secret_value(v, conn_id)
                    ),
                )
            )
        return env

    @staticmethod
    def apply_ssl_env_vars(
        env_vars: dict[str, str], existing_env: dict[str, str] | None = None
    ) -> dict[str, str]:
        """Apply SSL environment variables to existing environment."""
        if existing_env is None:
            return env_vars.copy()
        env = existing_env.copy()
        env.update(env_vars)
        return env

    @classmethod
    def load_from_connection(
        cls,
        conn: object,
        *,
        conn_id: str | None = None,
        logger: logging.Logger | None = None,
        enabled_flag_keys: tuple[str, ...] = (),
    ) -> dict[str, str] | None:
        """Build SSL env from connection extra with unified logging."""
        extra = EnvSecretHelper.get_connection_extra(conn)
        ssl_env_vars = cls.from_extra(extra, conn_id=conn_id).as_env()
        if not ssl_env_vars:
            if logger:
                logger.debug("No SSL/TLS configuration found in connection Extra")
            return None

        ssl_env = cls.apply_ssl_env_vars(ssl_env_vars)
        if logger:
            logger.debug("SSL/TLS configuration loaded from connection: %s", list(ssl_env_vars.keys()))
            if enabled_flag_keys and TypeNormalizationHelper.is_true_flag(extra, *enabled_flag_keys):
                logger.info("SSL/TLS enabled for connection")
        return ssl_env
