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
import os
from dataclasses import dataclass, field
from pathlib import Path

from airflow.configuration import ensure_secrets_loaded
from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.utils.cli_runner import KerberosCliRunner
from airflow.providers.arenadata.ozone.utils.helpers import ConnectionExtraHelper
from airflow.providers.arenadata.ozone.utils.params import (
    HDFS_KERBEROS_ENABLED_KEY,
    HDFS_SSL_ENABLED_KEY,
    HDFS_SSL_VALUE_MAP,
    OZONE_KERBEROS_ENABLED_KEY,
    OZONE_KERBEROS_VALUE_MAP,
    OZONE_SSL_ENABLED_KEY,
    OZONE_SSL_VALUE_MAP,
)
from airflow.utils.log.secrets_masker import mask_secret

log = logging.getLogger(__name__)


class SecretResolver:
    """Resolve `secret://` references via Airflow Secrets Backend."""

    @classmethod
    def get_secret_value(cls, value: object, conn_id: str | None = None, *, mask: bool = False) -> object:
        """
        Resolve secret from Airflow Secrets Backend or return value as-is.

        If ``value`` starts with ``secret://``, each configured backend is queried
        via ``get_config(value)`` and the first non-None value is returned.
        """
        if not value:
            return value

        if not (isinstance(value, str) and value.startswith("secret://")):
            if mask:
                mask_secret(value)
            return value

        for backend in ensure_secrets_loaded():
            try:
                secret_value = backend.get_config(value)
            except (AttributeError, TypeError) as err:
                log.error("Secrets Backend returned invalid response for %s: %s", value, str(err))
                raise ValueError(f"Failed to retrieve secret '{value}': {err}") from err

            if secret_value is not None:
                if conn_id:
                    log.debug(
                        "Resolved secret from Secrets Backend for connection %s: %s",
                        conn_id,
                        value.split("/")[-1] if "/" in value else value,
                    )
                if mask:
                    mask_secret(secret_value)
                return secret_value
        raise ValueError(f"Secret not found: {value}")


@dataclass(frozen=True)
class SSLConfig:
    """Structured SSL/TLS configuration split by subsystem."""

    ozone: dict[str, str] = field(default_factory=dict)
    hdfs: dict[str, str] = field(default_factory=dict)
    conn_id: str | None = None

    def as_env(self) -> dict[str, str]:
        """Merge all subsystem env vars into a single mapping."""
        merged: dict[str, str] = {}
        merged.update(self.ozone)
        merged.update(self.hdfs)
        return merged

    @staticmethod
    def _validate_scope(scope: str) -> str:
        normalized = str(scope).strip().lower()
        if normalized not in {"ozone", "hdfs", "all"}:
            raise ValueError("scope must be one of: 'ozone', 'hdfs', 'all'")
        return normalized

    @classmethod
    def from_extra(
        cls,
        extra: dict[str, object],
        *,
        conn_id: str | None = None,
        scope: str,
    ) -> SSLConfig:
        """Build SSL configuration from connection Extra fields by explicit scope."""
        normalized_scope = cls._validate_scope(scope)
        return cls(
            ozone=cls._build_ozone_env(extra, conn_id) if normalized_scope in {"ozone", "all"} else {},
            hdfs=cls._build_hdfs_env(extra, conn_id) if normalized_scope in {"hdfs", "all"} else {},
            conn_id=conn_id,
        )

    @classmethod
    def _build_ozone_env(cls, extra: dict[str, object], conn_id: str | None) -> dict[str, str]:
        """Build SSL env vars for Ozone Native CLI (ozone-site.xml mapping)."""
        env: dict[str, str] = {}
        if ConnectionExtraHelper.is_true_flag(extra, OZONE_SSL_ENABLED_KEY):
            env["OZONE_SECURITY_ENABLED"] = "true"
            env.update(
                ConnectionExtraHelper.build_mapped_env(
                    extra,
                    OZONE_SSL_VALUE_MAP,
                    resolve_secret=lambda value: SecretResolver.get_secret_value(
                        value, conn_id=conn_id, mask=True
                    ),
                )
            )
        return env

    @classmethod
    def _build_hdfs_env(cls, extra: dict[str, object], conn_id: str | None) -> dict[str, str]:
        """Build SSL env vars for HDFS clients (core-site.xml / hdfs-site.xml mapping)."""
        env: dict[str, str] = {}
        if ConnectionExtraHelper.is_true_flag(extra, HDFS_SSL_ENABLED_KEY):
            env["HDFS_SSL_ENABLED"] = "true"
            env.update(
                ConnectionExtraHelper.build_mapped_env(
                    extra,
                    HDFS_SSL_VALUE_MAP,
                    resolve_secret=lambda value: SecretResolver.get_secret_value(
                        value, conn_id=conn_id, mask=True
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
        scope: str,
        enabled_flag_key: str | None = None,
    ) -> dict[str, str] | None:
        """Build SSL env from connection extra with unified logging."""
        extra = ConnectionExtraHelper.get_connection_extra(conn)
        ssl_env_vars = cls.from_extra(extra, conn_id=conn_id, scope=scope).as_env()
        if not ssl_env_vars:
            log.debug("No SSL/TLS configuration found in connection Extra")
            return None

        ssl_env = cls.apply_ssl_env_vars(ssl_env_vars)
        log.debug("SSL/TLS configuration loaded from connection: %s", list(ssl_env_vars.keys()))
        if enabled_flag_key and ConnectionExtraHelper.is_true_flag(extra, enabled_flag_key):
            log.info("SSL/TLS enabled for connection")
        return ssl_env


@dataclass(frozen=True)
class KerberosConfig:
    """Structured Kerberos configuration split by subsystem."""

    core: dict[str, str] = field(default_factory=dict)
    hdfs: dict[str, str] = field(default_factory=dict)
    KINIT_TIMEOUT_SECONDS = 5 * 60
    CORE_SITE_XML = "core-site.xml"
    OZONE_SITE_XML = "ozone-site.xml"

    def as_env(self) -> dict[str, str]:
        merged: dict[str, str] = {}
        merged.update(self.core)
        merged.update(self.hdfs)
        return merged

    @staticmethod
    def _validate_scope(scope: str) -> str:
        normalized = str(scope).strip().lower()
        if normalized not in {"ozone", "hdfs", "all"}:
            raise ValueError("scope must be one of: 'ozone', 'hdfs', 'all'")
        return normalized

    @staticmethod
    def _principal_keytab_from_extra(
        extra: dict[str, object],
        principal_key: str,
        keytab_key: str,
        conn_id: str | None,
    ) -> None | tuple[str, str]:
        """Get (principal, keytab_path) from extra when both keys are present."""
        principal_value = ConnectionExtraHelper.get_extra(extra, principal_key, default=None)
        keytab_value = ConnectionExtraHelper.get_extra(extra, keytab_key, default=None)
        if principal_value is None or keytab_value is None:
            return None
        mapped = ConnectionExtraHelper.build_mapped_env(
            {
                "principal": principal_value,
                "keytab": keytab_value,
            },
            (
                ("principal", "PRINCIPAL", False),
                ("keytab", "KEYTAB", True),
            ),
            resolve_secret=lambda value: SecretResolver.get_secret_value(value, conn_id=conn_id, mask=True),
        )
        return (mapped["PRINCIPAL"], mapped["KEYTAB"])

    @classmethod
    def _get_core_env(cls, extra: dict[str, object], conn_id: str | None) -> dict[str, str]:
        """Kerberos env for Ozone/Hadoop core tools."""
        env_vars: dict[str, str] = {}

        if ConnectionExtraHelper.is_equals(extra, OZONE_KERBEROS_ENABLED_KEY, expected="kerberos"):
            env_vars["HADOOP_SECURITY_AUTHENTICATION"] = "kerberos"
            env_vars.update(
                ConnectionExtraHelper.build_mapped_env(
                    extra,
                    OZONE_KERBEROS_VALUE_MAP,
                    resolve_secret=lambda value: SecretResolver.get_secret_value(
                        value, conn_id=conn_id, mask=True
                    ),
                )
            )

        return env_vars

    @classmethod
    def _get_hdfs_env(cls, extra: dict[str, object], conn_id: str | None) -> dict[str, str]:
        """Kerberos env for HDFS clients."""
        env_vars: dict[str, str] = {}

        if ConnectionExtraHelper.is_true_flag(extra, HDFS_KERBEROS_ENABLED_KEY):
            pair = cls._principal_keytab_from_extra(
                extra,
                "hdfs_kerberos_principal",
                "hdfs_kerberos_keytab",
                conn_id,
            )
            if pair:
                env_vars["HDFS_KERBEROS_PRINCIPAL"], env_vars["HDFS_KERBEROS_KEYTAB"] = pair

        return env_vars

    @classmethod
    def get_env_vars(
        cls,
        extra: dict[str, object],
        *,
        scope: str,
        conn_id: str | None = None,
    ) -> dict[str, str]:
        """Extract Kerberos environment variables from connection extra by explicit scope."""
        normalized_scope = cls._validate_scope(scope)
        config = cls(
            core=cls._get_core_env(extra, conn_id) if normalized_scope in {"ozone", "all"} else {},
            hdfs=cls._get_hdfs_env(extra, conn_id) if normalized_scope in {"hdfs", "all"} else {},
        )
        return config.as_env()

    @classmethod
    def kinit_with_keytab(cls, principal: str, keytab: str, krb5_conf: str | None = None) -> bool:
        """
        Perform Kerberos authentication using keytab file.

        Runs system ``kinit -kt keytab principal`` with optional KRB5_CONFIG.
        """
        if not principal or not keytab:
            log.warning("Kerberos principal or keytab not provided, skipping kinit")
            return False

        if not Path(keytab).exists():
            log.error("Keytab file not found: %s", keytab)
            return False

        cmd = ["kinit", "-kt", keytab, principal]
        env_overrides: dict[str, str] = {}
        if krb5_conf and Path(krb5_conf).exists():
            env_overrides["KRB5_CONFIG"] = krb5_conf

        if KerberosCliRunner.run_kerberos(
            cmd,
            env_overrides=env_overrides or None,
            timeout=cls.KINIT_TIMEOUT_SECONDS,
        ):
            log.info("Successfully authenticated with Kerberos: %s", principal)
            return True
        return False

    @classmethod
    def kinit_from_env_vars(
        cls, env_vars: dict[str, str], existing_env: dict[str, str] | None = None
    ) -> bool:
        """Run kinit when principal and keytab are available."""
        if not env_vars:
            return False

        principal = env_vars.get("KERBEROS_PRINCIPAL")
        keytab = env_vars.get("KERBEROS_KEYTAB")
        if not principal or not keytab:
            return False

        base_env = (existing_env if existing_env is not None else os.environ).copy()
        krb5_conf = env_vars.get("KRB5_CONFIG") or base_env.get("KRB5_CONFIG")
        return cls.kinit_with_keytab(principal, keytab, krb5_conf)

    @staticmethod
    def apply_env_vars(
        env_vars: dict[str, str], existing_env: dict[str, str] | None = None
    ) -> dict[str, str]:
        """Build Kerberos-related environment overrides for a subprocess."""
        base_env = (existing_env if existing_env is not None else os.environ).copy()
        overrides: dict[str, str] = env_vars.copy()

        enabled = (
            overrides.get(
                "HADOOP_SECURITY_AUTHENTICATION", base_env.get("HADOOP_SECURITY_AUTHENTICATION", "")
            ).lower()
            == "kerberos"
        )
        if not enabled:
            return overrides

        hadoop_opts = overrides.get("HADOOP_OPTS") or base_env.get("HADOOP_OPTS", "")
        hadoop_flag = "-Dhadoop.security.authentication=kerberos"
        if hadoop_flag not in hadoop_opts:
            hadoop_opts = (hadoop_opts + " " + hadoop_flag).strip()
        overrides["HADOOP_OPTS"] = hadoop_opts

        ozone_opts = overrides.get("OZONE_OPTS") or base_env.get("OZONE_OPTS", "")
        ozone_flags = [
            "-Dhadoop.security.authentication=kerberos",
            "-Dozone.security.enabled=true",
        ]
        for flag in ozone_flags:
            if flag not in ozone_opts:
                ozone_opts = (ozone_opts + " " + flag).strip()
        overrides["OZONE_OPTS"] = ozone_opts

        ozone_conf_dir = overrides.get("OZONE_CONF_DIR")
        if ozone_conf_dir:
            overrides["HADOOP_CONF_DIR"] = ozone_conf_dir
        else:
            # Process-level fallback is intentionally limited to OZONE_CONF_DIR only.
            base_ozone_conf_dir = base_env.get("OZONE_CONF_DIR")
            if base_ozone_conf_dir:
                overrides["OZONE_CONF_DIR"] = base_ozone_conf_dir
                overrides["HADOOP_CONF_DIR"] = base_ozone_conf_dir
        return overrides

    @classmethod
    def load_ozone_env(
        cls,
        *,
        extra: dict[str, object],
        conn_id: str,
    ) -> dict[str, str] | None:
        """Build Kerberos env for Ozone CLI with unified logging and errors."""
        try:
            kerberos_env_vars = cls.get_env_vars(extra, scope="ozone", conn_id=conn_id)
            if kerberos_env_vars:
                kerberos_env = cls.apply_env_vars(kerberos_env_vars)
                log.debug("Kerberos configuration loaded from connection: %s", list(kerberos_env_vars.keys()))
                if ConnectionExtraHelper.is_equals(extra, OZONE_KERBEROS_ENABLED_KEY, expected="kerberos"):
                    log.debug("Kerberos authentication enabled for Ozone Native CLI")
                return kerberos_env
            log.debug("No Kerberos configuration found in connection Extra")
            return None
        except AirflowException as err:
            log.debug("Could not load Kerberos configuration (connection may not exist): %s", str(err))
            return None
        except ValueError as err:
            raise AirflowException(
                f"Invalid Kerberos configuration in connection '{conn_id}': {err}"
            ) from err

    @classmethod
    def load_hdfs_env(
        cls,
        *,
        extra: dict[str, object],
        conn_id: str,
    ) -> dict[str, str] | None:
        """Build Kerberos env for HDFS clients with unified logging and errors."""
        try:
            kerberos_env_vars = cls.get_env_vars(extra, scope="hdfs", conn_id=conn_id)
            if kerberos_env_vars:
                log.debug(
                    "HDFS Kerberos configuration loaded from connection: %s",
                    list(kerberos_env_vars.keys()),
                )
                return kerberos_env_vars
            log.debug("No HDFS Kerberos configuration found in connection Extra")
            return None
        except AirflowException as err:
            log.debug("Could not load HDFS Kerberos configuration (connection may not exist): %s", str(err))
            return None
        except ValueError as err:
            raise AirflowException(
                f"Invalid HDFS Kerberos configuration in connection '{conn_id}': {err}"
            ) from err

    @classmethod
    def ensure_ticket(
        cls,
        *,
        extra: dict[str, object],
        conn_id: str,
        kerberos_ticket_ready: bool,
    ) -> bool:
        """Ensure Kerberos ticket is initialized; returns updated readiness flag."""
        if kerberos_ticket_ready:
            return True

        kerberos_env_vars = cls.get_env_vars(extra, scope="ozone", conn_id=conn_id)
        principal = kerberos_env_vars.get("KERBEROS_PRINCIPAL")
        keytab = kerberos_env_vars.get("KERBEROS_KEYTAB")
        if not principal or not keytab:
            return False

        if not cls.kinit_from_env_vars(kerberos_env_vars):
            raise AirflowException(
                f"Kerberos authentication failed for connection '{conn_id}' using principal '{principal}'."
            )
        log.debug("Kerberos ticket is ready for connection '%s'", conn_id)
        return True

    @staticmethod
    def is_enabled(kerberos_env: dict[str, str] | None) -> bool:
        """Return True if Kerberos is effectively enabled."""
        if not kerberos_env:
            return False
        return str(kerberos_env.get("HADOOP_SECURITY_AUTHENTICATION", "")).lower() == "kerberos"

    @staticmethod
    def resolve_config_dir(kerberos_env: dict[str, str] | None) -> str | None:
        """Return effective config dir for Kerberos-enabled Ozone CLI."""
        if not kerberos_env:
            return None
        return kerberos_env.get("OZONE_CONF_DIR")

    @staticmethod
    def check_config_files_exist(config_dir: str) -> bool:
        """Return True when both core-site.xml and ozone-site.xml exist."""
        if not config_dir:
            log.debug("Config directory is None or empty")
            return False

        config_path = Path(config_dir)
        if not config_path.is_dir():
            log.debug("Config directory does not exist: %s", config_dir)
            return False

        core_site = config_path / KerberosConfig.CORE_SITE_XML
        ozone_site = config_path / KerberosConfig.OZONE_SITE_XML
        core_exists = core_site.is_file()
        ozone_exists = ozone_site.is_file()
        if not core_exists:
            log.debug("core-site.xml not found at: %s", core_site)
        if not ozone_exists:
            log.debug("ozone-site.xml not found at: %s", ozone_site)
        if core_exists and ozone_exists:
            log.debug("Both configuration files found in %s", config_dir)
        return core_exists and ozone_exists
