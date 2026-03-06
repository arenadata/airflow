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

import atexit
import contextlib
import logging
import os
import shutil
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.utils.cli_runner import CliRunner
from airflow.providers.arenadata.ozone.utils.helpers import EnvSecretHelper
from airflow.providers.arenadata.ozone.utils.security.secret_resolver import SecretResolver

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class KerberosConfig:
    """Structured Kerberos configuration split by subsystem."""

    core: dict[str, str] = field(default_factory=dict)
    hive: dict[str, str] = field(default_factory=dict)
    hdfs: dict[str, str] = field(default_factory=dict)
    KINIT_TIMEOUT_SECONDS: ClassVar[int] = 5 * 60
    _CONF_DIRS: ClassVar[set[str]] = set()

    def as_env(self) -> dict[str, str]:
        merged: dict[str, str] = {}
        merged.update(self.core)
        merged.update(self.hive)
        merged.update(self.hdfs)
        return merged

    @classmethod
    def _cleanup_conf_dirs(cls) -> None:
        """Cleanup temporary client configuration directories."""
        for conf_dir in list(cls._CONF_DIRS):
            conf_path = Path(conf_dir)
            try:
                with contextlib.suppress(FileNotFoundError):
                    shutil.rmtree(conf_path)
            except OSError as exc:
                log.warning("Failed to clean temporary Ozone conf directory %s: %s", conf_path, exc)
            finally:
                if not conf_path.exists():
                    log.debug("Temporary Ozone conf directory already removed: %s", conf_path)
                cls._CONF_DIRS.discard(conf_dir)

    @staticmethod
    def _principal_keytab_from_extra(
        extra: dict[str, object], principal_key: str, keytab_key: str, conn_id: str | None
    ) -> None | tuple[str, str]:
        """Get (principal, keytab_path) from extra when both keys are present."""
        if principal_key not in extra or keytab_key not in extra:
            return None
        mapped = EnvSecretHelper.build_mapped_env(
            extra,
            (
                (principal_key, "PRINCIPAL", False),
                (keytab_key, "KEYTAB", True),
            ),
            resolve_secret=lambda value: EnvSecretHelper.resolve_secret_masked(
                value, lambda v: SecretResolver.get_secret_value(v, conn_id)
            ),
        )
        return (mapped["PRINCIPAL"], mapped["KEYTAB"])

    @classmethod
    def _get_core_env(
        cls, extra: dict[str, object], extra_lower: dict[str, str], conn_id: str | None
    ) -> dict[str, str]:
        """Kerberos env for Ozone/Hadoop core tools."""
        env_vars: dict[str, str] = {}

        if (
            extra_lower.get("hadoop_security_authentication") == "kerberos"
            or extra_lower.get("hadoop.security.authentication") == "kerberos"
        ):
            env_vars["HADOOP_SECURITY_AUTHENTICATION"] = "kerberos"
            env_vars.update(
                EnvSecretHelper.build_mapped_env(
                    extra,
                    (
                        ("kerberos_principal", "KERBEROS_PRINCIPAL", False),
                        ("kerberos_keytab", "KERBEROS_KEYTAB", True),
                        ("kerberos_realm", "KERBEROS_REALM", False),
                        ("krb5_conf", "KRB5_CONFIG", False),
                        ("ozone_conf_dir", "OZONE_CONF_DIR", False),
                    ),
                    resolve_secret=lambda value: EnvSecretHelper.resolve_secret_masked(
                        value, lambda v: SecretResolver.get_secret_value(v, conn_id)
                    ),
                )
            )
            if "OZONE_CONF_DIR" not in env_vars:
                env_vars.update(
                    EnvSecretHelper.build_mapped_env(
                        extra,
                        (("hadoop_conf_dir", "HADOOP_CONF_DIR", False),),
                    )
                )

        return env_vars

    @classmethod
    def _get_hive_env(
        cls, extra: dict[str, object], extra_lower: dict[str, str], conn_id: str | None
    ) -> dict[str, str]:
        """Kerberos env for Hive CLI."""
        env_vars: dict[str, str] = {}

        if (
            extra_lower.get("hive_kerberos_enabled") == "true"
            or extra_lower.get("hive.security.authentication") == "kerberos"
        ):
            pair = cls._principal_keytab_from_extra(
                extra, "hive_kerberos_principal", "hive_kerberos_keytab", conn_id
            )
            if pair:
                env_vars["HIVE_KERBEROS_PRINCIPAL"], env_vars["HIVE_KERBEROS_KEYTAB"] = pair

        return env_vars

    @classmethod
    def _get_hdfs_env(
        cls, extra: dict[str, object], extra_lower: dict[str, str], conn_id: str | None
    ) -> dict[str, str]:
        """Kerberos env for HDFS clients."""
        env_vars: dict[str, str] = {}

        if (
            extra_lower.get("hdfs_kerberos_enabled") == "true"
            or extra_lower.get("dfs.kerberos.enabled") == "true"
        ):
            pair = cls._principal_keytab_from_extra(
                extra, "hdfs_kerberos_principal", "hdfs_kerberos_keytab", conn_id
            )
            if pair:
                env_vars["HDFS_KERBEROS_PRINCIPAL"], env_vars["HDFS_KERBEROS_KEYTAB"] = pair

        return env_vars

    @classmethod
    def get_env_vars(cls, extra: dict[str, object], conn_id: str | None = None) -> dict[str, str]:
        """Extract Kerberos environment variables from connection extra."""
        extra_lower = {k: str(v).lower() for k, v in extra.items() if v is not None}
        config = cls(
            core=cls._get_core_env(extra, extra_lower, conn_id),
            hive=cls._get_hive_env(extra, extra_lower, conn_id),
            hdfs=cls._get_hdfs_env(extra, extra_lower, conn_id),
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

        if CliRunner.run_kerberos(
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

        conf_dir = (
            overrides.get("OZONE_CONF_DIR")
            or overrides.get("HADOOP_CONF_DIR")
            or base_env.get("OZONE_CONF_DIR")
            or base_env.get("HADOOP_CONF_DIR")
        )
        if conf_dir:
            overrides.setdefault("HADOOP_CONF_DIR", conf_dir)
            overrides.setdefault("OZONE_CONF_DIR", conf_dir)
        return overrides

    @classmethod
    def load_ozone_env(
        cls,
        *,
        extra: dict[str, object],
        conn_id: str,
        logger: logging.Logger,
    ) -> dict[str, str] | None:
        """Build Kerberos env for Ozone CLI with unified logging and errors."""
        try:
            kerberos_env_vars = cls.get_env_vars(extra, conn_id=conn_id)
            if kerberos_env_vars:
                kerberos_env = cls.apply_env_vars(kerberos_env_vars)
                logger.debug(
                    "Kerberos configuration loaded from connection: %s", list(kerberos_env_vars.keys())
                )
                if extra.get("hadoop_security_authentication") == "kerberos":
                    logger.debug("Kerberos authentication enabled for Ozone Native CLI")
                return kerberos_env
            logger.debug("No Kerberos configuration found in connection Extra")
            return None
        except AirflowException as err:
            logger.debug("Could not load Kerberos configuration (connection may not exist): %s", str(err))
            return None
        except ValueError as err:
            raise AirflowException(
                f"Invalid Kerberos configuration in connection '{conn_id}': {err}"
            ) from err

    @classmethod
    def ensure_ticket(
        cls,
        *,
        extra: dict[str, object],
        conn_id: str,
        logger: logging.Logger,
        kerberos_ticket_ready: bool,
    ) -> bool:
        """Ensure Kerberos ticket is initialized; returns updated readiness flag."""
        if kerberos_ticket_ready:
            return True

        kerberos_env_vars = cls.get_env_vars(extra, conn_id=conn_id)
        principal = kerberos_env_vars.get("KERBEROS_PRINCIPAL")
        keytab = kerberos_env_vars.get("KERBEROS_KEYTAB")
        if not principal or not keytab:
            return False

        if not cls.kinit_from_env_vars(kerberos_env_vars):
            raise AirflowException(
                f"Kerberos authentication failed for connection '{conn_id}' using principal '{principal}'."
            )
        logger.debug("Kerberos ticket is ready for connection '%s'", conn_id)
        return True

    @staticmethod
    def is_enabled(kerberos_env: dict[str, str] | None) -> bool:
        """Return True if Kerberos is effectively enabled."""
        if not kerberos_env:
            return False
        return str(kerberos_env.get("HADOOP_SECURITY_AUTHENTICATION", "")).lower() == "kerberos"

    @staticmethod
    def check_config_files_exist(config_dir: str, *, logger: logging.Logger) -> bool:
        """Return True when both core-site.xml and ozone-site.xml exist."""
        if not config_dir:
            logger.debug("Config directory is None or empty")
            return False

        config_path = Path(config_dir)
        if not config_path.is_dir():
            logger.debug("Config directory does not exist: %s", config_dir)
            return False

        core_site = config_path / "core-site.xml"
        ozone_site = config_path / "ozone-site.xml"
        core_exists = core_site.is_file()
        ozone_exists = ozone_site.is_file()
        if not core_exists:
            logger.debug("core-site.xml not found at: %s", core_site)
        if not ozone_exists:
            logger.debug("ozone-site.xml not found at: %s", ozone_site)
        if core_exists and ozone_exists:
            logger.debug("Both configuration files found in %s", config_dir)
        return core_exists and ozone_exists

    @classmethod
    def build_client_conf_dir(
        cls,
        *,
        host: str,
        port: int,
        extra: dict[str, object],
        conn_id: str,
        logger: logging.Logger,
    ) -> str | None:
        """Generate minimal Ozone client config directory when ozone_scm_address is provided."""
        ozone_scm_address = extra.get("ozone_scm_address")
        if not ozone_scm_address:
            return None

        tmpdir = tempfile.mkdtemp(prefix="airflow_ozone_conf_")
        try:
            om_addr = f"{host}:{port}"
            scm_addr = str(ozone_scm_address or "scm").strip()
            recon_addr = str(extra.get("ozone_recon_address") or "").strip()
            tmp_path = Path(tmpdir)

            ozone_root = ET.Element("configuration")
            ozone_props = [
                ("ozone.om.address", om_addr),
                ("ozone.scm.client.address", scm_addr),
                ("ozone.scm.block.client.address", scm_addr),
            ]
            if recon_addr:
                ozone_props.append(("ozone.recon.address", recon_addr))

            for prop_name, prop_value in ozone_props:
                prop = ET.SubElement(ozone_root, "property")
                ET.SubElement(prop, "name").text = prop_name
                ET.SubElement(prop, "value").text = prop_value

            ET.ElementTree(ozone_root).write(
                tmp_path / "ozone-site.xml",
                encoding="utf-8",
                xml_declaration=True,
            )
            ET.ElementTree(ET.Element("configuration")).write(
                tmp_path / "core-site.xml",
                encoding="utf-8",
                xml_declaration=True,
            )

            logger.debug("Generated minimal ozone-site.xml (om=%s, scm=%s)", om_addr, scm_addr)
            cls._CONF_DIRS.add(tmpdir)
            return tmpdir
        except (OSError, TypeError, ValueError) as err:
            logger.error("Failed to create Ozone client conf from Extra: %s", err)
            tmp_path = Path(tmpdir)
            try:
                with contextlib.suppress(FileNotFoundError):
                    shutil.rmtree(tmp_path)
            except OSError as cleanup_error:
                logger.warning("Failed to clean temporary Ozone conf directory %s: %s", tmpdir, cleanup_error)
            raise AirflowException(
                f"Failed to create temporary Ozone client configuration for connection '{conn_id}'"
            ) from err

    @staticmethod
    def effective_config_dir(kerberos_env: dict[str, str] | None, client_conf_dir: str | None) -> str | None:
        """Return effective config dir used by env and CLI --config."""
        if kerberos_env:
            conf_dir = kerberos_env.get("OZONE_CONF_DIR") or kerberos_env.get("HADOOP_CONF_DIR")
            if conf_dir:
                return conf_dir
        return client_conf_dir

    @staticmethod
    def build_ozone_cli_env(
        *,
        host: str,
        port: int,
        conn_id: str,
        logger: logging.Logger,
        effective_config_dir: str | None,
        ssl_env: dict[str, str] | None,
        kerberos_env: dict[str, str] | None,
    ) -> dict[str, str]:
        """Build subprocess env for Ozone CLI with SSL/Kerberos and optional generated conf."""
        env = CliRunner.merge_env(None)
        env["OZONE_OM_ADDRESS"] = f"{host}:{port}"
        if effective_config_dir:
            env["OZONE_CONF_DIR"] = effective_config_dir
        if ssl_env:
            env.update(ssl_env)
        if kerberos_env:
            env.update(kerberos_env)
        logger.debug("Prepared Ozone CLI env for connection '%s'", conn_id)
        return env


atexit.register(KerberosConfig._cleanup_conf_dirs)
