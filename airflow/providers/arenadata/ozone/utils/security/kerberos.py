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
import subprocess
from dataclasses import dataclass, field

from airflow.providers.arenadata.ozone.utils.security.secret_resolver import get_secret_value
from airflow.utils.log.secrets_masker import mask_secret

log = logging.getLogger(__name__)


def _kerberos_principal_keytab_from_extra(
    extra: dict, principal_key: str, keytab_key: str, conn_id: str | None
) -> None | tuple[str, str]:
    """Get (principal, keytab_path) from extra if both keys present; resolve keytab via get_secret_value."""
    if principal_key not in extra or keytab_key not in extra:
        return None
    keytab_path = get_secret_value(extra[keytab_key], conn_id)
    mask_secret(keytab_path)
    return (extra[principal_key], keytab_path)


@dataclass(frozen=True)
class KerberosConfig:
    """Structured Kerberos configuration split by subsystem."""

    core: dict[str, str] = field(default_factory=dict)
    hive: dict[str, str] = field(default_factory=dict)
    hdfs: dict[str, str] = field(default_factory=dict)

    @property
    def env(self) -> dict[str, str]:
        merged: dict[str, str] = {}
        merged.update(self.core)
        merged.update(self.hive)
        merged.update(self.hdfs)
        return merged


def _get_core_kerberos_env(
    extra: dict[str, object], extra_lower: dict[str, str], conn_id: str | None
) -> dict[str, str]:
    """Kerberos env for Ozone/Hadoop core tools."""
    env_vars: dict[str, str] = {}

    if (
        extra_lower.get("hadoop_security_authentication") == "kerberos"
        or extra_lower.get("hadoop.security.authentication") == "kerberos"
    ):
        env_vars["HADOOP_SECURITY_AUTHENTICATION"] = "kerberos"
        if "kerberos_principal" in extra:
            env_vars["KERBEROS_PRINCIPAL"] = extra["kerberos_principal"]
        if "kerberos_keytab" in extra:
            keytab_path = get_secret_value(extra["kerberos_keytab"], conn_id)
            mask_secret(keytab_path)
            env_vars["KERBEROS_KEYTAB"] = keytab_path
        if "kerberos_realm" in extra:
            env_vars["KERBEROS_REALM"] = extra["kerberos_realm"]
        if "krb5_conf" in extra:
            env_vars["KRB5_CONFIG"] = extra["krb5_conf"]
        if "ozone_conf_dir" in extra:
            env_vars["OZONE_CONF_DIR"] = extra["ozone_conf_dir"]
        elif "hadoop_conf_dir" in extra:
            env_vars["HADOOP_CONF_DIR"] = extra["hadoop_conf_dir"]

    return env_vars


def _get_hive_kerberos_env(
    extra: dict[str, object], extra_lower: dict[str, str], conn_id: str | None
) -> dict[str, str]:
    """Kerberos env for Hive CLI."""
    env_vars: dict[str, str] = {}

    if (
        extra_lower.get("hive_kerberos_enabled") == "true"
        or extra_lower.get("hive.security.authentication") == "kerberos"
    ):
        pair = _kerberos_principal_keytab_from_extra(
            extra, "hive_kerberos_principal", "hive_kerberos_keytab", conn_id
        )
        if pair:
            env_vars["HIVE_KERBEROS_PRINCIPAL"], env_vars["HIVE_KERBEROS_KEYTAB"] = pair

    return env_vars


def _get_hdfs_kerberos_env(
    extra: dict[str, object], extra_lower: dict[str, str], conn_id: str | None
) -> dict[str, str]:
    """Kerberos env for HDFS clients."""
    env_vars: dict[str, str] = {}

    if (
        extra_lower.get("hdfs_kerberos_enabled") == "true"
        or extra_lower.get("dfs.kerberos.enabled") == "true"
    ):
        pair = _kerberos_principal_keytab_from_extra(
            extra, "hdfs_kerberos_principal", "hdfs_kerberos_keytab", conn_id
        )
        if pair:
            env_vars["HDFS_KERBEROS_PRINCIPAL"], env_vars["HDFS_KERBEROS_KEYTAB"] = pair

    return env_vars


def get_kerberos_env_vars(extra: dict, conn_id: str | None = None) -> dict[str, str]:
    """
    Extract Kerberos configuration from connection Extra and return environment variables.

    Supports Ozone, Hive, and HDFS Kerberos (principal/keytab and conf dir from extra).
    """
    extra_lower = {k: str(v).lower() for k, v in extra.items() if v is not None}
    config = KerberosConfig(
        core=_get_core_kerberos_env(extra, extra_lower, conn_id),
        hive=_get_hive_kerberos_env(extra, extra_lower, conn_id),
        hdfs=_get_hdfs_kerberos_env(extra, extra_lower, conn_id),
    )
    return config.env


def kinit_with_keytab(principal: str, keytab: str, krb5_conf: str | None = None) -> bool:
    """
    Perform Kerberos authentication using keytab file.

    Runs system ``kinit -kt keytab principal`` with optional KRB5_CONFIG.
    """
    if not principal or not keytab:
        log.warning("Kerberos principal or keytab not provided, skipping kinit")
        return False

    if not os.path.exists(keytab):
        log.error("Keytab file not found: %s", keytab)
        return False

    try:
        cmd = ["kinit", "-kt", keytab, principal]
        env = os.environ.copy()
        if krb5_conf and os.path.exists(krb5_conf):
            env["KRB5_CONFIG"] = krb5_conf

        subprocess.run(cmd, capture_output=True, text=True, check=True, env=env, timeout=30)
        log.info("Successfully authenticated with Kerberos: %s", principal)
        return True
    except subprocess.CalledProcessError as e:
        log.error("kinit failed: %s", e.stderr.strip() if e.stderr else str(e))
        return False
    except subprocess.TimeoutExpired:
        log.error("kinit timed out after 30 seconds")
        return False
    except FileNotFoundError:
        log.error("kinit command not found. Ensure Kerberos client tools are installed.")
        return False
    except Exception as e:
        log.error("Unexpected error during kinit: %s", str(e))
        return False


def kinit_from_env_vars(env_vars: dict[str, str], existing_env: dict[str, str] | None = None) -> bool:
    """
    Run kinit if env_vars contain principal+keytab.

    This is intentionally separated from apply_kerberos_env_vars() to make side-effects explicit.
    """
    if not env_vars:
        return False

    principal = env_vars.get("KERBEROS_PRINCIPAL")
    keytab = env_vars.get("KERBEROS_KEYTAB")
    if not principal or not keytab:
        return False

    base_env = (existing_env if existing_env is not None else os.environ).copy()
    krb5_conf = env_vars.get("KRB5_CONFIG") or base_env.get("KRB5_CONFIG")
    return kinit_with_keytab(principal, keytab, krb5_conf)


def apply_kerberos_env_vars(
    env_vars: dict[str, str], existing_env: dict[str, str] | None = None
) -> dict[str, str]:
    """
    Build Kerberos environment overrides (delta) for a process.

    This function is intentionally pure with respect to side effects:
    - does NOT run ``kinit``
    - does NOT create or modify XML config files

    It only:
    - adds required JVM flags (HADOOP_OPTS / OZONE_OPTS),
    - ensures a reasonable config directory hint (HADOOP_CONF_DIR / OZONE_CONF_DIR).
    """
    base_env = (existing_env if existing_env is not None else os.environ).copy()
    overrides: dict[str, str] = env_vars.copy()

    kerberos_enabled = (
        overrides.get(
            "HADOOP_SECURITY_AUTHENTICATION", base_env.get("HADOOP_SECURITY_AUTHENTICATION", "")
        ).lower()
        == "kerberos"
    )

    if kerberos_enabled:
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
            or "/opt/airflow/ozone-conf"
        )
        overrides.setdefault("HADOOP_CONF_DIR", conf_dir)
        overrides.setdefault("OZONE_CONF_DIR", conf_dir)

    return overrides
