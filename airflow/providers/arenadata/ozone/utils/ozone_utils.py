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

from airflow.configuration import ensure_secrets_loaded
from airflow.utils.log.secrets_masker import mask_secret

log = logging.getLogger(__name__)


def get_secret_value(value: str, conn_id: str | None = None) -> str:
    """
    Resolve secret from Airflow Secrets Backend or return value as-is.

    If ``value`` starts with ``secret://``, it is resolved via the configured
    Secrets Backends (same as Airflow config secrets): each backend's
    :meth:`~airflow.secrets.base_secrets.BaseSecretsBackend.get_config` is tried
    with the URI; the first non-None result is returned. Otherwise the value
    is returned unchanged.

    Other providers do not resolve ``secret://`` in connection Extra fields;
    this helper is specific to the Ozone provider for SSL/Kerberos passwords
    and keytab paths stored as secret URIs.

    :param value: Secret value or ``secret://`` URI
    :param conn_id: Optional connection ID (for logging only)
    :return: Resolved secret value or original value
    """
    if not value:
        return value

    if not isinstance(value, str) or not value.startswith("secret://"):
        return value

    try:
        for backend in ensure_secrets_loaded():
            secret_value = backend.get_config(value)
            if secret_value is not None:
                if conn_id:
                    log.debug(
                        "Resolved secret from Secrets Backend for connection %s: %s",
                        conn_id,
                        value.split("/")[-1] if "/" in value else value,
                    )
                mask_secret(secret_value)
                return secret_value
        raise ValueError(f"Secret not found: {value}")
    except ValueError:
        raise
    except Exception as e:
        log.error("Failed to retrieve secret from Secrets Backend: %s - %s", value, str(e))
        raise ValueError(f"Failed to retrieve secret '{value}': {str(e)}") from e


def get_ssl_env_vars(extra: dict, conn_id: str | None = None) -> dict[str, str]:
    """
    Extract SSL/TLS configuration from connection Extra and return environment variables.

    Supports:
    - Ozone Native CLI SSL configuration
    - Hive SSL configuration
    - HDFS SSL configuration
    - Airflow Secrets Backend integration (secret:// paths)

    :param extra: Connection Extra dictionary
    :param conn_id: Optional connection ID for Secrets Backend context
    :return: Dictionary of environment variables to set for SSL/TLS
    """
    env_vars = {}

    # Ozone Native CLI SSL Configuration
    # These map to ozone-site.xml properties
    if extra.get("ozone_security_enabled") == "true" or extra.get("ozone.security.enabled") == "true":
        env_vars["OZONE_SECURITY_ENABLED"] = "true"

        # SSL/TLS ports
        if "ozone_om_https_port" in extra:
            env_vars["OZONE_OM_HTTPS_PORT"] = str(extra["ozone_om_https_port"])
        if "ozone.scm.https.port" in extra:
            env_vars["OZONE_SCM_HTTPS_PORT"] = str(extra["ozone.scm.https.port"])

        # Keystore configuration (for client certificates)
        if "ozone_ssl_keystore_location" in extra:
            env_vars["OZONE_SSL_KEYSTORE_LOCATION"] = extra["ozone_ssl_keystore_location"]
        if "ozone_ssl_keystore_password" in extra:
            password = get_secret_value(extra["ozone_ssl_keystore_password"], conn_id)
            mask_secret(password)
            env_vars["OZONE_SSL_KEYSTORE_PASSWORD"] = password
        if "ozone_ssl_keystore_type" in extra:
            env_vars["OZONE_SSL_KEYSTORE_TYPE"] = extra["ozone_ssl_keystore_type"]

        # Truststore configuration (for CA certificates)
        if "ozone_ssl_truststore_location" in extra:
            env_vars["OZONE_SSL_TRUSTSTORE_LOCATION"] = extra["ozone_ssl_truststore_location"]
        if "ozone_ssl_truststore_password" in extra:
            password = get_secret_value(extra["ozone_ssl_truststore_password"], conn_id)
            mask_secret(password)
            env_vars["OZONE_SSL_TRUSTSTORE_PASSWORD"] = password
        if "ozone_ssl_truststore_type" in extra:
            env_vars["OZONE_SSL_TRUSTSTORE_TYPE"] = extra["ozone_ssl_truststore_type"]

    # Hive SSL Configuration
    # These map to hive-site.xml properties
    if extra.get("hive_ssl_enabled") == "true" or extra.get("hive.ssl.enabled") == "true":
        env_vars["HIVE_SSL_ENABLED"] = "true"

        if "hive_ssl_keystore_path" in extra:
            env_vars["HIVE_SSL_KEYSTORE_PATH"] = extra["hive_ssl_keystore_path"]
        if "hive_ssl_keystore_password" in extra:
            password = get_secret_value(extra["hive_ssl_keystore_password"], conn_id)
            mask_secret(password)
            env_vars["HIVE_SSL_KEYSTORE_PASSWORD"] = password
        if "hive_ssl_truststore_path" in extra:
            env_vars["HIVE_SSL_TRUSTSTORE_PATH"] = extra["hive_ssl_truststore_path"]
        if "hive_ssl_truststore_password" in extra:
            password = get_secret_value(extra["hive_ssl_truststore_password"], conn_id)
            mask_secret(password)
            env_vars["HIVE_SSL_TRUSTSTORE_PASSWORD"] = password

    # HDFS SSL Configuration
    # These map to core-site.xml and hdfs-site.xml properties
    if extra.get("hdfs_ssl_enabled") == "true" or extra.get("dfs.encrypt.data.transfer") == "true":
        env_vars["HDFS_SSL_ENABLED"] = "true"

        # Data transfer encryption
        if "dfs_encrypt_data_transfer" in extra:
            env_vars["DFS_ENCRYPT_DATA_TRANSFER"] = extra["dfs_encrypt_data_transfer"]
        if "dfs.encrypt.data.transfer" in extra:
            env_vars["DFS_ENCRYPT_DATA_TRANSFER"] = extra["dfs.encrypt.data.transfer"]

        # Keystore configuration
        if "hdfs_ssl_keystore_location" in extra:
            env_vars["HDFS_SSL_KEYSTORE_LOCATION"] = extra["hdfs_ssl_keystore_location"]
        if "hdfs_ssl_keystore_password" in extra:
            password = get_secret_value(extra["hdfs_ssl_keystore_password"], conn_id)
            mask_secret(password)
            env_vars["HDFS_SSL_KEYSTORE_PASSWORD"] = password
        if "hdfs_ssl_keystore_type" in extra:
            env_vars["HDFS_SSL_KEYSTORE_TYPE"] = extra["hdfs_ssl_keystore_type"]

        # Truststore configuration
        if "hdfs_ssl_truststore_location" in extra:
            env_vars["HDFS_SSL_TRUSTSTORE_LOCATION"] = extra["hdfs_ssl_truststore_location"]
        if "hdfs_ssl_truststore_password" in extra:
            password = get_secret_value(extra["hdfs_ssl_truststore_password"], conn_id)
            mask_secret(password)
            env_vars["HDFS_SSL_TRUSTSTORE_PASSWORD"] = password
        if "hdfs_ssl_truststore_type" in extra:
            env_vars["HDFS_SSL_TRUSTSTORE_TYPE"] = extra["hdfs_ssl_truststore_type"]

    return env_vars


def apply_ssl_env_vars(
    env_vars: dict[str, str], existing_env: dict[str, str] | None = None
) -> dict[str, str]:
    """
    Apply SSL environment variables to existing environment.

    :param env_vars: SSL environment variables to apply
    :param existing_env: Existing environment dictionary (defaults to os.environ.copy())
    :return: Updated environment dictionary
    """
    if existing_env is None:
        # Return only the env overrides (delta). Callers can merge into os.environ.copy().
        return env_vars.copy()

    env = existing_env.copy()
    env.update(env_vars)
    return env


def _kerberos_principal_keytab_from_extra(
    extra: dict, principal_key: str, keytab_key: str, conn_id: str | None
) -> None | tuple[str, str]:
    """Get (principal, keytab_path) from extra if both keys present; resolve keytab via get_secret_value."""
    if principal_key not in extra or keytab_key not in extra:
        return None
    keytab_path = get_secret_value(extra[keytab_key], conn_id)
    mask_secret(keytab_path)
    return (extra[principal_key], keytab_path)


def get_kerberos_env_vars(extra: dict, conn_id: str | None = None) -> dict[str, str]:
    """
    Extract Kerberos configuration from connection Extra and return environment variables.

    Supports Ozone, Hive, and HDFS Kerberos (principal/keytab and conf dir from extra).
    """
    env_vars: dict[str, str] = {}
    extra_lower = {k: str(v).lower() for k, v in extra.items() if v is not None}

    # Ozone/Hadoop core Kerberos
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

    # Hive
    if (
        extra_lower.get("hive_kerberos_enabled") == "true"
        or extra_lower.get("hive.security.authentication") == "kerberos"
    ):
        pair = _kerberos_principal_keytab_from_extra(
            extra, "hive_kerberos_principal", "hive_kerberos_keytab", conn_id
        )
        if pair:
            env_vars["HIVE_KERBEROS_PRINCIPAL"], env_vars["HIVE_KERBEROS_KEYTAB"] = pair

    # HDFS
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


def kinit_with_keytab(principal: str, keytab: str, krb5_conf: str | None = None) -> bool:
    """
    Perform Kerberos authentication using keytab file.

    Runs system ``kinit -kt keytab principal``. We do not use
    :func:`airflow.security.kerberos.renew_from_kt` because that is intended for the
    Airflow Kerberos daemon and requires [kerberos] config (ccache, kinit_path, etc.);
    here we need a one-time kinit driven by connection extra, with optional KRB5_CONFIG.

    :param principal: Kerberos principal (e.g., 'user@REALM.COM')
    :param keytab: Path to keytab file
    :param krb5_conf: Optional path to krb5.conf file
    :return: True if kinit succeeded, False otherwise
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


def apply_kerberos_env_vars(
    env_vars: dict[str, str], existing_env: dict[str, str] | None = None
) -> dict[str, str]:
    """
    Apply Kerberos environment variables to an environment.

    This function:
    - builds env overrides (delta) for Kerberos-related env vars,
    - configures JVM flags so Hadoop/Ozone clients use Kerberos,
    - ensures HADOOP_CONF_DIR / OZONE_CONF_DIR point to a config dir
      that should already contain core-site.xml and ozone-site.xml,
    - performs kinit if keytab and principal are provided.

    It does NOT create or modify any XML config files on disk.

    :param env_vars: Kerberos environment variables to apply
    :param existing_env: Existing environment dictionary (defaults to os.environ)
    :return: Environment overrides (delta) to merge into a process env
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
        # Append hadoop.security.authentication=kerberos to HADOOP_OPTS
        hadoop_opts = overrides.get("HADOOP_OPTS") or base_env.get("HADOOP_OPTS", "")
        hadoop_flag = "-Dhadoop.security.authentication=kerberos"
        if hadoop_flag not in hadoop_opts:
            hadoop_opts = (hadoop_opts + " " + hadoop_flag).strip()
        overrides["HADOOP_OPTS"] = hadoop_opts

        # Ensure Ozone clients know security is enabled and are forced to use Kerberos.
        ozone_opts = overrides.get("OZONE_OPTS") or base_env.get("OZONE_OPTS", "")
        ozone_flags = [
            "-Dhadoop.security.authentication=kerberos",
            "-Dozone.security.enabled=true",
        ]
        for flag in ozone_flags:
            if flag not in ozone_opts:
                ozone_opts = (ozone_opts + " " + flag).strip()
        overrides["OZONE_OPTS"] = ozone_opts

        # Ensure a config directory hint is set for clients.
        conf_dir = (
            overrides.get("OZONE_CONF_DIR")
            or overrides.get("HADOOP_CONF_DIR")
            or base_env.get("OZONE_CONF_DIR")
            or base_env.get("HADOOP_CONF_DIR")
            or "/opt/airflow/ozone-conf"
        )
        overrides.setdefault("HADOOP_CONF_DIR", conf_dir)
        overrides.setdefault("OZONE_CONF_DIR", conf_dir)

    # Perform kinit if keytab and principal are available
    if "KERBEROS_KEYTAB" in env_vars and "KERBEROS_PRINCIPAL" in env_vars:
        krb5_conf = env_vars.get("KRB5_CONFIG") or base_env.get("KRB5_CONFIG")
        kinit_with_keytab(env_vars["KERBEROS_PRINCIPAL"], env_vars["KERBEROS_KEYTAB"], krb5_conf)

    return overrides
