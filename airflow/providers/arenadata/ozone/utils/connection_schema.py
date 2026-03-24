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

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.providers.arenadata.ozone.utils.helpers import (
    JsonDict,
    TypeNormalizationHelper,
)


# ============================================================================
# Ozone connection schema and runtime defaults
# ============================================================================
# This module is the single source of truth for:
# - connection schema (canonical keys)
# - defaults
# - connection UI behaviour
# - OzoneConnSnapshot typed contract
# ============================================================================

# --- Base runtime parameters --------------------------------------------------
RETRY_ATTEMPTS = 3
FAST_TIMEOUT_SECONDS = 5 * 60
SLOW_TIMEOUT_SECONDS = 60 * 60
KINIT_TIMEOUT_SECONDS = 5 * 60
CORE_SITE_XML = "core-site.xml"
OZONE_SITE_XML = "ozone-site.xml"
MAX_CONTENT_SIZE_BYTES = 1024 * 1024 * 1024

OZONE_CONNECTION_UI_FIELD_BEHAVIOUR: JsonDict = {
    "hidden_fields": ["schema"],
    "relabeling": {"host": "Ozone OM Host", "port": "Ozone OM Port"},
    "placeholders": {
        "host": "ozone-om",
        "port": "9862",
        "extra": (
            '{"ozone_security_enabled": "true", '
            '"hadoop_security_authentication": "kerberos", "kerberos_principal": "user@REALM", '
            '"kerberos_keytab": "/opt/airflow/keytabs/user.keytab", '
            '"krb5_conf": "/opt/airflow/kerberos-config/krb5.conf", '
            '"ozone_conf_dir": "/opt/airflow/ozone-conf"}'
        ),
    },
}


@dataclass(frozen=True)
class OzoneConnSnapshot:
    """Strict typed snapshot of all supported connection parameters."""

    # Connection extra keys.
    OZONE_SECURITY_ENABLED_KEY = "ozone_security_enabled"
    HDFS_SSL_ENABLED_KEY = "hdfs_ssl_enabled"
    OZONE_OM_HTTPS_PORT_KEY = "ozone_om_https_port"
    OZONE_SSL_KEYSTORE_LOCATION_KEY = "ozone_ssl_keystore_location"
    OZONE_SSL_KEYSTORE_PASSWORD_KEY = "ozone_ssl_keystore_password"
    OZONE_SSL_KEYSTORE_TYPE_KEY = "ozone_ssl_keystore_type"
    OZONE_SSL_TRUSTSTORE_LOCATION_KEY = "ozone_ssl_truststore_location"
    OZONE_SSL_TRUSTSTORE_PASSWORD_KEY = "ozone_ssl_truststore_password"
    OZONE_SSL_TRUSTSTORE_TYPE_KEY = "ozone_ssl_truststore_type"
    DFS_ENCRYPT_DATA_TRANSFER_KEY = "dfs_encrypt_data_transfer"
    HDFS_SSL_KEYSTORE_LOCATION_KEY = "hdfs_ssl_keystore_location"
    HDFS_SSL_KEYSTORE_PASSWORD_KEY = "hdfs_ssl_keystore_password"
    HDFS_SSL_KEYSTORE_TYPE_KEY = "hdfs_ssl_keystore_type"
    HDFS_SSL_TRUSTSTORE_LOCATION_KEY = "hdfs_ssl_truststore_location"
    HDFS_SSL_TRUSTSTORE_PASSWORD_KEY = "hdfs_ssl_truststore_password"
    HDFS_SSL_TRUSTSTORE_TYPE_KEY = "hdfs_ssl_truststore_type"
    HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop_security_authentication"
    HDFS_KERBEROS_ENABLED_KEY = "hdfs_kerberos_enabled"
    KERBEROS_PRINCIPAL_KEY = "kerberos_principal"
    KERBEROS_KEYTAB_KEY = "kerberos_keytab"
    KRB5_CONF_KEY = "krb5_conf"
    OZONE_CONF_DIR_KEY = "ozone_conf_dir"
    HDFS_KERBEROS_PRINCIPAL_KEY = "hdfs_kerberos_principal"
    HDFS_KERBEROS_KEYTAB_KEY = "hdfs_kerberos_keytab"
    KINIT_TIMEOUT_SECONDS_KEY = "kinit_timeout_seconds"
    CORE_SITE_XML_KEY = "core_site_xml"
    OZONE_SITE_XML_KEY = "ozone_site_xml"
    MAX_CONTENT_SIZE_BYTES_KEY = "max_content_size_bytes"
    OZONE_SSL_ENV_MAPPING = (
        ("ozone_om_https_port", "OZONE_OM_HTTPS_PORT", False),
        ("ozone_ssl_keystore_location", "OZONE_SSL_KEYSTORE_LOCATION", False),
        ("ozone_ssl_keystore_password", "OZONE_SSL_KEYSTORE_PASSWORD", True),
        ("ozone_ssl_keystore_type", "OZONE_SSL_KEYSTORE_TYPE", False),
        ("ozone_ssl_truststore_location", "OZONE_SSL_TRUSTSTORE_LOCATION", False),
        ("ozone_ssl_truststore_password", "OZONE_SSL_TRUSTSTORE_PASSWORD", True),
        ("ozone_ssl_truststore_type", "OZONE_SSL_TRUSTSTORE_TYPE", False),
    )
    HDFS_SSL_ENV_MAPPING = (
        ("dfs_encrypt_data_transfer", "DFS_ENCRYPT_DATA_TRANSFER", False),
        ("hdfs_ssl_keystore_location", "HDFS_SSL_KEYSTORE_LOCATION", False),
        ("hdfs_ssl_keystore_password", "HDFS_SSL_KEYSTORE_PASSWORD", True),
        ("hdfs_ssl_keystore_type", "HDFS_SSL_KEYSTORE_TYPE", False),
        ("hdfs_ssl_truststore_location", "HDFS_SSL_TRUSTSTORE_LOCATION", False),
        ("hdfs_ssl_truststore_password", "HDFS_SSL_TRUSTSTORE_PASSWORD", True),
        ("hdfs_ssl_truststore_type", "HDFS_SSL_TRUSTSTORE_TYPE", False),
    )
    OZONE_KERBEROS_ENV_MAPPING = (
        ("kerberos_principal", "KERBEROS_PRINCIPAL", False),
        ("kerberos_keytab", "KERBEROS_KEYTAB", True),
        ("krb5_conf", "KRB5_CONFIG", False),
        ("ozone_conf_dir", "OZONE_CONF_DIR", False),
    )
    HDFS_KERBEROS_ENV_MAPPING = (
        ("hdfs_kerberos_principal", "HDFS_KERBEROS_PRINCIPAL", False),
        ("hdfs_kerberos_keytab", "HDFS_KERBEROS_KEYTAB", True),
    )

    host: str
    port: int
    ozone_security_enabled: bool = False
    hdfs_ssl_enabled: bool = False
    ozone_om_https_port: str | None = None
    ozone_ssl_keystore_location: str | None = None
    ozone_ssl_keystore_password: str | None = None
    ozone_ssl_keystore_type: str | None = None
    ozone_ssl_truststore_location: str | None = None
    ozone_ssl_truststore_password: str | None = None
    ozone_ssl_truststore_type: str | None = None
    dfs_encrypt_data_transfer: str | None = None
    hdfs_ssl_keystore_location: str | None = None
    hdfs_ssl_keystore_password: str | None = None
    hdfs_ssl_keystore_type: str | None = None
    hdfs_ssl_truststore_location: str | None = None
    hdfs_ssl_truststore_password: str | None = None
    hdfs_ssl_truststore_type: str | None = None
    hadoop_security_authentication: str | None = None
    hdfs_kerberos_enabled: bool = False
    kerberos_principal: str | None = None
    kerberos_keytab: str | None = None
    krb5_conf: str | None = None
    ozone_conf_dir: str | None = None
    hdfs_kerberos_principal: str | None = None
    hdfs_kerberos_keytab: str | None = None
    kinit_timeout_seconds: int = KINIT_TIMEOUT_SECONDS
    core_site_xml: str = CORE_SITE_XML
    ozone_site_xml: str = OZONE_SITE_XML
    max_content_size_bytes: int = MAX_CONTENT_SIZE_BYTES
    # Raw connection extra is exposed for DAG-level custom extensions when
    # developers need to read additional non-provider keys directly.
    raw_extra: JsonDict = field(default_factory=dict)

    @property
    def ssl_enabled(self) -> bool:
        """Return True when Ozone CLI should run with TLS enabled."""
        return self.ozone_security_enabled

    @property
    def kerberos_enabled(self) -> bool:
        """Return True when Ozone CLI should run with Kerberos auth enabled."""
        auth = (self.hadoop_security_authentication or "").strip().lower()
        return auth == "kerberos"

    @classmethod
    def from_connection(
        cls,
        conn: Connection,
        *,
        conn_id: str,
        require_host_port: bool = True,
    ) -> OzoneConnSnapshot:
        """Create a typed snapshot from Connection model and strict canonical keys."""
        raw_host = getattr(conn, "host", None)
        raw_port = getattr(conn, "port", None)
        if require_host_port:
            if not raw_host or not str(raw_host).strip():
                raise AirflowException(f"Connection '{conn_id}' must define host for Ozone CLI operations.")
            if raw_port in (None, ""):
                raise AirflowException(f"Connection '{conn_id}' must define port for Ozone CLI operations.")

        extra = TypeNormalizationHelper.ensure_json_dict(conn.extra_dejson)
        normalize_flag_bool = TypeNormalizationHelper.normalize_flag_bool
        normalize_optional_str = TypeNormalizationHelper.normalize_optional_str
        parse_positive_int = TypeNormalizationHelper.parse_positive_int
        return cls(
            host=str(raw_host).strip() if raw_host else "",
            port=int(raw_port) if raw_port not in (None, "") else 0,
            ozone_security_enabled=normalize_flag_bool(
                extra.get(cls.OZONE_SECURITY_ENABLED_KEY),
                default=False,
            ),
            hdfs_ssl_enabled=normalize_flag_bool(
                extra.get(cls.HDFS_SSL_ENABLED_KEY),
                default=False,
            ),
            ozone_om_https_port=normalize_optional_str(extra.get(cls.OZONE_OM_HTTPS_PORT_KEY)),
            ozone_ssl_keystore_location=normalize_optional_str(
                extra.get(cls.OZONE_SSL_KEYSTORE_LOCATION_KEY)
            ),
            ozone_ssl_keystore_password=normalize_optional_str(
                extra.get(cls.OZONE_SSL_KEYSTORE_PASSWORD_KEY)
            ),
            ozone_ssl_keystore_type=normalize_optional_str(extra.get(cls.OZONE_SSL_KEYSTORE_TYPE_KEY)),
            ozone_ssl_truststore_location=normalize_optional_str(
                extra.get(cls.OZONE_SSL_TRUSTSTORE_LOCATION_KEY)
            ),
            ozone_ssl_truststore_password=normalize_optional_str(
                extra.get(cls.OZONE_SSL_TRUSTSTORE_PASSWORD_KEY)
            ),
            ozone_ssl_truststore_type=normalize_optional_str(extra.get(cls.OZONE_SSL_TRUSTSTORE_TYPE_KEY)),
            dfs_encrypt_data_transfer=normalize_optional_str(extra.get(cls.DFS_ENCRYPT_DATA_TRANSFER_KEY)),
            hdfs_ssl_keystore_location=normalize_optional_str(extra.get(cls.HDFS_SSL_KEYSTORE_LOCATION_KEY)),
            hdfs_ssl_keystore_password=normalize_optional_str(extra.get(cls.HDFS_SSL_KEYSTORE_PASSWORD_KEY)),
            hdfs_ssl_keystore_type=normalize_optional_str(extra.get(cls.HDFS_SSL_KEYSTORE_TYPE_KEY)),
            hdfs_ssl_truststore_location=normalize_optional_str(
                extra.get(cls.HDFS_SSL_TRUSTSTORE_LOCATION_KEY)
            ),
            hdfs_ssl_truststore_password=normalize_optional_str(
                extra.get(cls.HDFS_SSL_TRUSTSTORE_PASSWORD_KEY)
            ),
            hdfs_ssl_truststore_type=normalize_optional_str(extra.get(cls.HDFS_SSL_TRUSTSTORE_TYPE_KEY)),
            hadoop_security_authentication=normalize_optional_str(
                extra.get(cls.HADOOP_SECURITY_AUTHENTICATION_KEY)
            ),
            hdfs_kerberos_enabled=normalize_flag_bool(
                extra.get(cls.HDFS_KERBEROS_ENABLED_KEY),
                default=False,
            ),
            kerberos_principal=normalize_optional_str(extra.get(cls.KERBEROS_PRINCIPAL_KEY)),
            kerberos_keytab=normalize_optional_str(extra.get(cls.KERBEROS_KEYTAB_KEY)),
            krb5_conf=normalize_optional_str(extra.get(cls.KRB5_CONF_KEY)),
            ozone_conf_dir=normalize_optional_str(extra.get(cls.OZONE_CONF_DIR_KEY)),
            hdfs_kerberos_principal=normalize_optional_str(extra.get(cls.HDFS_KERBEROS_PRINCIPAL_KEY)),
            hdfs_kerberos_keytab=normalize_optional_str(extra.get(cls.HDFS_KERBEROS_KEYTAB_KEY)),
            kinit_timeout_seconds=parse_positive_int(
                extra.get(cls.KINIT_TIMEOUT_SECONDS_KEY),
                KINIT_TIMEOUT_SECONDS,
            ),
            core_site_xml=(normalize_optional_str(extra.get(cls.CORE_SITE_XML_KEY)) or CORE_SITE_XML),
            ozone_site_xml=(normalize_optional_str(extra.get(cls.OZONE_SITE_XML_KEY)) or OZONE_SITE_XML),
            max_content_size_bytes=parse_positive_int(
                extra.get(cls.MAX_CONTENT_SIZE_BYTES_KEY),
                MAX_CONTENT_SIZE_BYTES,
            ),
            raw_extra=dict(extra),
        )
