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

from dataclasses import dataclass

# ============================================================================
# Provider runtime parameters (constants only)
# ============================================================================
# This module is the single map of provider runtime settings:
# - default values
# - connection extra keys
# - SSL/Kerberos key groups and value mappings
#
# IMPORTANT:
# This file intentionally does NOT contain runtime orchestration logic:
# - no SSL env assembly
# - no Kerberos env assembly
# - no kinit/subprocess logic
# - no CLI execution logic
# ============================================================================

# --- Base connection parameters ------------------------------------------------
RETRY_ATTEMPTS = 3
FAST_TIMEOUT_SECONDS = 5 * 60
SLOW_TIMEOUT_SECONDS = 60 * 60

DEFAULT_OZONE_CONN_ID = "ozone_default"
DEFAULT_OZONE_ADMIN_CONN_ID = "ozone_admin_default"
OZONE_CONN_NAME_ATTR = "ozone_conn_id"

OZONE_CONNECTION_UI_FIELD_BEHAVIOUR: dict[str, object] = {
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

# --- SSL flags and value mappings ---------------------------------------------
OZONE_SSL_ENABLED_KEY = "ozone_security_enabled"
HDFS_SSL_ENABLED_KEY = "hdfs_ssl_enabled"

# Mapping format:
# (extra_key, ENV_VAR_NAME, is_secret)
OZONE_SSL_VALUE_MAP = (
    ("ozone_om_https_port", "OZONE_OM_HTTPS_PORT", False),
    ("ozone_ssl_keystore_location", "OZONE_SSL_KEYSTORE_LOCATION", False),
    ("ozone_ssl_keystore_password", "OZONE_SSL_KEYSTORE_PASSWORD", True),
    ("ozone_ssl_keystore_type", "OZONE_SSL_KEYSTORE_TYPE", False),
    ("ozone_ssl_truststore_location", "OZONE_SSL_TRUSTSTORE_LOCATION", False),
    ("ozone_ssl_truststore_password", "OZONE_SSL_TRUSTSTORE_PASSWORD", True),
    ("ozone_ssl_truststore_type", "OZONE_SSL_TRUSTSTORE_TYPE", False),
)

HDFS_SSL_VALUE_MAP = (
    ("dfs_encrypt_data_transfer", "DFS_ENCRYPT_DATA_TRANSFER", False),
    ("hdfs_ssl_keystore_location", "HDFS_SSL_KEYSTORE_LOCATION", False),
    ("hdfs_ssl_keystore_password", "HDFS_SSL_KEYSTORE_PASSWORD", True),
    ("hdfs_ssl_keystore_type", "HDFS_SSL_KEYSTORE_TYPE", False),
    ("hdfs_ssl_truststore_location", "HDFS_SSL_TRUSTSTORE_LOCATION", False),
    ("hdfs_ssl_truststore_password", "HDFS_SSL_TRUSTSTORE_PASSWORD", True),
    ("hdfs_ssl_truststore_type", "HDFS_SSL_TRUSTSTORE_TYPE", False),
)

# --- Kerberos flags and value mappings ----------------------------------------
OZONE_KERBEROS_ENABLED_KEY = "hadoop_security_authentication"
HDFS_KERBEROS_ENABLED_KEY = "hdfs_kerberos_enabled"

OZONE_KERBEROS_VALUE_MAP = (
    ("kerberos_principal", "KERBEROS_PRINCIPAL", False),
    ("kerberos_keytab", "KERBEROS_KEYTAB", True),
    ("krb5_conf", "KRB5_CONFIG", False),
    ("ozone_conf_dir", "OZONE_CONF_DIR", False),
)

HDFS_KERBEROS_VALUE_MAP = (
    ("hdfs_kerberos_principal", "HDFS_KERBEROS_PRINCIPAL", False),
    ("hdfs_kerberos_keytab", "HDFS_KERBEROS_KEYTAB", True),
)


@dataclass(frozen=True)
class OzoneConnSnapshot:
    """Lightweight connection snapshot to reduce repeated connection reads."""

    host: str
    port: int
    extra: dict[str, object]
