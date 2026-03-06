#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneCliHook
from airflow.providers.arenadata.ozone.utils.security import (
    KerberosConfig,
    SecretResolver,
    SSLConfig,
)


class TestGetSecretValue:
    """Tests for get_secret_value."""

    @patch("airflow.providers.arenadata.ozone.utils.security.secret_resolver.ensure_secrets_loaded")
    def test_resolves_secret_uri_from_backend(self, mock_ensure_loaded):
        mock_backend = MagicMock()
        mock_backend.get_config.return_value = "resolved_secret"
        mock_ensure_loaded.return_value = [mock_backend]

        result = SecretResolver.get_secret_value("secret://vault/ozone/password", conn_id="ozone_default")

        assert result == "resolved_secret"
        mock_backend.get_config.assert_called_once_with("secret://vault/ozone/password")

    @patch("airflow.providers.arenadata.ozone.utils.security.secret_resolver.ensure_secrets_loaded")
    def test_secret_not_found_raises(self, mock_ensure_loaded):
        mock_backend = MagicMock()
        mock_backend.get_config.return_value = None
        mock_ensure_loaded.return_value = [mock_backend]

        with pytest.raises(ValueError, match="Secret not found"):
            SecretResolver.get_secret_value("secret://missing/uri")


class TestGetKerberosEnvVars:
    """Tests for KerberosConfig.get_env_vars."""

    def test_empty_extra_returns_empty(self):
        assert KerberosConfig.get_env_vars({}) == {}

    def test_ozone_kerberos_from_extra(self):
        extra = {
            "hadoop_security_authentication": "kerberos",
            "kerberos_principal": "user@REALM",
            "kerberos_keytab": "/etc/keytab/user.keytab",
        }
        result = KerberosConfig.get_env_vars(extra)
        assert result["HADOOP_SECURITY_AUTHENTICATION"] == "kerberos"
        assert result["KERBEROS_PRINCIPAL"] == "user@REALM"
        assert result["KERBEROS_KEYTAB"] == "/etc/keytab/user.keytab"

    def test_ozone_kerberos_with_dot_key(self):
        extra = {"hadoop.security.authentication": "kerberos", "kerberos_principal": "u@R"}
        result = KerberosConfig.get_env_vars(extra)
        assert result["HADOOP_SECURITY_AUTHENTICATION"] == "kerberos"
        assert result["KERBEROS_PRINCIPAL"] == "u@R"

    def test_hive_kerberos_block(self):
        extra = {
            "hive_kerberos_enabled": "true",
            "hive_kerberos_principal": "hive@R",
            "hive_kerberos_keytab": "/path/hive.keytab",
        }
        result = KerberosConfig.get_env_vars(extra)
        assert result["HIVE_KERBEROS_PRINCIPAL"] == "hive@R"
        assert result["HIVE_KERBEROS_KEYTAB"] == "/path/hive.keytab"


class TestApplySslEnvVars:
    """Tests for SSLConfig.apply_ssl_env_vars."""

    def test_merges_into_existing(self):
        overrides = {"A": "1"}
        existing = {"B": "2"}
        result = SSLConfig.apply_ssl_env_vars(overrides, existing)
        assert result == {"A": "1", "B": "2"}


class TestApplyKerberosEnvVars:
    """Tests for KerberosConfig.apply_env_vars."""

    def test_sets_hadoop_opts_when_kerberos_enabled(self):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        result = KerberosConfig.apply_env_vars(env_vars)
        assert "-Dhadoop.security.authentication=kerberos" in result.get("HADOOP_OPTS", "")
        assert "-Dhadoop.security.authentication=kerberos" in result.get("OZONE_OPTS", "")

    def test_reuses_existing_conf_dir_when_kerberos_enabled(self):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        existing = {"HADOOP_CONF_DIR": "/opt/airflow/ozone-conf"}
        result = KerberosConfig.apply_env_vars(env_vars, existing)
        assert result["HADOOP_CONF_DIR"] == "/opt/airflow/ozone-conf"
        assert result["OZONE_CONF_DIR"] == "/opt/airflow/ozone-conf"

    def test_does_not_inject_default_conf_dir(self):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        result = KerberosConfig.apply_env_vars(env_vars, existing_env={})
        assert "HADOOP_CONF_DIR" not in result
        assert "OZONE_CONF_DIR" not in result


class TestOzoneCliHookConnectionSnapshot:
    """Tests for fail-fast host/port validation in OzoneCliHook."""

    def test_connection_snapshot_requires_host(self):
        conn = MagicMock()
        conn.host = None
        conn.port = 9862
        conn.extra_dejson = {}
        hook = OzoneCliHook(ozone_conn_id="ozone_default")
        hook.get_connection = lambda _: conn
        with pytest.raises(AirflowException, match="must define host"):
            _ = hook.connection_snapshot

    def test_connection_snapshot_requires_port(self):
        conn = MagicMock()
        conn.host = "om-host"
        conn.port = None
        conn.extra_dejson = {}
        hook = OzoneCliHook(ozone_conn_id="ozone_default")
        hook.get_connection = lambda _: conn
        with pytest.raises(AirflowException, match="must define port"):
            _ = hook.connection_snapshot
