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

from airflow.providers.arenadata.ozone.utils import ozone_utils


class TestGetSecretValue:
    """Tests for get_secret_value."""

    def test_returns_empty_unchanged(self):
        assert ozone_utils.get_secret_value("") == ""

    def test_returns_plain_value_unchanged(self):
        assert ozone_utils.get_secret_value("plain") == "plain"
        assert ozone_utils.get_secret_value("/path/to/keytab") == "/path/to/keytab"

    @patch("airflow.providers.arenadata.ozone.utils.ozone_utils.ensure_secrets_loaded")
    def test_resolves_secret_uri_from_backend(self, mock_ensure_loaded):
        mock_backend = MagicMock()
        mock_backend.get_config.return_value = "resolved_secret"
        mock_ensure_loaded.return_value = [mock_backend]

        result = ozone_utils.get_secret_value("secret://vault/ozone/password", conn_id="ozone_default")

        assert result == "resolved_secret"
        mock_backend.get_config.assert_called_once_with("secret://vault/ozone/password")

    @patch("airflow.providers.arenadata.ozone.utils.ozone_utils.ensure_secrets_loaded")
    def test_secret_not_found_raises(self, mock_ensure_loaded):
        mock_backend = MagicMock()
        mock_backend.get_config.return_value = None
        mock_ensure_loaded.return_value = [mock_backend]

        with pytest.raises(ValueError, match="Secret not found"):
            ozone_utils.get_secret_value("secret://missing/uri")


class TestGetKerberosEnvVars:
    """Tests for get_kerberos_env_vars."""

    def test_empty_extra_returns_empty(self):
        assert ozone_utils.get_kerberos_env_vars({}) == {}

    def test_ozone_kerberos_from_extra(self):
        extra = {
            "hadoop_security_authentication": "kerberos",
            "kerberos_principal": "user@REALM",
            "kerberos_keytab": "/etc/keytab/user.keytab",
        }
        result = ozone_utils.get_kerberos_env_vars(extra)
        assert result["HADOOP_SECURITY_AUTHENTICATION"] == "kerberos"
        assert result["KERBEROS_PRINCIPAL"] == "user@REALM"
        assert result["KERBEROS_KEYTAB"] == "/etc/keytab/user.keytab"

    def test_ozone_kerberos_with_dot_key(self):
        extra = {"hadoop.security.authentication": "kerberos", "kerberos_principal": "u@R"}
        result = ozone_utils.get_kerberos_env_vars(extra)
        assert result["HADOOP_SECURITY_AUTHENTICATION"] == "kerberos"
        assert result["KERBEROS_PRINCIPAL"] == "u@R"

    def test_hive_kerberos_block(self):
        extra = {
            "hive_kerberos_enabled": "true",
            "hive_kerberos_principal": "hive@R",
            "hive_kerberos_keytab": "/path/hive.keytab",
        }
        result = ozone_utils.get_kerberos_env_vars(extra)
        assert result["HIVE_KERBEROS_PRINCIPAL"] == "hive@R"
        assert result["HIVE_KERBEROS_KEYTAB"] == "/path/hive.keytab"

    def test_hdfs_kerberos_block(self):
        extra = {
            "hdfs_kerberos_enabled": "true",
            "hdfs_kerberos_principal": "hdfs@R",
            "hdfs_kerberos_keytab": "/path/hdfs.keytab",
        }
        result = ozone_utils.get_kerberos_env_vars(extra)
        assert result["HDFS_KERBEROS_PRINCIPAL"] == "hdfs@R"
        assert result["HDFS_KERBEROS_KEYTAB"] == "/path/hdfs.keytab"


class TestApplySslEnvVars:
    """Tests for apply_ssl_env_vars."""

    def test_returns_copy_when_no_existing(self):
        env = {"OZONE_SECURITY_ENABLED": "true"}
        assert ozone_utils.apply_ssl_env_vars(env, None) == env
        assert ozone_utils.apply_ssl_env_vars(env, None) is not env

    def test_merges_into_existing(self):
        overrides = {"A": "1"}
        existing = {"B": "2"}
        result = ozone_utils.apply_ssl_env_vars(overrides, existing)
        assert result == {"A": "1", "B": "2"}


class TestApplyKerberosEnvVars:
    """Tests for apply_kerberos_env_vars."""

    def test_sets_hadoop_opts_when_kerberos_enabled(self):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        result = ozone_utils.apply_kerberos_env_vars(env_vars)
        assert "-Dhadoop.security.authentication=kerberos" in result.get("HADOOP_OPTS", "")
        assert "-Dhadoop.security.authentication=kerberos" in result.get("OZONE_OPTS", "")
        assert "HADOOP_CONF_DIR" in result or "OZONE_CONF_DIR" in result
