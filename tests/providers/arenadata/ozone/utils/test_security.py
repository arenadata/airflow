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

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.arenadata.ozone.utils.connection_schema import OzoneConnSnapshot
from airflow.providers.arenadata.ozone.utils.security import (
    KerberosConfig,
    SecretResolver,
    SSLConfig,
)


class TestGetSecretValue:
    """Tests for get_secret_value."""

    @patch("airflow.providers.arenadata.ozone.utils.security.ensure_secrets_loaded")
    def test_resolves_secret_uri_from_backend(self, mock_ensure_loaded):
        mock_backend = MagicMock()
        mock_backend.get_config.return_value = "resolved_secret"
        mock_ensure_loaded.return_value = [mock_backend]

        result = SecretResolver.get_secret_value("secret://vault/ozone/password", conn_id="ozone_default")

        assert result == "resolved_secret"
        mock_backend.get_config.assert_called_once_with("secret://vault/ozone/password")

    @patch("airflow.providers.arenadata.ozone.utils.security.ensure_secrets_loaded")
    def test_secret_not_found_raises(self, mock_ensure_loaded):
        mock_backend = MagicMock()
        mock_backend.get_config.return_value = None
        mock_ensure_loaded.return_value = [mock_backend]

        with pytest.raises(ValueError, match="Secret not found"):
            SecretResolver.get_secret_value("secret://missing/uri")

    def test_get_secret_value_masks_plain_value_when_requested(self, monkeypatch):
        masked: dict[str, object] = {}

        def _capture_masked(value: object) -> None:
            masked["value"] = value

        monkeypatch.setattr("airflow.providers.arenadata.ozone.utils.security.mask_secret", _capture_masked)
        result = SecretResolver.get_secret_value("plain_secret", mask=True)
        assert result == "plain_secret"
        assert masked["value"] == "plain_secret"


class TestGetKerberosEnvVars:
    """Tests for KerberosConfig.get_env_vars."""

    def test_empty_snapshot_returns_empty(self):
        snapshot = OzoneConnSnapshot(host="om", port=9862)
        assert KerberosConfig.get_env_vars(snapshot, scope="ozone") == {}

    def test_ozone_kerberos_from_snapshot(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hadoop_security_authentication="kerberos",
            kerberos_principal="user@REALM",
            kerberos_keytab="/etc/keytab/user.keytab",
        )
        result = KerberosConfig.get_env_vars(snapshot, scope="ozone")
        assert result["HADOOP_SECURITY_AUTHENTICATION"] == "kerberos"
        assert result["KERBEROS_PRINCIPAL"] == "user@REALM"
        assert result["KERBEROS_KEYTAB"] == "/etc/keytab/user.keytab"

    def test_ozone_scope_does_not_include_hdfs_kerberos_env(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hadoop_security_authentication="kerberos",
            kerberos_principal="user@REALM",
            kerberos_keytab="/etc/keytab/user.keytab",
            hdfs_kerberos_enabled=True,
            hdfs_kerberos_principal="hdfs@REALM",
            hdfs_kerberos_keytab="/etc/keytab/hdfs.keytab",
        )
        result = KerberosConfig.get_env_vars(snapshot, scope="ozone")
        assert "HDFS_KERBEROS_PRINCIPAL" not in result
        assert "HDFS_KERBEROS_KEYTAB" not in result

    def test_hdfs_scope_does_not_include_ozone_kerberos_env(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            hadoop_security_authentication="kerberos",
            kerberos_principal="user@REALM",
            kerberos_keytab="/etc/keytab/user.keytab",
            hdfs_kerberos_enabled=True,
            hdfs_kerberos_principal="hdfs@REALM",
            hdfs_kerberos_keytab="/etc/keytab/hdfs.keytab",
        )
        result = KerberosConfig.get_env_vars(snapshot, scope="hdfs")
        assert "HADOOP_SECURITY_AUTHENTICATION" not in result
        assert "KERBEROS_PRINCIPAL" not in result
        assert result["HDFS_KERBEROS_PRINCIPAL"] == "hdfs@REALM"
        assert result["HDFS_KERBEROS_KEYTAB"] == "/etc/keytab/hdfs.keytab"


class TestApplySslEnvVars:
    """Tests for SSLConfig.apply_ssl_env_vars."""

    def test_merges_into_existing(self):
        overrides = {"A": "1"}
        existing = {"B": "2"}
        result = SSLConfig.apply_ssl_env_vars(overrides, existing)
        assert result == {"A": "1", "B": "2"}

    def test_scope_ozone_does_not_include_hdfs_ssl_env(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            ozone_security_enabled=True,
            ozone_om_https_port="9879",
            hdfs_ssl_enabled=True,
            hdfs_ssl_keystore_location="/etc/hdfs.ks",
        )
        config = SSLConfig.from_snapshot(snapshot, scope="ozone")
        env = config.as_env()
        assert env["OZONE_SECURITY_ENABLED"] == "true"
        assert "HDFS_SSL_ENABLED" not in env

    def test_scope_hdfs_does_not_include_ozone_ssl_env(self):
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            ozone_security_enabled=True,
            ozone_om_https_port="9879",
            hdfs_ssl_enabled=True,
            hdfs_ssl_keystore_location="/etc/hdfs.ks",
        )
        config = SSLConfig.from_snapshot(snapshot, scope="hdfs")
        env = config.as_env()
        assert env["HDFS_SSL_ENABLED"] == "true"
        assert "OZONE_SECURITY_ENABLED" not in env


class TestApplyKerberosEnvVars:
    """Tests for KerberosConfig.apply_env_vars."""

    def test_sets_hadoop_opts_when_kerberos_enabled(self):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        result = KerberosConfig.apply_env_vars(env_vars)
        assert "-Dhadoop.security.authentication=kerberos" in result.get("HADOOP_OPTS", "")
        assert "-Dhadoop.security.authentication=kerberos" in result.get("OZONE_OPTS", "")

    def test_reuses_existing_ozone_conf_dir_when_kerberos_enabled(self):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        existing = {"OZONE_CONF_DIR": "/opt/airflow/ozone-conf"}
        result = KerberosConfig.apply_env_vars(env_vars, existing)
        assert result["HADOOP_CONF_DIR"] == "/opt/airflow/ozone-conf"
        assert result["OZONE_CONF_DIR"] == "/opt/airflow/ozone-conf"

    def test_does_not_inject_default_conf_dir(self):
        env_vars = {"HADOOP_SECURITY_AUTHENTICATION": "kerberos"}
        result = KerberosConfig.apply_env_vars(env_vars, existing_env={})
        assert "HADOOP_CONF_DIR" not in result
        assert "OZONE_CONF_DIR" not in result


class TestSnapshotDrivenKerberosRuntime:
    @patch("airflow.providers.arenadata.ozone.utils.security.FileHelper.is_readable_file", return_value=True)
    @patch("airflow.providers.arenadata.ozone.utils.security.KerberosCliRunner.run_kerberos")
    def test_kinit_timeout_from_snapshot(self, mock_run_kerberos, _mock_readable):
        mock_run_kerberos.return_value = True
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            kinit_timeout_seconds=42,
        )
        assert KerberosConfig.kinit_with_keytab(
            "user@REALM",
            "/tmp/user.keytab",
            "/tmp/krb5.conf",
            snapshot=snapshot,
        )
        assert mock_run_kerberos.call_args.kwargs["timeout"] == 42

    def test_check_config_files_exist_uses_snapshot_names(self, tmp_path: Path):
        (tmp_path / "custom-core.xml").write_text("", encoding="utf-8")
        (tmp_path / "custom-ozone.xml").write_text("", encoding="utf-8")
        snapshot = OzoneConnSnapshot(
            host="om",
            port=9862,
            core_site_xml="custom-core.xml",
            ozone_site_xml="custom-ozone.xml",
        )
        assert KerberosConfig.check_config_files_exist(str(tmp_path), snapshot=snapshot)
