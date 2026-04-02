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

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.hooks.ozone import (
    OzoneAdminExtraHook,
    OzoneAdminHook,
    OzoneCliHook,
    OzoneFsHook,
)
from airflow.providers.arenadata.ozone.utils.errors import OzoneCliError

MOCK_CLI_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.OzoneCliHook.run_cli"
MOCK_RUN_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.OzoneCliRunner.run_ozone_once"
MOCK_RUN_RETRY_PATH = "airflow.providers.arenadata.ozone.hooks.ozone.OzoneCliRunner.run_ozone"


@pytest.fixture
def admin_hook():
    hook = OzoneAdminHook(ozone_conn_id="test_admin_conn")
    conn = MagicMock()
    conn.host = "ozone-om"
    conn.port = 9862
    conn.extra_dejson = {}
    hook.get_connection = lambda _: conn
    return hook


@pytest.fixture
def admin_extra_hook():
    hook = OzoneAdminExtraHook(ozone_conn_id="test_admin_conn")
    conn = MagicMock()
    conn.host = "ozone-om"
    conn.port = 9862
    conn.extra_dejson = {}
    hook.get_connection = lambda _: conn
    return hook


@pytest.fixture
def ozone_fs_hook():
    hook = OzoneFsHook(ozone_conn_id="test_conn")
    conn = MagicMock()
    conn.host = "ozone-om"
    conn.port = 9862
    conn.extra_dejson = {}
    hook.get_connection = lambda _: conn
    return hook


class TestOzoneCliHookConnectionSnapshot:
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

    def test_prepared_cli_env_uses_ozone_conf_dir_from_connection_extra(self):
        conn = MagicMock()
        conn.host = "om-host"
        conn.port = 9862
        conn.extra_dejson = {"ozone_conf_dir": "/opt/airflow/ozone-conf"}
        hook = OzoneCliHook(ozone_conn_id="ozone_default")
        hook.get_connection = lambda _: conn

        env = hook._prepared_cli_env()
        assert env["OZONE_CONF_DIR"] == "/opt/airflow/ozone-conf"
        assert env["HADOOP_CONF_DIR"] == "/opt/airflow/ozone-conf"

    @pytest.mark.parametrize(
        ("extra", "expected_mode"),
        [
            ({}, "plain"),
            ({"ozone_security_enabled": "true"}, "ssl"),
            (
                {
                    "hadoop_security_authentication": "kerberos",
                    "kerberos_principal": "airflow@EXAMPLE.COM",
                    "kerberos_keytab": "/tmp/airflow.keytab",
                    "krb5_conf": "/etc/krb5.conf",
                },
                "kerberos",
            ),
            (
                {
                    "ozone_security_enabled": "true",
                    "hadoop_security_authentication": "kerberos",
                    "kerberos_principal": "airflow@EXAMPLE.COM",
                    "kerberos_keytab": "/tmp/airflow.keytab",
                    "krb5_conf": "/etc/krb5.conf",
                },
                "ssl+kerberos",
            ),
        ],
    )
    @patch("airflow.providers.arenadata.ozone.hooks.ozone.OzoneCliRunner.run_ozone")
    @patch("airflow.providers.arenadata.ozone.hooks.ozone.KerberosConfig.ensure_ticket")
    def test_run_cli_logs_runtime_mode(
        self,
        mock_ensure_ticket: MagicMock,
        mock_run_ozone: MagicMock,
        extra: dict[str, str],
        expected_mode: str,
        caplog,
    ) -> None:
        conn = MagicMock()
        conn.host = "om-host"
        conn.port = 9862
        conn.extra_dejson = extra
        hook = OzoneCliHook(ozone_conn_id="ozone_default")
        hook.get_connection = lambda _: conn

        mock_ensure_ticket.return_value = False
        mock_run_ozone.return_value = subprocess.CompletedProcess(
            args=["ozone", "sh", "volume", "list", "/"],
            returncode=0,
            stdout="[]",
            stderr="",
        )

        with caplog.at_level("INFO"):
            hook.run_cli(["ozone", "sh", "volume", "list", "/"], log_output=False)

        assert f"mode: {expected_mode}" in caplog.text


class TestOzoneAdminHook:
    @patch(MOCK_RUN_RETRY_PATH)
    def test_create_volume_uses_run_with_retry(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        mock_run_with_retry.return_value = MagicMock(returncode=0, stdout="", stderr="")
        admin_hook.create_volume(volume_name="test_vol")
        mock_run_with_retry.assert_called_once()
        assert mock_run_with_retry.call_args.args[0] == ["ozone", "sh", "volume", "create", "/test_vol"]

    @patch(MOCK_RUN_RETRY_PATH)
    def test_create_volume_already_exists_is_idempotent(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        mock_run_with_retry.side_effect = OzoneCliError(
            "VOLUME_ALREADY_EXISTS",
            command=["ozone", "sh", "volume", "create", "/test_vol"],
            stderr="VOLUME_ALREADY_EXISTS",
            returncode=1,
            retryable=False,
        )
        admin_hook.create_volume(volume_name="test_vol")
        mock_run_with_retry.assert_called_once()

    @patch(MOCK_RUN_RETRY_PATH)
    def test_create_bucket_raises_on_non_idempotent_failure(
        self, mock_run_with_retry: MagicMock, admin_hook: OzoneAdminHook
    ):
        mock_run_with_retry.side_effect = OzoneCliError(
            "ACCESS_DENIED",
            command=["ozone", "sh", "bucket", "create", "/test_vol/test_bkt", "--quota", "100GB"],
            stderr="ACCESS_DENIED",
            returncode=1,
            retryable=False,
        )
        with pytest.raises(AirflowException, match="Ozone command failed"):
            admin_hook.create_bucket(volume_name="test_vol", bucket_name="test_bkt", quota="100GB")

    @patch(MOCK_CLI_PATH)
    def test_get_container_report(self, mock_run_cli: MagicMock, admin_extra_hook: OzoneAdminExtraHook):
        mock_run_cli.return_value = {"total": 1, "containers": [{"id": 1}]}
        result = admin_extra_hook.get_container_report()
        mock_run_cli.assert_called_once_with(
            ["ozone", "admin", "container", "report", "--json"],
            timeout=3600,
            return_json_result=True,
        )
        assert result["total"] == 1
        assert len(result["containers"]) == 1


class TestOzoneFsHook:
    @patch(MOCK_RUN_RETRY_PATH)
    def test_exists_false(self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook):
        mock_run_cli.side_effect = OzoneCliError(
            "not found",
            command=["ozone", "fs", "-test", "-e", "ofs://path/does_not_exist"],
            stderr="not found",
            returncode=1,
            retryable=False,
        )
        assert ozone_fs_hook.exists("ofs://path/does_not_exist") is False
        mock_run_cli.assert_called_once()
        command = mock_run_cli.call_args.args[0]
        assert command[:4] == ["ozone", "fs", "-test", "-e"]
        assert command[-1] == "ofs://path/does_not_exist"

    @patch(MOCK_RUN_RETRY_PATH)
    def test_exists_raises_when_cli_missing(self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook):
        mock_run_cli.side_effect = OzoneCliError("Ozone CLI not found.", retryable=False)
        with pytest.raises(AirflowException):
            ozone_fs_hook.exists("ofs://path/any")

    @patch(MOCK_CLI_PATH)
    def test_upload_key_raises_when_file_missing(
        self, mock_run_cli: MagicMock, ozone_fs_hook: OzoneFsHook, tmp_path
    ):
        missing_path = tmp_path / "missing.txt"
        with pytest.raises(AirflowException):
            ozone_fs_hook.upload_key(str(missing_path), "ofs://vol1/bucket1/file.txt")
        mock_run_cli.assert_not_called()

    @patch(MOCK_RUN_RETRY_PATH)
    def test_test_connection_failure(self, mock_run_cli_check: MagicMock, ozone_fs_hook: OzoneFsHook):
        mock_run_cli_check.return_value = MagicMock(returncode=1, stdout="", stderr="auth failed")
        ok, message = ozone_fs_hook.test_connection()
        assert ok is False
        assert "auth failed" in message

    @patch(MOCK_RUN_RETRY_PATH)
    def test_test_connection_timeout(self, mock_run_cli_check: MagicMock, ozone_fs_hook: OzoneFsHook):
        mock_run_cli_check.side_effect = OzoneCliError("Ozone command timed out", retryable=True)
        ok, message = ozone_fs_hook.test_connection()
        assert ok is False
        assert "connection test failed" in message

    def test_get_key_property_warns_when_replication_config_missing(self, ozone_fs_hook: OzoneFsHook, caplog):
        with patch.object(
            ozone_fs_hook,
            "get_key_info",
            return_value={"name": "file.txt", "replicationType": "RATIS", "replicationFactor": 3},
        ):
            with caplog.at_level("WARNING"):
                result = ozone_fs_hook.get_key_property("ofs://vol1/b1/file.txt")
        assert result["replication_type"] == "RATIS"
        assert result["replication"] == 3
        assert "does not contain 'replicationConfig'" in caplog.text
