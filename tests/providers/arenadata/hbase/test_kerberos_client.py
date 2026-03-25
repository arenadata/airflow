#
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

import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock sasl module before importing client
sys.modules["sasl"] = MagicMock()
sys.modules["thrift_sasl"] = MagicMock()

from airflow.providers.arenadata.hbase.client.thrift2_client import HBaseThrift2Client


class TestKerberosAuthentication:
    """Test Kerberos authentication functionality."""

    def test_client_initialization_with_kerberos(self):
        """Test client initialization with Kerberos parameters."""
        client = HBaseThrift2Client(
            host="localhost", port=9090, auth_method="GSSAPI", kerberos_service_name="hbase"
        )

        assert client.config.host == "localhost"
        assert client.config.port == 9090
        assert client.config.auth_method == "GSSAPI"
        assert client.config.kerberos_service_name == "hbase"

    def test_client_initialization_with_custom_service_name(self):
        """Test client initialization with custom Kerberos service name."""
        client = HBaseThrift2Client(
            host="localhost", port=9090, auth_method="GSSAPI", kerberos_service_name="HTTP"
        )

        assert client.config.kerberos_service_name == "HTTP"

    @patch("airflow.providers.arenadata.hbase.client.thrift2_client.SASL_AVAILABLE", False)
    def test_kerberos_without_thrift_sasl_raises_error(self):
        """Test that using Kerberos without thrift_sasl raises ImportError."""
        with pytest.raises(ImportError, match="thrift_sasl and sasl libraries are required"):
            HBaseThrift2Client(host="localhost", port=9090, auth_method="GSSAPI")

    @patch("airflow.providers.arenadata.hbase.client.thrift2_client.SASL_AVAILABLE", True)
    @patch("airflow.providers.arenadata.hbase.client.thrift2_client.subprocess.run")
    @patch("airflow.providers.arenadata.hbase.client.thrift2_client.TSaslClientTransport")
    @patch("airflow.providers.arenadata.hbase.client.thrift2_client.THBaseService")
    @patch("airflow.providers.arenadata.hbase.client.thrift2_client.TBinaryProtocol")
    @patch("airflow.providers.arenadata.hbase.client.thrift2_client.TSocket")
    def test_open_connection_with_kerberos(
        self, mock_socket, mock_protocol, mock_service, mock_sasl_transport, mock_subprocess
    ):
        """Test opening connection with Kerberos authentication."""
        mock_socket_inst = MagicMock()
        mock_socket.TSocket.return_value = mock_socket_inst

        mock_sasl_transport_inst = MagicMock()
        mock_sasl_transport.return_value = mock_sasl_transport_inst

        mock_protocol_inst = MagicMock()
        mock_protocol.TBinaryProtocol.return_value = mock_protocol_inst

        mock_client = MagicMock()
        mock_service.Client.return_value = mock_client

        mock_subprocess.return_value = MagicMock(returncode=0)

        client = HBaseThrift2Client(
            host="localhost",
            port=9090,
            auth_method="GSSAPI",
            kerberos_service_name="hbase",
            kerberos_keytab="/path/to/keytab",
        )
        client.open()

        mock_socket.TSocket.assert_called_once_with("localhost", 9090)
        mock_socket_inst.setTimeout.assert_called_once_with(30000)
        mock_sasl_transport.assert_called_once()
        mock_sasl_transport_inst.open.assert_called_once()
        assert client._client == mock_client

    def test_no_auth_method_uses_regular_transport(self):
        """Test that no auth_method uses regular transport, not SASL."""
        with patch("airflow.providers.arenadata.hbase.client.thrift2_client.TSocket") as mock_socket:
            with patch(
                "airflow.providers.arenadata.hbase.client.thrift2_client.TTransport"
            ) as mock_transport:
                with patch("airflow.providers.arenadata.hbase.client.thrift2_client.TBinaryProtocol"):
                    with patch("airflow.providers.arenadata.hbase.client.thrift2_client.THBaseService"):
                        mock_socket_inst = MagicMock()
                        mock_socket.TSocket.return_value = mock_socket_inst

                        mock_transport_inst = MagicMock()
                        mock_transport.TBufferedTransport.return_value = mock_transport_inst

                        client = HBaseThrift2Client(host="localhost", port=9090)
                        client.open()

                        # Verify regular transport was used, not SASL
                        mock_transport.TBufferedTransport.assert_called_once_with(mock_socket_inst)
