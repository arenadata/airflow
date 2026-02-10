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

import pytest
from unittest.mock import MagicMock, patch

from airflow.providers.arenadata.hbase.thrift2_pool import (
    Thrift2ConnectionPool,
    get_or_create_thrift2_pool,
)


class TestKerberosPool:
    """Test Kerberos authentication in connection pool."""

    def test_pool_initialization_with_kerberos(self):
        """Test pool initialization with Kerberos parameters."""
        pool = Thrift2ConnectionPool(
            size=5,
            host="localhost",
            port=9090,
            auth_method="GSSAPI",
            kerberos_service_name="hbase"
        )
        
        assert pool.size == 5
        assert pool.host == "localhost"
        assert pool.port == 9090
        assert pool.auth_method == "GSSAPI"
        assert pool.kerberos_service_name == "hbase"

    def test_pool_initialization_with_custom_service_name(self):
        """Test pool initialization with custom Kerberos service name."""
        pool = Thrift2ConnectionPool(
            size=5,
            host="localhost",
            port=9090,
            auth_method="GSSAPI",
            kerberos_service_name="HTTP"
        )
        
        assert pool.kerberos_service_name == "HTTP"

    @patch("airflow.providers.arenadata.hbase.thrift2_pool.HBaseThrift2Client")
    def test_pool_creates_client_with_kerberos(self, mock_client):
        """Test that pool creates clients with Kerberos parameters."""
        mock_client_inst = MagicMock()
        mock_client.return_value = mock_client_inst
        
        pool = Thrift2ConnectionPool(
            size=5,
            host="localhost",
            port=9090,
            auth_method="GSSAPI",
            kerberos_service_name="hbase"
        )
        
        client = pool._create_connection()
        
        mock_client.assert_called_once_with(
            host="localhost",
            port=9090,
            timeout=30000,
            ssl_options=None,
            auth_method="GSSAPI",
            kerberos_service_name="hbase",
            retry_max_attempts=3,
            retry_delay=1.0,
            retry_backoff_factor=2.0
        )
        mock_client_inst.open.assert_called_once()

    @patch("airflow.providers.arenadata.hbase.thrift2_pool.HBaseThrift2Client")
    def test_pool_creates_client_with_kerberos_and_ssl(self, mock_client):
        """Test that pool creates clients with both Kerberos and SSL."""
        mock_client_inst = MagicMock()
        mock_client.return_value = mock_client_inst
        
        ssl_options = {"ca_certs": "/path/to/ca.crt", "validate": True}
        pool = Thrift2ConnectionPool(
            size=5,
            host="localhost",
            port=9090,
            ssl_options=ssl_options,
            auth_method="GSSAPI",
            kerberos_service_name="hbase"
        )
        
        client = pool._create_connection()
        
        call_kwargs = mock_client.call_args[1]
        assert call_kwargs["ssl_options"] == ssl_options
        assert call_kwargs["auth_method"] == "GSSAPI"
        assert call_kwargs["kerberos_service_name"] == "hbase"

    def test_get_or_create_pool_with_kerberos(self):
        """Test get_or_create_thrift2_pool with Kerberos parameters."""
        pool = get_or_create_thrift2_pool(
            conn_id="test_kerberos",
            pool_size=10,
            host="localhost",
            port=9090,
            auth_method="GSSAPI",
            kerberos_service_name="hbase"
        )
        
        assert pool.auth_method == "GSSAPI"
        assert pool.kerberos_service_name == "hbase"
        assert pool.size == 10

    def test_get_or_create_pool_returns_existing_pool(self):
        """Test that get_or_create_thrift2_pool returns existing pool."""
        pool1 = get_or_create_thrift2_pool(
            conn_id="test_existing",
            pool_size=10,
            host="localhost",
            port=9090,
            auth_method="GSSAPI"
        )
        
        pool2 = get_or_create_thrift2_pool(
            conn_id="test_existing",
            pool_size=20,  # Different size
            host="different",  # Different host
            port=9091,
            auth_method="GSSAPI"
        )
        
        # Should return the same pool instance
        assert pool1 is pool2

    @patch("airflow.providers.arenadata.hbase.thrift2_pool.HBaseThrift2Client")
    def test_pool_connection_context_with_kerberos(self, mock_client):
        """Test pool connection context manager with Kerberos."""
        mock_client_inst = MagicMock()
        mock_client.return_value = mock_client_inst
        
        pool = Thrift2ConnectionPool(
            size=5,
            host="localhost",
            port=9090,
            auth_method="GSSAPI",
            kerberos_service_name="hbase"
        )
        
        with pool.connection() as client:
            assert client == mock_client_inst
        
        # Verify client was created with Kerberos
        call_kwargs = mock_client.call_args[1]
        assert call_kwargs["auth_method"] == "GSSAPI"

    def test_pool_without_auth_method(self):
        """Test pool without auth_method uses None."""
        pool = Thrift2ConnectionPool(
            size=5,
            host="localhost",
            port=9090
        )
        
        assert pool.auth_method is None
        assert pool.kerberos_service_name == "hbase"  # Default value
