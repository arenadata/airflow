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
from unittest.mock import MagicMock, patch, PropertyMock

from airflow.models import Connection
from airflow.providers.arenadata.hbase.hooks.hbase import HBaseThriftHook


class TestKerberosHook:
    """Test Kerberos authentication in HBaseThriftHook."""

    @patch("airflow.providers.arenadata.hbase.hooks.hbase.HBaseThrift2Client")
    @patch.object(HBaseThriftHook, "get_connection")
    def test_hook_extracts_kerberos_settings(self, mock_get_conn, mock_client):
        """Test that hook extracts Kerberos settings from connection extra."""
        mock_conn = Connection(
            conn_id="hbase_kerberos",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"auth_method": "GSSAPI", "kerberos_service_name": "hbase"}'
        )
        mock_get_conn.return_value = mock_conn
        
        mock_client_inst = MagicMock()
        mock_client.return_value = mock_client_inst
        
        hook = HBaseThriftHook(hbase_conn_id="hbase_kerberos")
        hook._get_strategy()
        
        # Verify client was created with Kerberos parameters
        mock_client.assert_called_once()
        call_kwargs = mock_client.call_args[1]
        assert call_kwargs["auth_method"] == "GSSAPI"
        assert call_kwargs["kerberos_service_name"] == "hbase"

    @patch("airflow.providers.arenadata.hbase.hooks.hbase.get_or_create_thrift2_pool")
    @patch.object(HBaseThriftHook, "get_connection")
    def test_hook_with_kerberos_and_pool(self, mock_get_conn, mock_pool):
        """Test hook with Kerberos and connection pooling."""
        mock_conn = Connection(
            conn_id="hbase_kerberos_pool",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"auth_method": "GSSAPI", "kerberos_service_name": "hbase", "connection_pool": {"enabled": true, "size": 10}}'
        )
        mock_get_conn.return_value = mock_conn
        
        mock_pool_inst = MagicMock()
        mock_pool.return_value = mock_pool_inst
        
        hook = HBaseThriftHook(hbase_conn_id="hbase_kerberos_pool")
        hook._get_strategy()
        
        # Verify pool was created with Kerberos parameters
        mock_pool.assert_called_once()
        call_args = mock_pool.call_args
        # Check positional args
        assert call_args[0][0] == "hbase_kerberos_pool"
        assert call_args[0][1] == 10
        # Check keyword args
        assert call_args[1]["auth_method"] == "GSSAPI"
        assert call_args[1]["kerberos_service_name"] == "hbase"

    @patch("airflow.providers.arenadata.hbase.hooks.hbase.HBaseThrift2Client")
    @patch.object(HBaseThriftHook, "get_connection")
    def test_hook_logs_kerberos_authentication(self, mock_get_conn, mock_client):
        """Test that hook logs when Kerberos authentication is enabled."""
        mock_conn = Connection(
            conn_id="hbase_kerberos",
            conn_type="hbase",
            host="localhost",
            port=9090,
            extra='{"auth_method": "GSSAPI", "kerberos_service_name": "hbase"}'
        )
        mock_get_conn.return_value = mock_conn
        
        mock_client_inst = MagicMock()
        mock_client.return_value = mock_client_inst
        
        hook = HBaseThriftHook(hbase_conn_id="hbase_kerberos")
        
        # Mock log as property
        mock_log = MagicMock()
        type(hook).log = PropertyMock(return_value=mock_log)
        
        hook._get_strategy()
        
        # Verify authentication was logged
        mock_log.info.assert_any_call(
            "Authentication enabled: %s (service: %s)", "GSSAPI", "hbase"
        )
