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
"""HBase Thrift2 client implementation."""

from __future__ import annotations

import logging
import time
from typing import Any

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport, TSSLSocket

try:
    from thrift_sasl import TSaslClientTransport
    SASL_AVAILABLE = True
except ImportError:
    SASL_AVAILABLE = False

from airflow.providers.arenadata.hbase.hbase_thrift2_generated import THBaseService, ttypes

logger = logging.getLogger(__name__)


class HBaseThrift2Client:
    """Lightweight HBase Thrift2 client."""

    def __init__(self, host: str, port: int = 9090, timeout: int = 30000, 
                 ssl_options: dict[str, Any] | None = None,
                 auth_method: str | None = None,
                 kerberos_service_name: str = 'hbase',
                 kerberos_principal: str | None = None,
                 kerberos_keytab: str | None = None,
                 retry_max_attempts: int = 3,
                 retry_delay: float = 1.0,
                 retry_backoff_factor: float = 2.0):
        """Initialize Thrift2 client.
        
        Args:
            host: HBase Thrift2 server host
            port: HBase Thrift2 server port (default 9090 for Arenadata/Apache HBase)
            timeout: Connection timeout in milliseconds
            ssl_options: SSL options dict with keys: ca_certs, cert_file, key_file, validate (optional)
            auth_method: Authentication method ('GSSAPI' for Kerberos, None for no auth)
            kerberos_service_name: Kerberos service name (default 'hbase')
            kerberos_principal: Kerberos principal username (e.g. 'airflow@REALM')
            kerberos_keytab: Path to keytab file (e.g. '/etc/security/keytabs/airflow.keytab')
            retry_max_attempts: Maximum number of connection attempts
            retry_delay: Initial delay between retry attempts in seconds
            retry_backoff_factor: Multiplier for delay after each failed attempt
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.ssl_options = ssl_options
        self.auth_method = auth_method
        self.kerberos_service_name = kerberos_service_name
        self.kerberos_principal = kerberos_principal
        self.kerberos_keytab = kerberos_keytab
        self.retry_max_attempts = retry_max_attempts
        self.retry_delay = retry_delay
        self.retry_backoff_factor = retry_backoff_factor
        self._client = None
        
        if auth_method and not SASL_AVAILABLE:
            raise ImportError(
                "thrift_sasl library is required for Kerberos authentication. "
                "Install it with: pip install thrift_sasl"
            )
        
        logger.debug("HBaseThrift2Client initialized with ssl_options: %s, auth_method: %s", 
                    ssl_options, auth_method)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        """Open connection to Thrift2 server with retry logic."""
        last_exception = None
        
        for attempt in range(self.retry_max_attempts):
            try:
                # Create socket (SSL or regular)
                if self.ssl_options:
                    import ssl as ssl_module
                    # Map our options to TSSLSocket parameters
                    ssl_params = {
                        'host': self.host,
                        'port': self.port,
                    }
                    if 'ca_certs' in self.ssl_options:
                        ssl_params['ca_certs'] = self.ssl_options['ca_certs']
                    if 'cert_file' in self.ssl_options:
                        ssl_params['certfile'] = self.ssl_options['cert_file']
                    if 'key_file' in self.ssl_options:
                        ssl_params['keyfile'] = self.ssl_options['key_file']
                    if 'validate' in self.ssl_options:
                        # Map validate to cert_reqs
                        ssl_params['cert_reqs'] = ssl_module.CERT_REQUIRED if self.ssl_options['validate'] else ssl_module.CERT_NONE
                    
                    socket = TSSLSocket.TSSLSocket(**ssl_params)
                else:
                    socket = TSocket.TSocket(self.host, self.port)
                
                socket.setTimeout(self.timeout)
                
                # Create transport with optional Kerberos authentication
                if self.auth_method == 'GSSAPI':
                    # Kerberos authentication
                    import subprocess
                    import os
                    
                    # Set KRB5CCNAME BEFORE any GSSAPI operations
                    if 'KRB5CCNAME' not in os.environ:
                        result = subprocess.run(['klist'], capture_output=True, text=True)
                        if result.returncode == 0:
                            for line in result.stdout.split('\n'):
                                if line.startswith('Ticket cache:'):
                                    cache = line.split(':', 1)[1].strip()
                                    os.environ['KRB5CCNAME'] = cache
                                    logger.info("Set KRB5CCNAME=%s", cache)
                                    break
                    
                    # Check if ticket exists and is valid
                    try:
                        subprocess.run(['klist', '-s'], check=True, capture_output=True)
                        logger.info("Using existing Kerberos ticket")
                    except subprocess.CalledProcessError:
                        # No valid ticket - try to get one with keytab
                        if self.kerberos_keytab:
                            # Use principal if provided, otherwise derive from keytab
                            principal = self.kerberos_principal
                            if not principal:
                                # Try to get principal from keytab
                                result = subprocess.run(['klist', '-kt', self.kerberos_keytab], capture_output=True, text=True)
                                if result.returncode == 0:
                                    # Parse first principal from keytab
                                    for line in result.stdout.split('\n'):
                                        if '@' in line and 'KVNO' not in line:
                                            principal = line.split()[-1]
                                            break
                            
                            if principal:
                                kinit_cmd = ['kinit', '-kt', self.kerberos_keytab, principal]
                                logger.info("Getting Kerberos ticket using keytab: %s for principal: %s", self.kerberos_keytab, principal)
                                result = subprocess.run(kinit_cmd, capture_output=True, text=True)
                                if result.returncode != 0:
                                    logger.error("kinit failed (exit code %d): %s", result.returncode, result.stderr)
                                    raise RuntimeError(f"Failed to obtain Kerberos ticket: {result.stderr}")
                                logger.info("Successfully obtained Kerberos ticket")
                            else:
                                raise RuntimeError("Could not determine principal from keytab")
                        else:
                            logger.error("No Kerberos ticket found and no keytab specified")
                            raise RuntimeError("No Kerberos credentials available. Please specify kerberos_keytab in connection extra.")
                    
                    def sasl_factory():
                        import sasl
                        
                        # Extract username from principal
                        username = None
                        if self.kerberos_principal:
                            username = self.kerberos_principal.split('@')[0].split('/')[0]
                        
                        logger.info("[SASL DEBUG] Creating SASL client")
                        logger.info("[SASL DEBUG] host=%s, service=%s, username=%s", self.host, self.kerberos_service_name, username)
                        logger.info("[SASL DEBUG] kerberos_principal=%s", self.kerberos_principal)
                        
                        sasl_client = sasl.Client()
                        sasl_client.setAttr('host', self.host)
                        sasl_client.setAttr('service', self.kerberos_service_name)
                        if username:
                            sasl_client.setAttr('username', username)
                        sasl_client.init()
                        
                        logger.info("[SASL DEBUG] SASL client initialized")
                        return sasl_client
                    
                    self._transport = TSaslClientTransport(
                        sasl_factory,
                        'GSSAPI',
                        socket
                    )
                    
                    protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
                    self._client = THBaseService.Client(protocol)
                    self._transport.open()
                    
                    # Test connection
                    self._client.getTableNamesByPattern(regex=None, includeSysTables=False)
                    
                    logger.info("Successfully connected to HBase Thrift2 at %s:%s (SSL: %s, Auth: %s)", 
                               self.host, self.port, bool(self.ssl_options), self.auth_method)
                    return
                else:
                    # No authentication - try TBufferedTransport first, then TFramedTransport
                    for transport_type in ['buffered', 'framed']:
                        try:
                            if transport_type == 'buffered':
                                self._transport = TTransport.TBufferedTransport(socket)
                            else:
                                self._transport = TTransport.TFramedTransport(socket)
                            
                            protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
                            self._client = THBaseService.Client(protocol)
                            self._transport.open()
                            
                            # Test connection with a simple operation
                            self._client.getTableNamesByPattern(regex=None, includeSysTables=False)
                            
                            logger.info("Successfully connected to HBase Thrift2 at %s:%s (SSL: %s, Transport: %s)", 
                                       self.host, self.port, bool(self.ssl_options), transport_type)
                            return
                        except Exception as transport_error:
                            logger.debug("Transport %s failed: %s", transport_type, transport_error)
                            if hasattr(self, '_transport') and self._transport:
                                try:
                                    self._transport.close()
                                except:
                                    pass
                            if transport_type == 'framed':
                                # Both transports failed, raise the last error
                                raise transport_error
                
            except (ConnectionError, TimeoutError, OSError, Exception) as e:
                last_exception = e
                if attempt == self.retry_max_attempts - 1:  # Last attempt
                    logger.error("All %d connection attempts failed. Last error: %s", self.retry_max_attempts, e)
                    raise e

                wait_time = self.retry_delay * (self.retry_backoff_factor ** attempt)
                logger.warning(
                    "Connection attempt %d/%d failed: %s. Retrying in %.1fs...",
                    attempt + 1, self.retry_max_attempts, e, wait_time
                )
                time.sleep(wait_time)
        
        # This should never be reached, but just in case
        if last_exception:
            raise last_exception

    def close(self):
        """Close connection."""
        if hasattr(self, '_transport') and self._transport:
            self._transport.close()
            self._transport = None
        self._client = None

    def list_tables(self) -> list[str]:
        """List all tables."""
        table_names = self._client.getTableNamesByPattern(regex=None, includeSysTables=False)
        return [tn.qualifier.decode() for tn in table_names]

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        table_name_obj = ttypes.TTableName(
            ns=b"default",
            qualifier=table_name.encode()
        )
        return self._client.tableExists(table_name_obj)

    def create_table(self, table_name: str, families: dict[str, dict]) -> None:
        """Create table.
        
        Args:
            table_name: Name of the table
            families: Dictionary of column families
        """
        table_name_obj = ttypes.TTableName(
            ns=b"default",
            qualifier=table_name.encode()
        )

        column_families = []
        for family_name in families.keys():
            col_desc = ttypes.TColumnFamilyDescriptor(
                name=family_name.encode()
            )
            column_families.append(col_desc)

        table_desc = ttypes.TTableDescriptor(
            tableName=table_name_obj,
            columns=column_families
        )

        self._client.createTable(table_desc, None)

    def delete_table(self, table_name: str) -> None:
        """Delete table.
        
        Args:
            table_name: Name of the table
        """
        table_name_obj = ttypes.TTableName(
            ns=b"default",
            qualifier=table_name.encode()
        )
        
        # Disable table first
        self._client.disableTable(table_name_obj)
        # Delete table
        self._client.deleteTable(table_name_obj)

    def put(self, table_name: str, row_key: str, data: dict[str, str]) -> None:
        """Put data into table.
        
        Args:
            table_name: Name of the table
            row_key: Row key
            data: Dictionary of column:value pairs (format: "family:qualifier": "value")
        """
        column_values = []
        for column, value in data.items():
            family, qualifier = column.split(":", 1)
            col_val = ttypes.TColumnValue(
                family=family.encode(),
                qualifier=qualifier.encode(),
                value=value.encode() if isinstance(value, str) else value
            )
            column_values.append(col_val)

        tput = ttypes.TPut(
            row=row_key.encode(),
            columnValues=column_values
        )

        # Use table name as bytes, not TTableName object
        self._client.put(table_name.encode(), tput)

    def put_multiple(self, table_name: str, puts: list[tuple[str, dict[str, str]]]) -> None:
        """Put multiple rows in batch.
        
        Args:
            table_name: Name of the table
            puts: List of (row_key, data) tuples
        """
        tputs = []
        for row_key, data in puts:
            column_values = []
            for column, value in data.items():
                family, qualifier = column.split(":", 1)
                col_val = ttypes.TColumnValue(
                    family=family.encode(),
                    qualifier=qualifier.encode(),
                    value=value.encode() if isinstance(value, str) else value
                )
                column_values.append(col_val)

            tput = ttypes.TPut(
                row=row_key.encode(),
                columnValues=column_values
            )
            tputs.append(tput)

        self._client.putMultiple(table_name.encode(), tputs)

    def get(self, table_name: str, row_key: str, columns: list[str] | None = None) -> dict[str, Any]:
        """Get row from table.
        
        Args:
            table_name: Name of the table
            row_key: Row key
            columns: List of columns to retrieve (format: "family:qualifier")
            
        Returns:
            Dictionary with row data
        """
        tget = ttypes.TGet(row=row_key.encode())

        if columns:
            tget.columns = []
            for column in columns:
                family, qualifier = column.split(":", 1)
                tcol = ttypes.TColumn(
                    family=family.encode(),
                    qualifier=qualifier.encode()
                )
                tget.columns.append(tcol)

        result = self._client.get(table_name.encode(), tget)
        return self._parse_result(result)

    def get_multiple(self, table_name: str, row_keys: list[str], columns: list[str] | None = None) -> list[dict[str, Any]]:
        """Get multiple rows in batch.
        
        Args:
            table_name: Name of the table
            row_keys: List of row keys
            columns: List of columns to retrieve
            
        Returns:
            List of row data dictionaries
        """
        tgets = []
        for row_key in row_keys:
            tget = ttypes.TGet(row=row_key.encode())
            
            if columns:
                tget.columns = []
                for column in columns:
                    family, qualifier = column.split(":", 1)
                    tcol = ttypes.TColumn(
                        family=family.encode(),
                        qualifier=qualifier.encode()
                    )
                    tget.columns.append(tcol)
            
            tgets.append(tget)

        results = self._client.getMultiple(table_name.encode(), tgets)
        return [self._parse_result(r) for r in results]

    def delete(self, table_name: str, row_key: str, columns: list[str] | None = None) -> None:
        """Delete row or columns.
        
        Args:
            table_name: Name of the table
            row_key: Row key
            columns: List of columns to delete (if None, deletes entire row)
        """
        tdelete = ttypes.TDelete(row=row_key.encode())

        if columns:
            tdelete.columns = []
            for column in columns:
                family, qualifier = column.split(":", 1)
                tcol = ttypes.TColumn(
                    family=family.encode(),
                    qualifier=qualifier.encode()
                )
                tdelete.columns.append(tcol)

        self._client.deleteSingle(table_name.encode(), tdelete)

    def delete_multiple(self, table_name: str, deletes: list[tuple[str, list[str] | None]]) -> None:
        """Delete multiple rows in batch.
        
        Args:
            table_name: Name of the table
            deletes: List of (row_key, columns) tuples
        """
        tdeletes = []
        for row_key, columns in deletes:
            tdelete = ttypes.TDelete(row=row_key.encode())
            
            if columns:
                tdelete.columns = []
                for column in columns:
                    family, qualifier = column.split(":", 1)
                    tcol = ttypes.TColumn(
                        family=family.encode(),
                        qualifier=qualifier.encode()
                    )
                    tdelete.columns.append(tcol)
            
            tdeletes.append(tdelete)

        self._client.deleteMultiple(table_name.encode(), tdeletes)

    def scan(
        self,
        table_name: str,
        start_row: str | None = None,
        stop_row: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Scan table.
        
        Args:
            table_name: Name of the table
            start_row: Start row key
            stop_row: Stop row key
            columns: List of columns to retrieve
            limit: Maximum number of rows
            
        Returns:
            List of row data dictionaries
        """
        tscan = ttypes.TScan()

        if start_row:
            tscan.startRow = start_row.encode()
        if stop_row:
            tscan.stopRow = stop_row.encode()
        if limit:
            tscan.limit = limit

        if columns:
            tscan.columns = []
            for column in columns:
                family, qualifier = column.split(":", 1)
                tcol = ttypes.TColumn(
                    family=family.encode(),
                    qualifier=qualifier.encode()
                )
                tscan.columns.append(tcol)

        scanner_id = self._client.openScanner(table_name.encode(), tscan)

        try:
            results = []
            while True:
                rows = self._client.getScannerRows(scanner_id, limit or 100)
                if not rows:
                    break

                for row in rows:
                    results.append(self._parse_result(row))

                if limit and len(results) >= limit:
                    break

            return results
        finally:
            self._client.closeScanner(scanner_id)

    def _parse_result(self, result) -> dict[str, Any]:
        """Parse Thrift2 result to dictionary.
        
        Args:
            result: TResult object from Thrift2
            
        Returns:
            Dictionary with parsed data
        """
        if not result or not result.columnValues:
            return {}

        parsed = {
            'row': result.row.decode() if result.row else None,
            'columns': {}
        }

        for col_val in result.columnValues:
            family = col_val.family.decode()
            qualifier = col_val.qualifier.decode()
            value = col_val.value
            timestamp = col_val.timestamp if hasattr(col_val, 'timestamp') else None

            column_name = f"{family}:{qualifier}"
            parsed['columns'][column_name] = {
                'value': value,
                'timestamp': timestamp
            }

        return parsed
