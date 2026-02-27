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
import os
import ssl as ssl_module
import subprocess
import time
from typing import Any

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport, TSSLSocket
from thrift.transport.TTransport import TTransportException

try:
    import sasl
    from thrift_sasl import TSaslClientTransport
    SASL_AVAILABLE = True
except ImportError:
    SASL_AVAILABLE = False
    sasl = None  # type: ignore
    TSaslClientTransport = None  # type: ignore

from airflow.providers.arenadata.hbase.hbase_thrift2_generated import THBaseService, ttypes
from airflow.providers.arenadata.hbase.connection_config import HBaseConnectionConfig

logger = logging.getLogger(__name__)


class HBaseThrift2Client:
    """Lightweight HBase Thrift2 client."""

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, host: str, port: int = 9090, timeout: int = 30000,
                 ssl_options: dict[str, Any] | None = None,
                 auth_method: str | None = None,
                 kerberos_service_name: str = 'hbase',
                 kerberos_principal: str | None = None,
                 kerberos_keytab: str | None = None,
                 namespace: str = 'default',
                 retry_max_attempts: int = 3,
                 retry_delay: float = 1.0,
                 retry_backoff_factor: float = 2.0):
        """Initialize Thrift2 client.

        Args:
            host: HBase Thrift2 server host
            port: HBase Thrift2 server port (default 9090 for Arenadata/Apache HBase)
            timeout: Connection timeout in milliseconds
            ssl_options: SSL options dict with keys:
                ca_certs, cert_file, key_file, validate (optional)
            auth_method: Authentication method ('GSSAPI' for Kerberos, None for no auth)
            kerberos_service_name: Kerberos service name (default 'hbase')
            kerberos_principal: Kerberos principal username (e.g. 'airflow@REALM')
            kerberos_keytab: Path to keytab file (e.g. '/etc/security/keytabs/airflow.keytab')
            namespace: HBase namespace (default 'default')
            retry_max_attempts: Maximum number of connection attempts
            retry_delay: Initial delay between retry attempts in seconds
            retry_backoff_factor: Multiplier for delay after each failed attempt
        """
        self.config = HBaseConnectionConfig(
            host=host,
            port=port,
            timeout=timeout,
            ssl_options=ssl_options,
            auth_method=auth_method,
            kerberos_service_name=kerberos_service_name,
            kerberos_principal=kerberos_principal,
            kerberos_keytab=kerberos_keytab,
            namespace=namespace,
            retry_max_attempts=retry_max_attempts,
            retry_delay=retry_delay,
            retry_backoff_factor=retry_backoff_factor
        )
        self._client = None
        self._transport = None

        if auth_method and not SASL_AVAILABLE:
            raise ImportError(
                "thrift_sasl and sasl libraries are required for Kerberos authentication. "
                "Install them with: pip install thrift_sasl sasl"
            )

        logger.debug("HBaseThrift2Client initialized with ssl_options: %s, auth_method: %s",
                    ssl_options, auth_method)

    @property
    def host(self) -> str:
        return self.config.host

    @property
    def port(self) -> int:
        return self.config.port

    @property
    def timeout(self) -> int:
        return self.config.timeout

    @property
    def ssl_options(self) -> dict[str, Any] | None:
        return self.config.ssl_options

    @property
    def auth_method(self) -> str | None:
        return self.config.auth_method

    @property
    def kerberos_service_name(self) -> str:
        return self.config.kerberos_service_name

    @property
    def kerberos_principal(self) -> str | None:
        return self.config.kerberos_principal

    @property
    def kerberos_keytab(self) -> str | None:
        return self.config.kerberos_keytab

    @property
    def namespace(self) -> str:
        return self.config.namespace

    @property
    def retry_max_attempts(self) -> int:
        return self.config.retry_max_attempts

    @property
    def retry_delay(self) -> float:
        return self.config.retry_delay

    @property
    def retry_backoff_factor(self) -> float:
        return self.config.retry_backoff_factor

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _create_socket(self) -> TSocket.TSocket | TSSLSocket.TSSLSocket:
        """Create socket (SSL or regular).

        Returns:
            Configured socket instance
        """
        if self.config.ssl_options:
            # Map our options to TSSLSocket parameters
            ssl_params = {
                'host': self.config.host,
                'port': self.config.port,
            }
            if 'ca_certs' in self.config.ssl_options:
                ssl_params['ca_certs'] = self.config.ssl_options['ca_certs']
            if 'cert_file' in self.config.ssl_options:
                ssl_params['certfile'] = self.config.ssl_options['cert_file']
            if 'key_file' in self.config.ssl_options:
                ssl_params['keyfile'] = self.config.ssl_options['key_file']
            if 'validate' in self.config.ssl_options:
                ssl_params['cert_reqs'] = (
                    ssl_module.CERT_REQUIRED if self.config.ssl_options['validate']
                    else ssl_module.CERT_NONE
                )

            sock = TSSLSocket.TSSLSocket(**ssl_params)
        else:
            sock = TSocket.TSocket(self.config.host, self.config.port)

        sock.setTimeout(self.config.timeout)
        return sock

    def _setup_kerberos(self) -> None:  # pylint: disable=too-many-branches
        """Setup Kerberos authentication.

        Raises:
            RuntimeError: If Kerberos setup fails
        """
        # Set KRB5CCNAME BEFORE any GSSAPI operations
        if 'KRB5CCNAME' not in os.environ:
            result = subprocess.run(['klist'], capture_output=True, text=True, check=False)
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
            if self.config.kerberos_keytab:
                principal = self.config.kerberos_principal
                if not principal:
                    # Try to get principal from keytab
                    result = subprocess.run(
                        ['klist', '-kt', self.config.kerberos_keytab],
                        capture_output=True, text=True, check=False
                    )
                    if result.returncode == 0:
                        for line in result.stdout.split('\n'):
                            if '@' in line and 'KVNO' not in line:
                                principal = line.split()[-1]
                                break

                if principal:
                    kinit_cmd = ['kinit', '-kt', self.config.kerberos_keytab, principal]
                    logger.info(
                        "Getting Kerberos ticket using keytab: %s for principal: %s",
                        self.config.kerberos_keytab, principal
                    )
                    result = subprocess.run(kinit_cmd, capture_output=True, text=True, check=False)
                    if result.returncode != 0:
                        logger.error(
                            "kinit failed (exit code %d): %s",
                            result.returncode, result.stderr
                        )
                        raise RuntimeError(f"Failed to obtain Kerberos ticket: {result.stderr}")  # pylint: disable=raise-missing-from
                    logger.info("Successfully obtained Kerberos ticket")
                else:
                    raise RuntimeError("Could not determine principal from keytab")  # pylint: disable=raise-missing-from
            else:
                logger.error("No Kerberos ticket found and no keytab specified")
                raise RuntimeError(  # pylint: disable=raise-missing-from
                    "No Kerberos credentials available. "
                    "Please specify kerberos_keytab in connection extra."
                )

    def _create_sasl_transport(self, sock: TSocket.TSocket | TSSLSocket.TSSLSocket):
        """Create SASL transport for Kerberos authentication.

        Args:
            sock: Base socket

        Returns:
            SASL transport instance
        """
        def sasl_factory():
            username = None
            if self.config.kerberos_principal:
                username = self.config.kerberos_principal.split('@')[0].split('/')[0]

            logger.info("[SASL DEBUG] Creating SASL client")
            logger.info(
                "[SASL DEBUG] host=%s, service=%s, username=%s",
                self.config.host, self.config.kerberos_service_name, username
            )
            logger.info("[SASL DEBUG] kerberos_principal=%s", self.config.kerberos_principal)

            sasl_client = sasl.Client()
            sasl_client.setAttr('host', self.config.host)
            sasl_client.setAttr('service', self.config.kerberos_service_name)
            if username:
                sasl_client.setAttr('username', username)
            sasl_client.init()

            logger.info("[SASL DEBUG] SASL client initialized")
            return sasl_client

        return TSaslClientTransport(sasl_factory, 'GSSAPI', sock)

    def _setup_kerberos_transport(self, sock: TSocket.TSocket | TSSLSocket.TSSLSocket) -> None:
        """Setup Kerberos transport and client.

        Args:
            sock: Base socket

        Raises:
            RuntimeError: If Kerberos setup fails
        """
        self._setup_kerberos()
        self._transport = self._create_sasl_transport(sock)
        protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = THBaseService.Client(protocol)
        self._transport.open()

    def _setup_simple_transport(self, sock: TSocket.TSocket | TSSLSocket.TSSLSocket) -> None:
        """Setup simple transport without authentication.

        Args:
            sock: Base socket

        Raises:
            Exception: If both transport types fail
        """
        for transport_type in ['buffered', 'framed']:
            try:
                if transport_type == 'buffered':
                    self._transport = TTransport.TBufferedTransport(sock)
                else:
                    self._transport = TTransport.TFramedTransport(sock)

                protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
                self._client = THBaseService.Client(protocol)
                self._transport.open()

                # Test connection
                self._client.getTableNamesByPattern(regex=None, includeSysTables=False)

                logger.info(
                    "Successfully connected to HBase Thrift2 at %s:%s (SSL: %s, Transport: %s)",
                    self.config.host, self.config.port, bool(self.config.ssl_options), transport_type
                )
                return
            except (TTransportException, OSError) as transport_error:
                logger.debug("Transport %s failed: %s", transport_type, transport_error)
                if hasattr(self, '_transport') and self._transport:
                    try:
                        self._transport.close()
                    except (TTransportException, OSError):
                        logger.debug("Failed to close transport during cleanup")
                if transport_type == 'framed':
                    raise transport_error

    def _test_connection(self) -> None:
        """Test connection with a simple operation.

        Raises:
            Exception: If connection test fails
        """
        self._client.getTableNamesByPattern(regex=None, includeSysTables=False)

    def open(self) -> None:
        """Open connection to Thrift2 server with retry logic.

        Raises:
            ConnectionError: If connection fails after all retries
            TimeoutError: If connection times out
            RuntimeError: If Kerberos authentication fails
            OSError: If socket operation fails
        """
        last_exception = None

        for attempt in range(self.config.retry_max_attempts):
            try:
                sock = self._create_socket()

                if self.config.auth_method == 'GSSAPI':
                    self._setup_kerberos_transport(sock)
                    self._test_connection()
                    logger.info(
                        "Successfully connected to HBase Thrift2 at %s:%s (SSL: %s, Auth: %s)",
                        self.config.host, self.config.port, bool(self.config.ssl_options), self.config.auth_method
                    )
                else:
                    self._setup_simple_transport(sock)

                return

            except (ConnectionError, TimeoutError, OSError, TTransportException) as e:
                last_exception = e
                if attempt == self.config.retry_max_attempts - 1:
                    logger.error(
                        "All %d connection attempts failed. Last error: %s",
                        self.config.retry_max_attempts, e
                    )
                    raise e

                wait_time = self.config.retry_delay * (self.config.retry_backoff_factor ** attempt)
                logger.warning(
                    "Connection attempt %d/%d failed: %s. Retrying in %.1fs...",
                    attempt + 1, self.config.retry_max_attempts, e, wait_time
                )
                time.sleep(wait_time)

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
            ns=self.config.namespace.encode(),
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
            ns=self.config.namespace.encode(),
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
            ns=self.config.namespace.encode(),
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

    def get(
        self, table_name: str, row_key: str, columns: list[str] | None = None
    ) -> dict[str, Any]:
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

    def get_multiple(
        self, table_name: str, row_keys: list[str], columns: list[str] | None = None
    ) -> list[dict[str, Any]]:
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

    def scan(  # pylint: disable=too-many-arguments,too-many-positional-arguments
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
