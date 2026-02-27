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
"""HBase connection configuration."""

from __future__ import annotations

from typing import Any


class HBaseConnectionConfig:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """Configuration for HBase Thrift2 connection."""

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        host: str,
        port: int = 9090,
        timeout: int = 30000,
        ssl_options: dict[str, Any] | None = None,
        auth_method: str | None = None,
        kerberos_service_name: str = 'hbase',
        kerberos_principal: str | None = None,
        kerberos_keytab: str | None = None,
        namespace: str = 'default',
        retry_max_attempts: int = 3,
        retry_delay: float = 1.0,
        retry_backoff_factor: float = 2.0
    ):
        """Initialize connection configuration."""
        self.host = host
        self.port = port
        self.timeout = timeout
        self.ssl_options = ssl_options
        self.auth_method = auth_method
        self.kerberos_service_name = kerberos_service_name
        self.kerberos_principal = kerberos_principal
        self.kerberos_keytab = kerberos_keytab
        self.namespace = namespace
        self.retry_max_attempts = retry_max_attempts
        self.retry_delay = retry_delay
        self.retry_backoff_factor = retry_backoff_factor


def create_connection_config(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    host: str,
    port: int = 9090,
    timeout: int = 30000,
    ssl_options: dict[str, Any] | None = None,
    auth_method: str | None = None,
    kerberos_service_name: str = 'hbase',
    kerberos_principal: str | None = None,
    kerberos_keytab: str | None = None,
    namespace: str = 'default',
    retry_max_attempts: int = 3,
    retry_delay: float = 1.0,
    retry_backoff_factor: float = 2.0
) -> HBaseConnectionConfig:
    """Create HBase connection configuration.
    
    Helper function to avoid code duplication.
    """
    return HBaseConnectionConfig(
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
