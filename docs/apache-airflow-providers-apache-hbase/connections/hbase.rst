 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



Apache HBase Connection
=======================

The Apache HBase connection type enables connection to `Apache HBase <https://hbase.apache.org/>`__.

Default Connection IDs
----------------------

HBase hook and HBase operators use ``hbase_default`` by default.

Supported Connection Types
--------------------------

The HBase provider supports multiple connection types for different use cases:

* **hbase** - Direct Thrift connection (recommended for most operations)
* **generic** - Generic connection for Thrift servers

Connection Strategies
--------------------

The provider supports two connection strategies for optimal performance:

* **ThriftStrategy** - Single connection for simple operations
* **PooledThriftStrategy** - Connection pooling for high-throughput operations

Connection pooling is automatically enabled when ``pool_size`` is specified in the connection Extra field.
Pooled connections provide better performance for batch operations and concurrent access.

Connection Pool Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable connection pooling, add the following to your connection's Extra field:

.. code-block:: json

    {
      "pool_size": 10,
      "pool_timeout": 30
    }

* ``pool_size`` - Maximum number of connections in the pool (default: 1, no pooling)
* ``pool_timeout`` - Timeout in seconds for getting connection from pool (default: 30)

Connection Examples
-------------------

The following connection examples are based on actual deployment configurations:

Basic Thrift2 Connection (No Authentication)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``hbase``
:Host: ``hbase-server.example.com``
:Port: ``9090``
:Extra:

.. code-block:: json

    {}

Pooled Thrift2 Connection
^^^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``hbase``
:Host: ``hbase-server.example.com``
:Port: ``9090``
:Extra:

.. code-block:: json

    {
      "java_home": "/usr/lib/jvm/java-arenadata-openjdk-8",
      "hbase_home": "/usr/lib/hbase",
      "connection_pool": {
        "enabled": true,
        "size": 10
      }
    }

.. note::
    Connection pooling significantly improves performance for batch operations
    and concurrent access patterns. Use pooled connections for production workloads.

Kerberos Authentication
^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``hbase``
:Host: ``hbase-server.example.com``
:Port: ``9090``
:Extra:

.. code-block:: json

    {
      "auth_method": "GSSAPI",
      "kerberos_service_name": "hbase",
      "kerberos_keytab": "/etc/security/keytabs/airflow.service.keytab"
    }

Kerberos with Connection Pool
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``hbase``
:Host: ``hbase-server.example.com``
:Port: ``9090``
:Extra:

.. code-block:: json

    {
      "auth_method": "GSSAPI",
      "kerberos_service_name": "hbase",
      "kerberos_principal": "airflow@KRB5-TEST",
      "kerberos_keytab": "/etc/security/keytabs/airflow.service.keytab",
      "java_home": "/usr/lib/jvm/java-arenadata-openjdk-8",
      "hbase_home": "/usr/lib/hbase",
      "connection_pool": {
        "enabled": true,
        "size": 10
      }
    }

Configuring the Connection
--------------------------

Host (required)
    The HBase Thrift2 server hostname.

Port (required)
    The HBase Thrift2 server port (default: 9090).

Extra (optional)
    Specify the extra parameters (as JSON dictionary) that can be used in HBase connection.

    The following extras are supported:

    **Connection parameters:**

    * ``timeout`` - Connection timeout in milliseconds (default: 30000).
    * ``namespace`` - HBase namespace (default: "default").
    * ``use_http`` - Use HTTP transport instead of binary socket (default: false for non-SSL, true for SSL connections).
    * ``retry_max_attempts`` - Maximum number of connection retry attempts (default: 3).
    * ``retry_delay`` - Initial delay between retry attempts in seconds (default: 1.0).
    * ``retry_backoff_factor`` - Multiplier for delay after each failed attempt (default: 2.0).

    **Connection pooling parameters:**

    * ``connection_pool`` - Connection pool configuration (optional):

      * ``enabled`` - Enable connection pooling (default: false).
      * ``size`` - Pool size (default: 10).
      * ``timeout`` - Pool connection timeout in seconds (default: 30).

    **Authentication parameters:**

    * ``auth_method`` - Authentication method. Set to ``GSSAPI`` for Kerberos authentication.
    * ``kerberos_service_name`` - Kerberos service name (default: "hbase").
    * ``kerberos_principal`` - Kerberos principal username (e.g., "airflow@REALM").
    * ``kerberos_keytab`` - Path to keytab file (e.g., "/etc/security/keytabs/airflow.service.keytab").

    **CLI operation parameters:**

    * ``java_home`` - Java home directory for CLI operations (default: "/usr/lib/jvm/java-arenadata-openjdk-8").
    * ``hbase_home`` - HBase installation directory for CLI operations (default: "/usr/lib/hbase").

Examples for the **Extra** field
--------------------------------

1. Basic connection (no authentication)

.. code-block:: json

    {}

2. Connection with timeout and namespace

.. code-block:: json

    {
      "timeout": 30000,
      "namespace": "production"
    }

3. Kerberos authentication with keytab file

.. code-block:: json

    {
      "auth_method": "GSSAPI",
      "kerberos_service_name": "hbase",
      "kerberos_principal": "airflow@REALM",
      "kerberos_keytab": "/etc/security/keytabs/airflow.service.keytab"
    }

4. Connection with pooling

.. code-block:: json

    {
      "connection_pool": {
        "enabled": true,
        "size": 10,
        "timeout": 30
      }
    }

5. Connection with retry configuration

.. code-block:: json

    {
      "retry_max_attempts": 5,
      "retry_delay": 2.0,
      "retry_backoff_factor": 3.0
    }

6. Full configuration with all options

.. code-block:: json

    {
      "timeout": 30000,
      "namespace": "production",
      "auth_method": "GSSAPI",
      "kerberos_service_name": "hbase",
      "kerberos_principal": "airflow@REALM",
      "kerberos_keytab": "/etc/security/keytabs/airflow.service.keytab",
      "java_home": "/usr/lib/jvm/java-arenadata-openjdk-8",
      "hbase_home": "/usr/lib/hbase",
      "connection_pool": {
        "enabled": true,
        "size": 10,
        "timeout": 30
      },
      "retry_max_attempts": 3,
      "retry_delay": 1.0,
      "retry_backoff_factor": 2.0
    }
