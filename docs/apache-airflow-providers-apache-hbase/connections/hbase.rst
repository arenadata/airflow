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

* **Thrift2Strategy** - Single connection for simple operations
* **PooledThrift2Strategy** - Connection pooling for high-throughput operations

Connection pooling is enabled when ``connection_pool.enabled`` is set to ``true`` in the connection Extra field.
Pooled connections provide better performance for batch operations and concurrent access.

Connection Pool Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable connection pooling, add the following to your connection's Extra field:

.. code-block:: json

    {
      "connection_pool": {
        "enabled": true,
        "size": 10,
        "timeout": 30
      }
    }

* ``connection_pool.enabled`` - Enable connection pooling (default: false).
* ``connection_pool.size`` - Maximum number of connections in the pool (default: 10).
* ``connection_pool.timeout`` - Timeout in seconds for borrowing a connection from the pool (default: 30).

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

    {
      "use_http": false
    }

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
      "use_http": false,
      "connection_pool": {
        "enabled": true,
        "size": 10
      }
    }

.. note::
    Connection pooling significantly improves performance for batch operations
    and concurrent access patterns. Use pooled connections for production workloads.

SSL/TLS Connection
^^^^^^^^^^^^^^^^^^

:Connection Type: ``hbase``
:Host: ``hbase-server.example.com``
:Port: ``9090``
:Extra:

.. code-block:: json

    {
      "ca_certs": "/etc/ssl/hbase_certs.pem",
      "validate": true,
      "use_http": true
    }

.. note::
    When using SSL with HBase Thrift2, ``use_http`` should typically be set to ``true``
    if the HBase server is configured with ``hbase.regionserver.thrift.http=true``.

SSL/TLS with Connection Pool
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``hbase``
:Host: ``hbase-server.example.com``
:Port: ``9090``
:Extra:

.. code-block:: json

    {
      "ca_certs": "/etc/ssl/hbase_certs.pem",
      "validate": true,
      "use_http": true,
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
    * ``use_http`` - Use HTTP transport instead of binary socket (default: false).
    * ``retry_max_attempts`` - Maximum number of connection retry attempts (default: 3).
    * ``retry_delay`` - Initial delay between retry attempts in seconds (default: 1.0).
    * ``retry_backoff_factor`` - Multiplier for delay after each failed attempt (default: 2.0).

    **Connection pooling parameters:**

    * ``connection_pool`` - Connection pool configuration (optional):

      * ``enabled`` - Enable connection pooling (default: false).
      * ``size`` - Pool size (default: 10).
      * ``timeout`` - Timeout in seconds for borrowing a connection from the pool (default: 30).

    **SSL/TLS parameters (flat format):**

    * ``ca_certs`` - Path to CA certificate bundle for server verification.
    * ``validate`` - Whether to validate the server certificate (default: true).

    **CLI operation parameters:**

    * ``java_home`` - Java home directory for CLI operations (default: "/usr/lib/jvm/java-arenadata-openjdk-8").
    * ``hbase_home`` - HBase installation directory for CLI operations (default: "/usr/lib/hbase").

Examples for the **Extra** field
--------------------------------

1. Basic connection (no authentication)

.. code-block:: json

    {
      "use_http": false
    }

2. Connection with timeout and namespace

.. code-block:: json

    {
      "timeout": 30000,
      "namespace": "production"
    }

3. Connection with pooling

.. code-block:: json

    {
      "connection_pool": {
        "enabled": true,
        "size": 10,
        "timeout": 30
      }
    }

4. SSL/TLS connection

.. code-block:: json

    {
      "ca_certs": "/etc/ssl/hbase_certs.pem",
      "validate": true,
      "use_http": true
    }

5. Connection with retry configuration

.. code-block:: json

    {
      "retry_max_attempts": 5,
      "retry_delay": 2.0,
      "retry_backoff_factor": 3.0
    }
