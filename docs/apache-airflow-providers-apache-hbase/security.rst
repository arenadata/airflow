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

Security
--------

The Apache HBase provider uses the Thrift2 protocol to connect to HBase with SSL/TLS support
for encrypted communication.

SSL/TLS Encryption
~~~~~~~~~~~~~~~~~~

The provider supports SSL/TLS for secure communication with HBase Thrift2 servers.

**SSL/TLS Features:**

* Server certificate verification via CA certificate bundle
* Configurable certificate validation
* Compatible with both binary socket and HTTP transport modes

**Configuration Example:**

.. code-block:: python

    from airflow.models import Connection

    ssl_connection = Connection(
        conn_id="hbase_ssl",
        conn_type="hbase",
        host="hbase-server.example.com",
        port=9090,
        extra={
            "ca_certs": "/etc/ssl/hbase_certs.pem",
            "validate": True,
            "use_http": True,
        },
    )

**Parameters:**

* ``ca_certs`` - Path to CA certificate bundle (PEM format) for server verification.
* ``validate`` - Whether to validate the server certificate (default: ``true``).
* ``use_http`` - Use HTTP transport (typically required when HBase Thrift2 is configured
  with ``hbase.regionserver.thrift.http=true``).

Connection Pooling
~~~~~~~~~~~~~~~~~~

The provider supports connection pooling for improved performance:

**Pooling Features:**

* Configurable pool size for concurrent connections
* Configurable borrow timeout
* Automatic connection lifecycle management
* Thread-safe connection reuse

**Configuration Example:**

.. code-block:: python

    pooled_connection = Connection(
        conn_id="hbase_pooled",
        conn_type="hbase",
        host="hbase-server.example.com",
        port=9090,
        extra={
            "connection_pool": {
                "enabled": True,
                "size": 10,
                "timeout": 30,
            }
        },
    )

Security Best Practices
~~~~~~~~~~~~~~~~~~~~~~~

**Connection Security:**

* Enable SSL/TLS (``ca_certs`` + ``validate``) for all production connections
* Use connection pooling to reduce connection overhead
* Set appropriate connection timeouts

**Access Control:**

* Configure HBase ACLs to restrict table and column family access
* Use principle of least privilege for service accounts
* Implement proper network segmentation and firewall rules
* Monitor and audit HBase access logs

**Operational Security:**

* Store sensitive information in Airflow's connection management system
* Avoid hardcoding credentials in DAG files
* Regularly update HBase and Airflow to latest security patches

**Network Security:**

* Deploy HBase in a secure network environment
* Use VPNs or private networks for HBase communication
* Implement proper DNS security and hostname verification

For comprehensive security configuration, consult:

* `HBase Security Guide <https://hbase.apache.org/book.html#security>`_
* `Airflow Security Documentation <https://airflow.apache.org/docs/apache-airflow/stable/security/index.html>`_
