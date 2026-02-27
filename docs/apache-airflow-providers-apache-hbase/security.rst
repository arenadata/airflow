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

The Apache HBase provider uses the Thrift2 protocol to connect to HBase with Kerberos authentication support.

Kerberos Authentication
~~~~~~~~~~~~~~~~~~~~~~~

The provider supports Kerberos authentication for secure access to HBase clusters:

**Kerberos Features:**

* SASL/GSSAPI authentication mechanism
* Keytab-based authentication
* Principal and realm configuration
* Integration with Airflow Secrets Backend for keytab storage

**Configuration Example:**

.. code-block:: python

    # Kerberos connection
    kerberos_connection = Connection(
        conn_id="hbase_kerberos",
        conn_type="hbase",
        host="hbase-kerb.example.com",
        port=9090,
        extra={
            "use_kerberos": True,
            "kerberos_principal": "airflow@EXAMPLE.COM",
            "kerberos_keytab_path": "/etc/security/keytabs/airflow.keytab"
        }
    )

    # Kerberos with keytab from Secrets Backend
    kerberos_secrets_connection = Connection(
        conn_id="hbase_kerberos_secrets",
        conn_type="hbase",
        host="hbase-kerb.example.com",
        port=9090,
        extra={
            "use_kerberos": True,
            "kerberos_principal": "airflow@EXAMPLE.COM",
            "kerberos_keytab_secret_key": "hbase_keytab"
        }
    )

Connection Pooling
~~~~~~~~~~~~~~~~~~

The provider supports connection pooling for improved performance:

**Pooling Features:**

* Configurable pool size for concurrent connections
* Automatic connection lifecycle management
* Thread-safe connection reuse
* Reduced connection overhead for batch operations

**Configuration Example:**

.. code-block:: python

    # Connection with pooling
    pooled_connection = Connection(
        conn_id="hbase_pooled",
        conn_type="hbase",
        host="hbase.example.com",
        port=9090,
        extra={
            "pool_size": 5
        }
    )

Security Best Practices
~~~~~~~~~~~~~~~~~~~~~~~

**Connection Security:**

* Use Kerberos authentication in production environments
* Store keytabs securely using Airflow Secrets Backend
* Regularly rotate Kerberos principals and keytabs
* Use connection pooling to minimize authentication overhead

**Access Control:**

* Configure HBase ACLs to restrict table and column family access
* Use principle of least privilege for service accounts
* Implement proper network segmentation and firewall rules
* Monitor and audit HBase access logs

**Operational Security:**

* Store sensitive information in Airflow's connection management system
* Avoid hardcoding credentials in DAG files
* Use Airflow's secrets backend for keytab storage
* Regularly update HBase and Airflow to latest security patches

**Network Security:**

* Deploy HBase in a secure network environment
* Use VPNs or private networks for HBase communication
* Implement proper DNS security and hostname verification
* Monitor network traffic for anomalies

For comprehensive security configuration, consult:

* `HBase Security Guide <https://hbase.apache.org/book.html#security>`_
* `Airflow Security Documentation <https://airflow.apache.org/docs/apache-airflow/stable/security/index.html>`_
* `Kerberos Authentication Guide <https://web.mit.edu/kerberos/krb5-latest/doc/>`_