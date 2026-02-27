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

``apache-airflow-providers-apache-hbase``
=========================================


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Basics

    Home <self>
    Changelog <changelog>
    Security <security>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/hbase>
    Operators <operators>
    Sensors <sensors>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/apache/hbase/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/providers/apache/hbase/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example DAGs <https://github.com/apache/airflow/tree/main/airflow/providers/hbase/example_dags>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-apache-hbase package
----------------------------------------------

`Apache HBase <https://hbase.apache.org/>`__ is a distributed, scalable, big data store built on Apache Hadoop.
It provides random, real-time read/write access to your big data and is designed to host very large tables
with billions of rows and millions of columns.

This provider package contains operators, hooks, and sensors for interacting with HBase, including:

- **Table Operations**: Create, delete, and manage HBase tables
- **Data Operations**: Insert, retrieve, scan, and batch operations on table data
- **Backup & Restore**: Full and incremental backup operations with restore capabilities
- **Monitoring**: Sensors for table existence, row counts, and column values
- **Security**: SSL/TLS encryption and Kerberos authentication support
- **Performance**: Connection pooling and optimized batch operations
- **Integration**: Seamless integration with Airflow Secrets Backend

Release: 1.0.0

Provider package
----------------

This package is for the ``hbase`` provider.
All classes for this package are included in the ``airflow.providers.hbase`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-apache-hbase``

Configuration
-------------

To use this provider, you need to configure an HBase connection in Airflow.
The provider supports Thrift2 protocol connections with optional Kerberos authentication and connection pooling.

For detailed connection configuration examples, see the :doc:`connections guide <connections/hbase>`.

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.7.0``.

=========================  ==================
PIP package                Version required
=========================  ==================
``apache-airflow``         ``>=2.7.0``
``thrift``                 ``>=0.16.0``
``kerberos``               ``>=1.3.0``
=========================  ==================

Features
--------

**Operators**

- ``HBaseCreateTableOperator`` - Create HBase tables with column families
- ``HBaseDeleteTableOperator`` - Delete HBase tables
- ``HBasePutOperator`` - Insert single rows into tables
- ``HBaseBatchPutOperator`` - Insert multiple rows in batch
- ``HBaseBatchGetOperator`` - Retrieve multiple rows in batch
- ``HBaseScanOperator`` - Scan tables with filtering options
- ``HBaseBackupSetOperator`` - Manage backup sets (add, list, describe, delete)
- ``HBaseCreateBackupOperator`` - Create full or incremental backups
- ``HBaseRestoreOperator`` - Restore tables from backups
- ``HBaseBackupHistoryOperator`` - View backup history

**Sensors**

- ``HBaseTableSensor`` - Monitor table existence
- ``HBaseRowSensor`` - Monitor row existence
- ``HBaseRowCountSensor`` - Monitor row count thresholds
- ``HBaseColumnValueSensor`` - Monitor specific column values

**Hooks**

- ``HBaseHook`` - Core hook for HBase operations via Thrift API and shell commands

**Security Features**

- **Kerberos Authentication** - Enterprise authentication with keytab support
- **Secrets Integration** - Keytab management via Airflow Secrets Backend
- **Connection Pooling** - Efficient connection reuse for high-performance operations