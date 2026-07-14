

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

``apache-airflow-providers-arenadata-hbase``
============================================

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

   Connections <connections>
   Operators <operators>
   Sensors <sensors>
   Hooks <hooks>
   Example DAGs <example-dags>

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Resources

   Installing from sources <installing-providers-from-sources>

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Commits

   Detailed list of commits <commits>


Package overview
----------------

`Apache HBase <https://hbase.apache.org/>`__ provider for Airflow by Arenadata.

Release: ``1.1.0``

Python package path:
``airflow.providers.arenadata.hbase``

Current provider scope
----------------------

This provider supports HBase interactions through two protocol layers:

* **HBase Thrift2 protocol** for table CRUD, scans, and batch operations
* **HBase CLI (command-line)** for administrative backup/restore workflows

The Thrift2 layer talks to a running ``HBase Thrift2`` server via the Apache
Thrift binary protocol. The CLI layer runs ``hbase backup`` commands on the
worker host through ``HBaseCLIHook``.

Both layers support plain, SSL/TLS, and Kerberos (GSSAPI) authentication,
configured via the Airflow connection extra.

Runtime deployment model
------------------------

This provider does not provision HBase client runtime by itself.
It is expected to run in one of the following environments:

* on a shared worker host or container image where HBase client tools
  (including ``hbase`` CLI and Java runtime) are already installed
  together with Airflow;
* on a worker host or container image prepared manually with the required
  HBase runtime files.

For manual preparation, the worker runtime should be provisioned with:

* Java runtime installed on the worker host or available in the container image;
* HBase CLI tools copied to a stable location such as ``/usr/lib/hbase``;
* HBase client configuration available at ``/etc/hbase/conf`` or a
  custom directory pointed to by ``HBASE_HOME`` and ``HBASE_CONF_DIR``;
* ``hbase`` available in ``PATH`` for CLI-based backup/restore operations;
* For Kerberos-enabled clusters: a valid keytab and passwordless ``sudo``
  access to run commands as the ``hbase`` user.

Quick start checklist
---------------------

For a minimal working setup:

1. Ensure a HBase Thrift2 server is running and accessible from the worker.
2. Create an ``hbase`` connection in Airflow with host, port (default 9090),
   and any required extra parameters (Kerberos, SSL).
3. Run the ``example_hbase`` DAG to verify connectivity.

Requirements
------------

* ``apache-airflow`` >= ``2.11.0``
* ``thrift`` >= ``0.13.0``
* ``thrift_sasl`` >= ``0.4.3``
* ``sasl`` >= ``0.3.1``

Connection contract
-------------------

Runtime connection parsing is centralized in:
``airflow/providers/arenadata/hbase/connection_config.py``
(``HBaseConnectionConfig``).

Built-in provider runtime uses typed configuration objects.
Raw ``Connection.extra`` is available for custom DAG-level extensions.

Example DAGs
------------

Example DAGs are located in:
``airflow/providers/arenadata/hbase/src/airflow/providers/arenadata/hbase/example_dags``.

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-arenadata-hbase package
------------------------------------------------------

`Apache HBase <https://hbase.apache.org/>`__ provider by Arenadata


Release: 1.1.0

Provider package
----------------

This package is for the ``arenadata.hbase`` provider.
All classes for this package are included in the ``airflow.providers.arenadata.hbase`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-arenadata-hbase``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.12.0``
``thrift``                                  ``>=0.13.0``
``thrift_sasl``                             ``>=0.4.3``
``sasl``                                    ``>=0.3.1``
==========================================  ==================

Cross provider package dependencies
-----------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider distributions in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-arenadata-hbase[common.compat]


==================================================================================================================  =================
Dependent package                                                                                                   Extra
==================================================================================================================  =================
`apache-airflow-providers-common-compat <https://airflow.apache.org/docs/apache-airflow-providers-common-compat>`_  ``common.compat``
`apache-airflow-providers-openlineage <https://airflow.apache.org/docs/apache-airflow-providers-openlineage>`_      ``openlineage``
==================================================================================================================  =================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-arenadata-hbase 1.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_arenadata_hbase-1.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_arenadata_hbase-1.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_arenadata_hbase-1.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-arenadata-hbase 1.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_arenadata_hbase-1.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_arenadata_hbase-1.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_arenadata_hbase-1.1.0-py3-none-any.whl.sha512>`__)
