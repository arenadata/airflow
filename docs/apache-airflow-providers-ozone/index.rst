
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

``apache-airflow-providers-ozone``
==================================

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

    Connection types <connections/index>
    Operators <operators/index>
    Transfers <transfers/index>
    How-to guides <howto/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/ozone/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Example DAGs <example-dags>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-ozone/>
    Installing from sources <installing-providers-from-sources>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-ozone package
--------------------------------------

`Apache Ozone <https://ozone.apache.org/>`__

Release: 1.0.0

Provider package
----------------

This package is for the ``ozone`` provider.
All classes for this package are included in the ``airflow.providers.arenadata.ozone`` python package.

The provider supports both:

* Native CLI operations via the ``ozone`` binary (for ``ofs://`` / ``o3fs://`` paths).
* S3 Gateway operations via the provider's own boto3-based S3 client and ``ozone_s3`` connection type.

Key features
------------

* Robust error handling with retries (transient failures) and subprocess timeouts.
* Security-first: SSL/TLS and Kerberos support for Native CLI, and Secrets Backend integration (``secret://``).
* Create/delete operations treat "already exists" and "not found" as success where applicable.
* Detailed logging with automatic sensitive-data masking.
* High-performance transfers (parallel transfers for Ozone → S3).

Installation
------------

You can install this package on top of an existing Airflow installation via:

.. code-block:: bash

    pip install apache-airflow-providers-ozone

For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider package is ``2.10.3``.

==============================  ==================
PIP package                     Version required
==============================  ==================
``apache-airflow``              ``>=2.10.3``
==============================  ==================

Provider dependencies
---------------------

* ``apache-airflow-providers-apache-hive`` (required for ``OzoneToHiveOperator`` shipped by this provider).

Runtime prerequisites (Native CLI)
---------------------------------

For Native CLI operations (filesystem/admin/snapshot):

* Java (JDK 11+)
* ``ozone`` CLI binary available in ``PATH``
* Ozone/Hadoop config available to the CLI (e.g. via ``OZONE_CONF_DIR`` / ``HADOOP_CONF_DIR``)

Example DAGs
------------

Example DAGs are available in ``airflow/providers/arenadata/ozone/example_dags`` in the Airflow source tree.
