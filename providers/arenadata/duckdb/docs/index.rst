
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

``apache-airflow-providers-arenadata-duckdb``
=============================================

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

`DuckDB <https://duckdb.org/>`__ provider for Airflow by Arenadata.

Release: ``1.0.0``

Python package path:
``airflow.providers.arenadata.duckdb``

Current provider scope
----------------------

This provider is **CLI-only**: it runs DuckDB SQL through the DuckDB CLI (or
ADO wrapper) in a subprocess. It does not embed the Python ``duckdb`` package.

Included building blocks:

* :class:`~airflow.providers.arenadata.duckdb.hooks.duckdb.DuckDbHook`
* :class:`~airflow.providers.arenadata.duckdb.operators.duckdb.DuckDbOperator`
* :class:`~airflow.providers.arenadata.duckdb.sensors.duckdb.DuckDbSqlSensor`

Runtime deployment model
------------------------

This provider does not install the DuckDB binary by itself. The worker must
already have a ``duckdb`` executable (upstream CLI or ADO wrapper that applies
global DuckDB configuration before starting DuckDB).

Configure the path in the DuckDB binary Connection field
(default ``/usr/bin/duckdb``). On macOS with Homebrew the binary is often
``/opt/homebrew/bin/duckdb``.

Quick start checklist
---------------------

For a minimal working setup:

1. Install or provision the ``duckdb`` CLI / ADO wrapper on the worker.
2. Create a ``duckdb`` connection in Airflow (``host`` = path to a ``.duckdb``
   file or ``:memory:``; DuckDB binary field if not at the default path).
3. Verify with connection ``test_connection()`` or an example DAG
   (see :doc:`example-dags`).

Connection contract
-------------------

Connection fields are parsed at runtime by ``DuckDbHook``. There is no separate
connection schema module. Full field reference, Extra keys, ban-list, logging,
and lock behavior: :doc:`connections`.

Not in scope
------------

* No embedded Python DuckDB API (``import duckdb``): execution is CLI-only.
* ``:memory:`` databases are not shared across tasks: each task runs a separate
  CLI subprocess, so in-memory state does not persist between tasks.
* DuckDB allows only one writer process per ``.duckdb`` file. Use separate
  files, an Airflow pool, or ``lock_retry_attempts`` as documented in
  :doc:`connections`.

Guides
------

* :doc:`connections`: Connection fields and Extra
* :doc:`operators`: ``DuckDbOperator`` how-to
* :doc:`sensors`: ``DuckDbSqlSensor`` how-to
* :doc:`hooks`: ``DuckDbHook`` API and when to call it directly
* :doc:`example-dags`: shipped example DAGs

Requirements
------------

* ``apache-airflow`` >= ``3.2.1``

Example DAGs
------------

Example DAGs are located in:
``providers/arenadata/duckdb/src/airflow/providers/arenadata/duckdb/example_dags``.

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-arenadata-duckdb package
------------------------------------------------------

`DuckDB <https://duckdb.org/>`__ provider by Arenadata.

Runs DuckDB SQL through the DuckDB CLI (or ADO wrapper) with:

- ``DuckDbOperator`` for SQL execution from DAGs
- ``DuckDbSqlSensor`` for waiting on truthy query results
- ``DuckDbHook`` and a ``duckdb`` Connection type


Release: 1.0.0

Provider package
----------------

This package is for the ``arenadata.duckdb`` provider.
All classes for this package are included in the ``airflow.providers.arenadata.duckdb`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-arenadata-duckdb``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

==================  ==================
PIP package         Version required
==================  ==================
``apache-airflow``  ``>=3.0.0``
==================  ==================
