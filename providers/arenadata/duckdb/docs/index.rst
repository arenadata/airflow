

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

This provider runs DuckDB SQL through the DuckDB CLI in a subprocess. It includes:

* :class:`~airflow.providers.arenadata.duckdb.hooks.duckdb.DuckDbHook`
* :class:`~airflow.providers.arenadata.duckdb.operators.duckdb.DuckDbOperator`
* :class:`~airflow.providers.arenadata.duckdb.sensors.duckdb.DuckDbSqlSensor`

Runtime deployment model
------------------------

The worker must have the ``duckdb`` binary available at the path configured in the
Airflow connection (default ``/usr/bin/duckdb``).

Requirements
------------

* ``apache-airflow`` >= ``3.0.0``

Example DAGs
------------

Example DAGs are located in:
``airflow/providers/arenadata/duckdb/src/airflow/providers/arenadata/duckdb/example_dags``.

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!
