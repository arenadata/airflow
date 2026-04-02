
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

``apache-airflow-providers-arenadata-ozone``
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

   Connection types <connections/index>
   Example DAG configuration <example-dags>

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

`Apache Ozone <https://ozone.apache.org/>`__ provider package for Airflow.

Release: ``1.0.0``

Python package path:
``airflow.providers.arenadata.ozone``

Current provider scope
----------------------

This provider is Native CLI-first and supports Ozone workflows through
``ozone sh`` and ``ozone fs`` commands:

* Ozone administration (volume/bucket lifecycle, quotas, admin extras)
* Ozone filesystem operations (create/list/delete/move/copy/upload/download)
* Ozone path sensors
* HDFS -> Ozone DistCp transfer
* Ozone bucket snapshot backup operator

Not in scope
------------

* No built-in S3 layer in this provider
* No compatibility aliases for connection keys

Connection contract
-------------------

Runtime connection parsing is centralized in:
``airflow/providers/arenadata/ozone/utils/connection_schema.py``
(``OzoneConnSnapshot``).

Built-in provider runtime uses typed snapshot fields.
Raw ``Connection.extra`` is exposed as ``snapshot.raw_extra`` only for
custom DAG-level extensions.

Requirements
------------

* ``apache-airflow`` >= ``2.10.3``
* ``apache-airflow-providers-apache-hdfs``

Example DAGs
------------

Example DAGs are located in:
``airflow/providers/arenadata/ozone/example_dags/``.
