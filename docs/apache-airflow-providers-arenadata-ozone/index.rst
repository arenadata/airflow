
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


apache-airflow-providers-arenadata-ozone package
------------------------------------------------

`Apache Ozone <https://ozone.apache.org/>`__ (Arenadata provider)

Release: 1.0.0

Provider package
----------------

All classes are in the ``airflow.providers.arenadata.ozone`` Python package.
Supports Native CLI (``ofs://`` / ``o3fs://``), admin, and filesystem workflows.

Requirements
------------

* ``apache-airflow`` >= 2.10.3
* ``apache-airflow-providers-apache-hdfs``

Example DAGs
------------

In the Airflow source tree: ``airflow/providers/arenadata/ozone/example_dags/``
