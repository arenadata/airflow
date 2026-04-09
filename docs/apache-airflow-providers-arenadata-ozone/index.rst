
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

   Connections <connections>
   Operators <operators>
   Sensors <sensors>
   Transfers <transfers>
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
* Connection-driven SSL and Kerberos runtime wiring

Runtime deployment model
------------------------

This provider does not provision Ozone client runtime by itself.
It is expected to run in one of the following environments:

* on a shared worker host or container image where Ozone client runtime is already
  installed and maintained together with Airflow;
* on a worker host or container image prepared manually with the required
  Ozone runtime files.

For manual preparation, the worker runtime should be provisioned with:

* Java runtime installed on the worker host or available in the container image;
* Ozone CLI runtime copied to a stable location such as ``/opt/ozone``;
* Ozone client configuration copied to a stable directory such as
  ``/etc/ozone/conf``;
* at minimum, ``core-site.xml`` and ``ozone-site.xml`` available in that
  configuration directory;
* environment variables wired for task runtime, typically
  ``JAVA_HOME``, ``JAVA``, ``OZONE_HOME``, ``OZONE_LIBEXEC_DIR``,
  ``OZONE_CONF_DIR``, and ``HADOOP_CONF_DIR``;
* ``ozone`` available in ``PATH`` for interactive shell runs and for Airflow
  service runtime;
* working hostname resolution for Ozone Manager and related cluster endpoints,
  whether via DNS or explicit host mappings.

In practical terms, this means the provider works best when Airflow runs close
to an Ozone-enabled runtime, or when the same runtime is reproduced explicitly
on the worker host or in the container image.

Quick start checklist
---------------------

For a minimal working setup:

1. Prepare Ozone client runtime on the worker host or in the container image.
2. Make ``ozone`` available in ``PATH`` together with Java runtime.
3. Place ``core-site.xml`` and ``ozone-site.xml`` in a directory available to
   the worker, then point ``ozone_conf_dir`` to that directory in the Airflow
   connection.
4. Create an ``ozone`` connection in Airflow and verify it with
   ``test_connection()`` or an example DAG.

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

Example DAGs
------------

Example DAGs are located in:
``airflow/providers/arenadata/ozone/example_dags/``.
