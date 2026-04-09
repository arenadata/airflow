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

Apache Ozone Hooks
==================

The provider keeps most Ozone runtime logic in hooks. Operators and sensors are
thin by design and delegate CLI execution, connection parsing, security wiring,
and retries to this layer.

OzoneCliHook
------------

``OzoneCliHook`` is the base hook for native Ozone CLI execution.

Main responsibilities:

* read Airflow connection data into ``OzoneConnSnapshot``;
* prepare Ozone CLI environment variables;
* apply SSL and Kerberos settings from connection extra;
* run commands through the common CLI runner with retries and timeouts;
* implement ``test_connection()`` for the ``ozone`` connection type.

This hook uses ``ozone_default`` by default.

OzoneFsHook
-----------

``OzoneFsHook`` extends ``OzoneCliHook`` for filesystem workflows based on
``ozone fs``.

Typical API areas:

* create and delete paths;
* check path existence;
* list keys and wildcard matches;
* upload and download files;
* move and copy data inside Ozone;
* inspect key metadata and selected key properties.

This is the main hook behind filesystem operators and sensors.

OzoneAdminHook
--------------

``OzoneAdminHook`` extends ``OzoneCliHook`` for administrative workflows based
on ``ozone sh``.

Typical API areas:

* create and delete volumes;
* create and delete buckets;
* inspect volume and bucket metadata;
* set and clear quotas;
* list volumes and buckets.

This hook uses ``ozone_admin_default`` by default.

OzoneAdminExtraHook
-------------------

``OzoneAdminExtraHook`` extends ``OzoneAdminHook`` with advanced administrative
operations.

Typical API areas:

* snapshot management;
* bucket link creation;
* bucket replication configuration;
* tenant management;
* SCM container reports.

When to use hooks directly
--------------------------

Using operators and sensors is preferred for standard DAG tasks.
Use hooks directly when you need:

* custom Python branching around Ozone results;
* direct access to structured metadata in task code;
* provider extension code that should reuse connection parsing and CLI helpers.
