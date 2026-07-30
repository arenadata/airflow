

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

.. _howto/connection:duckdb:

DuckDB Connection
=================

The DuckDB connection type configures CLI access to a DuckDB database file.

Default connection IDs
----------------------

* ``duckdb_default`` - default connection used by ``DuckDbHook`` when no explicit
  ``duckdb_conn_id`` is provided.

Connection fields
-----------------

.. list-table::
   :header-rows: 1

   * - Field
     - Description
   * - Host
     - Path to the ``.duckdb`` file, or ``:memory:`` for an in-memory database
   * - Extra ``duckdb_binary``
     - Path to the DuckDB binary (default: ``/usr/bin/duckdb``)
   * - Extra ``timeout``
     - Subprocess timeout in seconds (default: 300)
   * - Extra ``readonly``
     - Open the database in read-only mode (default: false)
   * - Extra ``cli_params``
     - Additional CLI parameters passed to the DuckDB binary

Example Extra JSON
------------------

.. code-block:: json

   {
     "duckdb_binary": "/usr/bin/duckdb",
     "timeout": 300,
     "readonly": false,
     "cli_params": ""
   }
