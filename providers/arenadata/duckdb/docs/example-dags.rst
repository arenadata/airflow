

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

DuckDB Example DAGs
===================

This provider ships example DAGs that run DuckDB SQL through the DuckDB CLI in a
subprocess. All examples require Apache Airflow 3.0+ and the ``duckdb`` binary on
the worker host.

Where example DAGs are located
------------------------------

``providers/arenadata/duckdb/src/airflow/providers/arenadata/duckdb/example_dags/``

Main examples:

* ``example_duckdb_basic.py``
* ``example_duckdb_sql_file.py``
* ``example_duckdb_sensors.py``

SQL templates used by the examples:

* ``example_dags/queries/load_demo.sql``
* ``example_dags/queries/wait_until_ready.sql``

Prerequisites
-------------

Before running any example DAG:

* Install the ``duckdb`` CLI on the worker (default path ``/usr/bin/duckdb``; on
  macOS with Homebrew use ``/opt/homebrew/bin/duckdb``).
* Create an Airflow Connection:

  * Connection ID: ``duckdb_default``
  * Connection Type: ``duckdb``
  * Host: path to the ``.duckdb`` file (for example ``/tmp/example_duckdb.duckdb``)
  * Extra (JSON): ``{"duckdb_binary": "/usr/bin/duckdb"}``

See :doc:`connections` for all connection fields and extras.

Important notes
---------------

* Do not use ``:memory:`` in multi-task DAGs. Each task runs the DuckDB CLI in a
  separate subprocess, so in-memory state does not persist between tasks.
* DuckDB allows only one writer process per database file. Do not run
  ``example_duckdb_basic`` and ``example_duckdb_sensors`` at the same time when
  they share ``duckdb_default.host``. Use separate Connections for parallel runs.
* ``DuckDbSqlSensor`` waits for a truthy query result against an **existing**
  table or view. A query against a missing object fails the task; it does not keep
  waiting.
* In production the data producer is often an external process or another DAG; the
  sensor then blocks until data appears.

Available example DAGs
------------------------

``example_duckdb_basic.py`` - Basic pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Demonstrates a minimal end-to-end DuckDB workflow with inline SQL, a sensor, and
XCom-friendly operator output.

* Creates a demo table with ``DuckDbOperator``
* Inserts sample rows
* Waits for rows with ``DuckDbSqlSensor`` (``mode="reschedule"``)
* Runs a final ``SELECT count(*)`` and returns stdout for XCom

Connection ID: ``duckdb_default``.

Task chain: ``create_table >> insert_data >> wait_for_rows >> select_count``.

Best for a first smoke-check after installing the provider.

``example_duckdb_sql_file.py`` - SQL files, Jinja, and database override
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Demonstrates operator features beyond inline SQL:

* Jinja templating in inline SQL (``{{ params.table }}``, ``{{ params.label }}``)
* SQL loaded from a file (``queries/load_demo.sql`` via ``template_ext``)
* Per-task ``database="{{ params.db_path }}"`` override of Connection ``host``
* ``output_format="csv"`` on an export task

DAG ``params`` (overridable from Trigger UI / ``dag_run.conf``):

* ``db_path`` - path to the ``.duckdb`` file (default
  ``/tmp/example_duckdb_sql_file.duckdb``)
* ``table`` - table name (default ``demo``)
* ``label`` - value inserted by the SQL file (default ``from_sql_file``)

Set ``params.db_path`` to a file **different** from ``duckdb_default.host`` so the
override demo is visible (for example host ``/tmp/example_duckdb.duckdb`` and
``db_path`` ``/tmp/example_duckdb_sql_file.duckdb``).

Task chain: ``create_table >> load_from_file >> export_csv``.

``example_duckdb_sensors.py`` - Sensor patterns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Demonstrates ``DuckDbSqlSensor`` with inline SQL and with a SQL file template.

* Seeds data with ``DuckDbOperator`` (``CREATE OR REPLACE TABLE`` + ``INSERT``)
* ``wait_inline`` - sensor with templated inline SQL
* ``wait_from_sql_file`` - sensor using ``queries/wait_until_ready.sql``

DAG ``params``:

* ``table`` - table name (default ``events``)
* ``min_id`` - minimum row id used by the SQL file sensor (default ``1``)

Connection ID: ``duckdb_default``; the database path comes from Connection
``host`` (this DAG does not set per-task ``database=``).

Task chain: ``seed_data >> wait_inline >> wait_from_sql_file``.

The sensor implementation also supports ``fail_on_empty`` and ``soft_fail``; this
example DAG does not demonstrate those flags.

Configuration
-------------

* Default connection: ``duckdb_default`` (override per task with ``duckdb_conn_id``).
* ``example_duckdb_sql_file`` and ``example_duckdb_sensors`` accept DAG ``params``
  and Trigger UI / ``dag_run.conf`` overrides at run time.
* Per-task ``database=`` on ``DuckDbOperator`` and ``DuckDbSqlSensor`` overrides
  Connection ``host`` (same semantics as documented in :doc:`connections`).

DAG developer notes
-------------------

* All example DAGs use ``schedule=None`` and ``catchup=False``; they do not
  backfill.
* Example DAGs require the ``duckdb`` CLI on the worker at the path configured in
  the connection extra.
* Connection IDs in the examples are configurable; create the required connections
  before triggering a DAG run.
* See also :doc:`hooks`, :doc:`operators`, and :doc:`sensors` for API reference.
