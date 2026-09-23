
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

DuckDB Sensors
==============

The DuckDB provider exposes
:class:`~airflow.providers.arenadata.duckdb.sensors.duckdb.DuckDbSqlSensor`
to wait until a DuckDB SQL query returns a truthy result. The sensor runs SQL
through :class:`~airflow.providers.arenadata.duckdb.hooks.duckdb.DuckDbHook`
(DuckDB CLI / ADO wrapper).

Prerequisites
-------------

* Configure a :doc:`DuckDB Connection <connections>`.
* Make sure the ``duckdb`` binary (or ADO wrapper) is available on the worker.
* The query must target an **existing** table, view, or dataset path that DuckDB
  can read. Waiting for a missing object is not supported (see behavior below).

Behavior
--------

On each ``poke()`` the sensor:

1. Runs ``sql`` with ``output_format="json"`` via the hook.
2. Parses the JSON result set (one statement / first JSON array; see
   :ref:`duckdb-sensor-single-statement` and :doc:`operators`).
3. Evaluates the **first cell of the first row** as a Python truthy check
   (``bool(value)``).

Returns ``False`` (keep waiting) when:

* the result set is empty (including empty CLI ``stdout``);
* the first row is missing, not a mapping, or has no columns;
* the first cell is falsy in Python (for example ``false``, ``0``, ``null``,
  or an empty string). Note that non-empty strings such as ``"0"`` are truthy.

Returns ``True`` (success) when the first cell is truthy.

**CLI and JSON errors fail the task by default.** Non-zero DuckDB CLI exit
codes and invalid JSON raise provider exceptions (subclasses of
``AirflowException``). With default sensor flags they are **not** treated as
"not ready yet".

BaseSensorOperator flags change that:

* ``silent_fail=True``: poke exceptions become ``False`` (keep waiting).
* ``never_fail=True``: poke exceptions skip the task.
* ``soft_fail=True`` does **not** convert generic CLI/JSON errors into wait/skip;
  it applies to ``AirflowFailException`` / timeouts (see ``fail_on_empty``).

Important caveat: a query against a missing table (for example
``Catalog Error: Table ... does not exist``) fails the task immediately with
default flags. It does **not** keep waiting. If you need to wait until data
appears, create the table (or view) ahead of time - empty is fine - and poke a
condition such as ``SELECT count(*) > 0 ...``.

.. _duckdb-sensor-single-statement:

Setup statements in ``sql``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The sensor parses only the **first JSON array** in ``stdout``; later statements
are discarded with a log warning. No statement before the condition may return
rows.

* ``INSTALL``, ``LOAD``, ``ATTACH``, ``SET`` print nothing and may precede the
  condition.
* ``CREATE SECRET`` returns a success row. It becomes the first result set, so
  the sensor succeeds without evaluating the condition - the task turns green
  with only a warning in the logs.
* Each task is a separate CLI process: ``LOAD`` / ``ATTACH`` must stay in the
  sensor's ``sql``, while ``INSTALL``, ``CREATE PERSISTENT SECRET`` and
  ``CREATE VIEW`` can run once in a preceding ``DuckDbOperator``.

.. code-block:: sql

    -- wrong: the sensor reads the CREATE SECRET row, not the condition
    CREATE SECRET s3_lake (TYPE s3, PROVIDER credential_chain);
    SELECT count(*) > 0 FROM events;

``fail_on_empty``
~~~~~~~~~~~~~~~~~

When ``fail_on_empty=True`` and the query returns no rows, the sensor raises
``AirflowFailException`` (no task retries for "no data yet"). With
``soft_fail=True``, that failure becomes a skip. When ``fail_on_empty=False``
(default), an empty result returns ``False`` and the sensor keeps waiting.

Aggregates such as ``SELECT count(*) ...`` always return one row (for example
``0``), so they are **not** an empty result set. For those queries use the
truthiness of the first cell (``0`` waits, ``> 0`` succeeds), not
``fail_on_empty``.

Lock retries in poke
~~~~~~~~~~~~~~~~~~~~

Each ``poke()`` creates the hook with ``lock_retry_attempts=0``. Waiting for a
busy ``.duckdb`` file (or for data) is done via Airflow ``poke_interval`` /
``mode="reschedule"``, not via in-poke CLI lock retries. See
:ref:`duckdb-lock-retry`.

Parameters
----------

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``sql``
     - SQL statement, or path to a ``.sql`` template file (templated).
   * - ``duckdb_conn_id``
     - Airflow connection ID (default: ``duckdb_default``; templated).
   * - ``database``
     - Optional path to a ``.duckdb`` file; overrides Connection ``host``
       (templated).
   * - ``parameters``
     - Optional mapping for textual ``%(name)s`` substitution after Jinja
       (templated). Same semantics as :doc:`operators` (not driver binds).
   * - ``fail_on_empty``
     - If ``True``, raise ``AirflowFailException`` on an empty result set.
       Default ``False``. Not templated.
   * - ``log_output_limit``
     - Max characters of ``stdout`` / ``stderr`` on INFO logs and in
       exception text (``None`` = hook default). Not templated.

Inherits ``timeout``, ``poke_interval``, ``mode``, ``soft_fail``,
``silent_fail``, and ``never_fail`` from
:class:`~airflow.sdk.bases.sensor.BaseSensorOperator`.
Sensor ``timeout`` is the overall wait horizon; per-CLI attempt timeout comes
from the connection Extra ``timeout`` (see :doc:`connections`).

Templating
----------

Same as :doc:`operators`:

* ``template_fields = ("sql", "parameters", "database", "duckdb_conn_id")``
* ``template_ext = (".sql",)``

Jinja and ``.sql`` file loading run before ``poke()``. SQL file / DAG-bundle
details: :doc:`connections`.

Example
-------

.. code-block:: python

    from airflow.providers.arenadata.duckdb.sensors.duckdb import DuckDbSqlSensor

    wait_for_rows = DuckDbSqlSensor(
        task_id="wait_for_rows",
        sql="SELECT count(*) AS c FROM demo",
        duckdb_conn_id="duckdb_default",
        mode="reschedule",
        poke_interval=30,
        timeout=300,
    )

Use case: wait for external data
--------------------------------

Typical pattern: an external job or another DAG loads data into a DuckDB table,
file, or external dataset; this sensor blocks until a truthy condition appears:

.. code-block:: python

    from airflow.providers.arenadata.duckdb.operators.duckdb import DuckDbOperator
    from airflow.providers.arenadata.duckdb.sensors.duckdb import DuckDbSqlSensor

    wait_until_ready = DuckDbSqlSensor(
        task_id="wait_until_ready",
        sql="queries/wait_until_ready.sql",
        duckdb_conn_id="duckdb_default",
        poke_interval=60,
        timeout=3600,
    )

    process = DuckDbOperator(
        task_id="process",
        sql="SELECT * FROM demo WHERE ready",
        duckdb_conn_id="duckdb_default",
    )

    wait_until_ready >> process

See :doc:`example-dags` for full example DAGs.
