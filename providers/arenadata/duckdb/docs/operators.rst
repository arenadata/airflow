
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

DuckDB Operators
================

The DuckDB provider exposes
:class:`~airflow.providers.arenadata.duckdb.operators.duckdb.DuckDbOperator`
to run DuckDB SQL through the DuckDB CLI (or ADO wrapper) on the worker and
return raw CLI ``stdout`` for XCom.

Prerequisites
-------------

To use this operator:

* Configure a :doc:`DuckDB Connection <connections>`.
* Make sure the ``duckdb`` binary (or ADO wrapper) is available on the worker
  at the path set in connection Extra ``duckdb_binary`` (default
  ``/usr/bin/duckdb``).

Common operator parameters
--------------------------

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
       when set (templated).
   * - ``parameters``
     - Optional mapping for textual ``%(name)s`` substitution in ``sql``
       **after** Jinja templating (templated). See the parameterized SQL
       section below.
   * - ``output_format``
     - CLI output format: ``"json"`` (default), ``"csv"``, or ``None``.
       Not templated.
   * - ``lock_retry_attempts``
     - Opt-in file-lock retries for the hook (``None`` = connection Extra /
       default ``0``). Not templated. See :ref:`duckdb-lock-retry`.
   * - ``log_output_limit``
     - Max characters of ``stdout`` / ``stderr`` on INFO logs and in
       exception text (``None`` = hook default). Not templated.
       See :doc:`connections`.

Templating
----------

``DuckDbOperator`` declares:

* ``template_fields = ("sql", "parameters", "database", "duckdb_conn_id")``
* ``template_ext = (".sql",)``

Airflow renders Jinja (and resolves ``.sql`` template files) **before**
``execute()`` runs. By the time ``execute()`` is called, ``sql`` is already the
final string (or the loaded file contents). Do not expect Jinja to run inside
``execute()`` itself.

When ``sql`` ends with ``.sql`` (case-sensitive), Airflow loads it from the DAG
bundle. Details for missing files and DAG-bundle behavior are in
:doc:`connections`.

XCom and output formats
-----------------------

``execute()`` returns raw CLI ``stdout`` as a ``str``. Airflow pushes that
string to XCom. It does **not** return parsed table rows.

With the default ``output_format="json"``, prefer
``airflow.providers.arenadata.duckdb.utils.json_output.parse_json_output``
(handles optional non-JSON prefixes and requires a JSON list of rows).
Plain ``json.loads`` is only safe for clean list-shaped JSON:

.. code-block:: python

    from airflow.providers.arenadata.duckdb.utils.json_output import parse_json_output

    raw = context["ti"].xcom_pull(task_ids="select_rows")
    rows = parse_json_output(raw)

``output_format`` values:

* ``"json"`` (default): CLI ``-json``; suitable for parsing and XCom hand-off.
* ``"csv"``: CLI ``-csv`` and ``-noheader``; ``stdout`` has **no** header row.
* ``None``: default human-readable DuckDB table layout; not suitable for
  reliable parsing.

Examples
--------

Inline SQL
~~~~~~~~~~

.. code-block:: python

    from airflow.providers.arenadata.duckdb.operators.duckdb import DuckDbOperator

    create_table = DuckDbOperator(
        task_id="create_table",
        sql="CREATE OR REPLACE TABLE demo(id INT, name VARCHAR);",
        duckdb_conn_id="duckdb_default",
    )

SQL file
~~~~~~~~

Pass a path ending in ``.sql``. Airflow loads the file via ``template_ext``
before execution (see :doc:`connections` for DAG-bundle notes):

.. code-block:: python

    load_from_file = DuckDbOperator(
        task_id="load_from_file",
        sql="queries/load_demo.sql",
        duckdb_conn_id="duckdb_default",
    )

Parameterized SQL (``%(name)s``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``parameters`` are applied **after** Jinja templating via textual substitution
(not driver bind parameters). Each ``%(name)s`` is replaced with a DuckDB SQL
literal:

* Supported value types: ``str``, ``int``, ``float``, ``Decimal``, ``bool``,
  ``None``, ``date``, ``datetime``. Other types raise ``TypeError`` (surfaced
  as a configuration error).
* Strings are escaped by doubling single quotes (``'`` -> ``''``).
* A missing key raises an error.
* Substitution is naive: a placeholder-looking pattern inside a SQL string
  literal (for example ``LIKE '%(abc)s'``) is still replaced when
  ``parameters`` is non-empty.
* Do **not** use ``parameters`` for identifiers (table/column names). Use Jinja
  for those.

This is **not** the same as prepared-statement binding. Prefer trusted
parameter values; do not treat ``parameters`` as full SQL-injection protection.

.. code-block:: python

    insert_row = DuckDbOperator(
        task_id="insert_row",
        sql="INSERT INTO demo VALUES (%(id)s, %(name)s);",
        parameters={"id": 1, "name": "alpha"},
        duckdb_conn_id="duckdb_default",
    )

Override database path
~~~~~~~~~~~~~~~~~~~~~~

``database`` overrides Connection ``host`` for this task:

.. code-block:: python

    select_count = DuckDbOperator(
        task_id="select_count",
        sql="SELECT count(*) AS c FROM demo",
        database="/tmp/example_duckdb_sql_file.duckdb",
        duckdb_conn_id="duckdb_default",
        output_format="json",
    )

CSV export (no header row)
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    export_csv = DuckDbOperator(
        task_id="export_csv",
        sql="SELECT * FROM demo ORDER BY id",
        output_format="csv",
        duckdb_conn_id="duckdb_default",
    )

See also
--------

Connection Extra details that apply to every operator run (and are not repeated
here):

* ``cli_params`` ban-list and secrets masking: :doc:`connections`
* File lock conflicts and ``lock_retry_attempts``: :ref:`duckdb-lock-retry`
* Logging levels for ``stdout`` / ``stderr``: :doc:`connections`
* Process lifecycle (``on_kill``, process group): :doc:`connections`
* SQL files and DAG bundle behavior: :doc:`connections`

Example DAGs: :doc:`example-dags`.

Design notes
------------

``DuckDbOperator`` stays thin on purpose:

* connection parsing, ``preflight``, CLI execution, logging, and errors live in
  :class:`~airflow.providers.arenadata.duckdb.hooks.duckdb.DuckDbHook`;
* the operator focuses on Airflow templating, task arguments, XCom return
  value, and forwarding ``on_kill`` to the hook.
