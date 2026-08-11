
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

DuckDB Hooks
============

The provider keeps DuckDB runtime logic in
:class:`~airflow.providers.arenadata.duckdb.hooks.duckdb.DuckDbHook`.
Operators and sensors are thin and delegate connection parsing, ``preflight``,
CLI execution, logging, and errors to this layer.

The hook registers under the ``duckdb`` connection type (default connection ID
``duckdb_default``).

Role
----

On each run the hook:

1. Reads the Airflow Connection (``host`` = database path, Extra fields).
2. Applies ``cli_params`` ban-list and secrets masking, then binary
   ``preflight``.
3. Soft-checks the local database path when applicable.
4. Builds and runs the DuckDB CLI (or ADO wrapper) command.
5. Writes ``stdout`` / ``stderr`` to task logs and raises typed errors on
   failure.

Full field reference, ban-list, lock retries, logging levels, and process
lifecycle: :doc:`connections`.

Main methods
------------

``run_cli``
~~~~~~~~~~~

Execute an SQL string via the DuckDB CLI.

After optional textual ``%(name)s`` substitution (same rules as
:doc:`operators`), the hook writes SQL to a temporary file and runs it with
``-f``. That avoids Linux ``MAX_ARG_STRLEN`` limits on large inline ``-c``
arguments, so large SQL scripts work.

Default ``output_format`` is ``"json"``.

Returns raw stripped ``stdout`` as a ``str``.

``run_file``
~~~~~~~~~~~~

Execute an existing ``.sql`` file path via ``-f``.

The file is run as-is. ``%(name)s`` parameter substitution is **not** applied.
Use ``run_cli`` when you need parameterized SQL.

Default ``output_format`` is ``None`` (human-readable CLI table), unlike
``run_cli``. Pass ``output_format="json"`` explicitly when you need to parse
rows.

Raises a configuration error if the file does not exist.

``test_connection``
~~~~~~~~~~~~~~~~~~~

Used by the Airflow UI / connection test for the ``duckdb`` connection type.

The probe runs ``SELECT 1`` against ``:memory:`` (database-path ``preflight``
is skipped). It validates that the binary is reachable and that connection
Extra parsing (including ``cli_params`` ban-list / secrets registration)
succeeds.

It does **not** verify that the Connection ``host`` ``.duckdb`` file exists or
is writable.

``on_kill``
~~~~~~~~~~~

Marks the hook as killed and terminates the active DuckDB process group (used
by operators and sensors when the task is killed). Details:
:doc:`connections`.

When to use the hook directly
-----------------------------

Prefer :doc:`operators` and :doc:`sensors` for standard DAG tasks.

Use ``DuckDbHook`` directly when you need:

* custom Python branching around CLI ``stdout``;
* calling ``run_file`` or ``run_cli`` from a ``@task`` / PythonOperator;
* reuse of connection parsing and CLI helpers in extension code.

Example:

.. code-block:: python

    from airflow.providers.arenadata.duckdb.hooks.duckdb import DuckDbHook
    from airflow.providers.arenadata.duckdb.utils.json_output import parse_json_output

    hook = DuckDbHook(duckdb_conn_id="duckdb_default")
    raw = hook.run_cli("SELECT 1 AS n;", output_format="json")
    rows = parse_json_output(raw)

See also
--------

* Connection fields, ``cli_params`` ban-list, secrets: :doc:`connections`
* File locks and ``lock_retry_attempts``: :ref:`duckdb-lock-retry`
* Operator how-to: :doc:`operators`
* Sensor how-to: :doc:`sensors`
