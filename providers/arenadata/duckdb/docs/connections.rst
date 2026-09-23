
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
   * - DuckDB binary
     - Dedicated Connection field (stored in ``extra``); path to the DuckDB
       binary or ADO wrapper. Empty, omitted, or JSON ``null`` uses
       ``/usr/bin/duckdb``. The Connection extra is not rewritten; the hook
       resolves the path at runtime.
   * - Extra ``timeout``
     - Subprocess timeout **per CLI attempt** in seconds (default: 300)
   * - Extra ``readonly``
     - Open the database in read-only mode (default: false). Soft preflight
       checks that the file exists and is readable; parent writability is not
       required (read-only mounts are supported).
   * - Extra ``cli_params``
     - Additional CLI parameters: a shell string or a JSON array of strings
       (see ban-list below)
   * - Extra ``lock_retry_attempts``
     - Opt-in file-lock retries: number of CLI launches when ``> 0``
       (default: ``0`` = off). See :ref:`duckdb-lock-retry`.

Example Extra JSON
------------------

``duckdb_binary`` is a dedicated Connection form field (stored in ``extra``).
Do not put it in the Extra JSON textarea.

.. code-block:: json

   {
     "timeout": 300,
     "readonly": false,
     "cli_params": "",
     "lock_retry_attempts": 0
   }

.. _duckdb-lock-retry:

Lock conflicts and ``lock_retry_attempts``
------------------------------------------

DuckDB takes an exclusive lock on the database file when the CLI opens it.
If another process already holds the lock, the CLI fails with a clear provider
error that includes ``duckdb_conn_id`` and ``db_path``.

Resolution order for ``lock_retry_attempts``:

1. ``DuckDbOperator(..., lock_retry_attempts=...)`` / ``DuckDbHook(..., lock_retry_attempts=...)``
2. Connection Extra ``lock_retry_attempts``
3. Default ``0`` (off)

When the value is ``0``, the task fails immediately with the clear lock message
(no in-hook retries). When ``> 0``, it is the **number of CLI launches**; the
hook sleeps with backoff ``1, 2, 4, 8, 16`` seconds before attempts 2..N
(then repeats ``16``).

**Wall-clock:** each attempt uses its own ``timeout``. With retries enabled,
total time can approach ``attempts × timeout + sum(backoff)``. Set task
``execution_timeout`` accordingly.

**Prefer architecture over retries:** use an Airflow pool with one slot for
the shared ``.duckdb`` file, or use separate database files per writer.
Retries only help short overlaps; a long-running holder can keep the lock for
minutes.

**ATTACH caveat:** lock-retry assumes the CLI opens the connection ``host``
database. Scripts that ``ATTACH`` another file mid-run can hit the same lock
markers after statements already ran - set ``lock_retry_attempts=0`` for those
workloads.

``DuckDbSqlSensor`` always creates the hook with ``lock_retry_attempts=0``.
Waiting for the lock is done via ``poke_interval`` / reschedule, not in-poke
CLI retries. ``lock_retry_attempts`` and ``log_output_limit`` are **not** in
``template_fields``.

Logging levels
--------------

.. list-table::
   :header-rows: 1

   * - What
     - Level
   * - Full CLI command (after secret registration)
     - DEBUG (``redact``)
   * - Run summary (``duckdb_conn_id``, ``db_path``, return code, duration)
     - INFO
   * - stdout
     - INFO truncated to ``log_output_limit`` (hook/operator/sensor kwarg;
       default ``2000``); full text on DEBUG. Operator XCom still returns full
       stdout. **Not** a Connection Extra.
   * - stderr on success
     - INFO (truncated like stdout; full on DEBUG)
   * - stderr on failure
     - ERROR (+ text in the exception; exception text keeps the tail)
   * - Lock conflict retry attempt
     - WARNING with attempt number and **raw** stderr (not redacted)

``cli_params`` ban-list and secrets
-----------------------------------

SQL submission (``-c`` / ``-f`` / ``-s``), output format (``-json`` / ``-csv``),
``-readonly``, ``-bail``, and ``-no-stdin`` are managed by the provider.
Putting them in ``cli_params`` raises a configuration error. Use
``DuckDbOperator(output_format=...)`` for json/csv.

Hard-banned tokens (normalized after ``shlex.split`` or JSON-array tokens, verified against
DuckDB CLI ``1.5.x`` ``duckdb -help``; ``-s`` is an alias of ``-c``):
``-c``, ``-s``, ``-f``, ``-cmd``, ``-init``, ``-json``, ``-csv``, ``-readonly``,
``-bail``, ``-no-stdin``.

``cli_params`` may be a shell string (``"--threads 4"``, as in the Extra example
above) or a JSON array of string tokens (``["--threads", "4"]``). Non-string
array elements raise a configuration error.

Store credentials in Connection Extra keys that Airflow masks
(``*password*``, ``*secret*``, ``*token*``, ``*access_key*``, …), not inline in
SQL and not in operator/hook ``parameters``. Bound ``parameters`` are inlined as
SQL literals; on CLI error DuckDB typically echoes the statement in stderr, so
the value appears in task logs. Sensitive flag values in ``cli_params`` (for
example ``--password`` / ``--token=...``) are registered with ``mask_secret`` so
they appear as ``***`` in logs.

SQL files (operator / sensor)
-----------------------------

When ``sql`` ends with ``.sql`` (case-sensitive; ``load.SQL`` is **not** treated
as a file), Airflow 3 loads it from the DAG bundle (``dag.folder`` and/or
``template_searchpath``) so both the DAG processor and the worker can read it.

If the file is missing, ``resolve_template_files`` may log
``Failed to resolve template field 'sql'`` and leave the path string; Jinja
render then raises ``jinja2.TemplateNotFound``. Prefer a lowercase ``.sql``
extension and keep the file inside the bundle.

Process lifecycle note
----------------------

The hook runs the CLI with ``start_new_session=True`` so a process-group kill
can stop an ADO wrapper and its DuckDB child on task kill / timeout. Trade-off:
if the worker is ``SIGKILL``'d (OOM / ``docker kill``), ``on_kill`` is not
called and an orphaned DuckDB process may keep the file lock until reaped.
