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


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-arenadata-duckdb``

Changelog
---------

1.0.0
.....

Initial version of the provider.

Features
~~~~~~~~

* DuckDB hook, operator, and SQL sensor via CLI subprocess
* Example DAGs for basic pipeline, SQL files, and sensors
* Typed errors, Popen process control (``on_kill`` / timeout killpg), and
  opt-in ``lock_retry_attempts`` (default ``0``) with clear lock-conflict messages
* Managed INFO log truncation via ``log_output_limit`` (full output on DEBUG)

Behavior
~~~~~~~~

* Sensor: invalid JSON output fails with ``DuckDbOutputError`` (no silent wait)
* Sensor: ``fail_on_empty`` raises ``AirflowFailException`` (no task retries for
  empty results)
* Success-path stderr is logged at INFO (not WARNING)

Notes
~~~~~

* Requires Apache Airflow 3.2.1 or later
* Uses ``version_compat.py`` as an Airflow 3-only SDK shim (no Airflow 2.x support)
* Ban-list for ``cli_params`` and soft path preflight are documented in
  ``docs/connections.rst``
* ``duckdb_binary`` is exposed as a dedicated Connection UI field (declared via
  ``conn-fields`` in ``provider.yaml``) instead of a raw ``extra`` JSON key
* Empty, omitted, or JSON ``null`` ``duckdb_binary`` resolves to
  ``/usr/bin/duckdb`` at hook runtime (Connection extra is not rewritten)
