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

Native CLI prerequisites
========================

Native CLI operations (Admin/Filesystem/Snapshot) execute the ``ozone`` binary on the worker.

Checklist
---------

* Java (JDK 11+)
* ``ozone`` CLI available in ``PATH``
* Ozone/Hadoop client configuration available to the CLI:

  * ``OZONE_CONF_DIR`` or ``HADOOP_CONF_DIR`` points to a directory containing at least:
    ``core-site.xml`` and ``ozone-site.xml``.
  * Alternatively, set connection Extra ``ozone_scm_address`` (and optionally
    ``ozone_recon_address``); the hook then builds a minimal config and sets
    ``OZONE_CONF_DIR`` automatically (see :ref:`howto/connection:ozone`).

Kerberos note
-------------

When Kerberos is enabled, the provider adds ``--config <conf_dir>`` to ``ozone`` commands (best-effort),
because config discovery can be fragile in some environments.
