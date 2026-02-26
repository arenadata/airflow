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

Native CLI operations (Admin/Filesystem/Snapshot) execute the ``ozone`` binary **on the worker
where the task runs**.

Checklist
---------

* Java (JDK 11+) installed in the worker (or Airflow image).
* ``ozone`` CLI available in ``PATH`` on every worker that will run Ozone tasks.
* Ozone/Hadoop client configuration available to the CLI:

  * ``OZONE_CONF_DIR`` or ``HADOOP_CONF_DIR`` points to a directory containing at least
    ``core-site.xml`` and ``ozone-site.xml``; **or**
  * you pass addresses via the Ozone connection Extra (see :ref:`howto/connection:ozone`)
    and the provider configures the environment variables for the CLI.

The provider package itself **does not install** Ozone CLI. The ``ozone`` binary and client
configs must be present on all Airflow workers. In this repository, the helper script ``files/run_af.sh`` is provided to build a CI
image and install Ozone CLI into the Airflow container for local development only.

Kerberos note
-------------

When Kerberos is enabled, the provider configures all necessary environment variables
(``KRB5_CONFIG``, ``HADOOP_SECURITY_AUTHENTICATION``, etc.) before executing CLI commands.
