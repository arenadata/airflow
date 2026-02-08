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

Kerberos for Native CLI
=======================

To enable Kerberos for Native CLI operations, configure the ``ozone`` connection Extra.
The provider performs ``kinit`` with keytab automatically (best-effort) before executing CLI commands.

Minimal example
---------------

.. code-block:: json

   {
     "hadoop_security_authentication": "kerberos",
     "kerberos_principal": "user@REALM.COM",
     "kerberos_keytab": "secret://vault/ozone/user-keytab",
     "krb5_conf": "/etc/krb5.conf",
     "ozone_conf_dir": "/opt/airflow/ozone-conf"
   }

Notes
-----

* ``kerberos_keytab`` supports ``secret://...``. The secret value is expected to resolve to a path usable by ``kinit``.
* When Kerberos is enabled, the provider may add ``--config <conf_dir>`` to ``ozone`` commands.
