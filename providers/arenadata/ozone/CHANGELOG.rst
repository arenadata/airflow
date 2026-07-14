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

``apache-airflow-providers-arenadata-ozone``


Changelog
---------

1.1.0
.....

Ozone provider migration:

* Migrated the provider to the Apache Airflow 3.2.1 provider package layout.

1.0.1
.....

Ozone provider improvements:

* Replaced module-level environment-derived constants in Ozone example DAGs with DAG ``params`` /
  ``Param(...)`` definitions.
* Split ``OzoneKeySensor`` CLI timeout handling from the parent sensor timeout. The public
  ``timeout`` parameter now controls one Ozone CLI existence check and is stored internally as
  ``cli_timeout``.
* Added ``if_exists`` support for selected Ozone FS operations.
* Added ``ExistingTargetPolicy`` enum for existing-target behavior.
* Added ``OzoneFsHook.make_path`` as the preferred path creation API. ``OzoneFsHook.create_path``
  is kept as a deprecated compatibility wrapper; its ``fail_if_exists`` parameter is deprecated in
  favor of ``if_exists``.
* Added Kerberos authentication via principal and password.

1.0.0
.....

Initial release of the Apache Ozone provider.
