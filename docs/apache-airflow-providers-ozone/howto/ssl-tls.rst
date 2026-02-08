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

SSL/TLS for Native CLI
======================

To enable SSL/TLS for Native CLI operations, configure the ``ozone`` connection Extra.

Minimal example
---------------

.. code-block:: json

   {
     "ozone_security_enabled": "true",
     "ozone_om_https_port": "9879",
     "ozone.scm.https.port": "9861",
     "ozone_ssl_keystore_location": "/path/to/keystore.jks",
     "ozone_ssl_keystore_password": "secret://vault/ozone/keystore-password",
     "ozone_ssl_truststore_location": "/path/to/truststore.jks",
     "ozone_ssl_truststore_password": "secret://vault/ozone/truststore-password"
   }

Notes
-----

* Password fields support Secrets Backend references via ``secret://...``.
* The provider only passes environment variables to the subprocess; it does not modify XML config files.
