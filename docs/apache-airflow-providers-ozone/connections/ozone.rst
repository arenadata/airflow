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


.. _howto/connection:ozone:

Ozone Connection
================

The ``ozone`` connection type is used by the Ozone provider for **Native CLI** operations
(``ozone sh`` / ``ozone fs``) and for configuring **SSL/TLS** and **Kerberos** runtime settings.

.. note::
   The Native CLI hooks execute the ``ozone`` binary on the worker. Make sure Java and the Ozone CLI are
   available in ``PATH`` and that the Ozone/Hadoop configuration is available to the CLI (for example via
   ``OZONE_CONF_DIR`` / ``HADOOP_CONF_DIR``).

Default Connection IDs
----------------------

* ``ozone_default`` is used by ``OzoneHook`` / ``OzoneFsHook`` by default.
* ``ozone_admin_default`` is used by ``OzoneAdminHook`` by default.

Configuring the Connection
--------------------------

Host (optional)
    Ozone Manager hostname (informational).

Port (optional)
    Ozone Manager port (informational).

Extra
    JSON dictionary. The provider reads SSL/Kerberos configuration from here.

Native CLI: SSL/TLS fields (Extra)
---------------------------------

Set ``ozone_security_enabled`` (or ``ozone.security.enabled``) to ``"true"`` and provide keystore/truststore details.

Common keys:

* ``ozone_security_enabled`` / ``ozone.security.enabled``: ``"true"`` to enable SSL/TLS flags.
* ``ozone_om_https_port``: Ozone Manager HTTPS port (e.g. ``"9879"``).
* ``ozone.scm.https.port``: SCM HTTPS port (e.g. ``"9861"``).
* ``ozone_ssl_keystore_location``: path to keystore.
* ``ozone_ssl_keystore_password``: keystore password (supports ``secret://...``).
* ``ozone_ssl_truststore_location``: path to truststore.
* ``ozone_ssl_truststore_password``: truststore password (supports ``secret://...``).

Native CLI: Kerberos fields (Extra)
-----------------------------------

Set ``hadoop_security_authentication`` (or ``hadoop.security.authentication``) to ``"kerberos"`` and provide principal/keytab.
The provider performs ``kinit`` automatically (best-effort) before executing CLI commands.

Common keys:

* ``hadoop_security_authentication`` / ``hadoop.security.authentication``: ``"kerberos"``.
* ``kerberos_principal``: e.g. ``"user@REALM.COM"``.
* ``kerberos_keytab``: keytab path (supports ``secret://...``).
* ``kerberos_realm``: e.g. ``"REALM.COM"``.
* ``krb5_conf``: optional path to ``krb5.conf``.
* ``ozone_conf_dir`` / ``hadoop_conf_dir``: optional config directory (defaults to ``/opt/airflow/ozone-conf`` when Kerberos is enabled).

Client config from Extra (2–3 parameters, no config file on disk)
----------------------------------------------------------------

If the worker has no Ozone config (e.g. empty ``ozone-site.xml``), the CLI can hang. Pass only
the needed addresses in connection Extra; the hook builds a minimal ``ozone-site.xml`` and sets
``OZONE_CONF_DIR`` automatically:

* ``ozone_scm_address``: SCM client address (e.g. ``"scm"`` or ``"scm:9860"``). Required.
  OM is taken from connection Host/Port.

* ``ozone_recon_address``: optional Recon address (e.g. ``"recon:9891"``).

Example (only SCM):

.. code-block:: json

   { "ozone_scm_address": "scm" }

With Recon:

.. code-block:: json

   { "ozone_scm_address": "scm", "ozone_recon_address": "recon:9891" }

Example (Extra)
---------------

.. code-block:: json

   {
     "ozone_security_enabled": "true",
     "ozone_om_https_port": "9879",
     "ozone.scm.https.port": "9861",
     "ozone_ssl_keystore_location": "/opt/airflow/ozone-conf/keystore.jks",
     "ozone_ssl_keystore_password": "secret://vault/ozone/keystore-password",
     "ozone_ssl_truststore_location": "/opt/airflow/ozone-conf/truststore.jks",
     "ozone_ssl_truststore_password": "secret://vault/ozone/truststore-password",
     "hadoop_security_authentication": "kerberos",
     "kerberos_principal": "testuser@EXAMPLE.COM",
     "kerberos_keytab": "secret://vault/ozone/testuser-keytab",
     "krb5_conf": "/etc/krb5.conf",
     "ozone_scm_address": "scm",
     "ozone_recon_address": "recon:9891"
   }

Ozone S3 Gateway connection
---------------------------

S3 Gateway operations use the **Amazon provider** and an **AWS connection** (not the ``ozone`` connection type).

Default Connection ID used by this provider:

* ``ozone_s3_default`` (AWS connection)

Configure the AWS connection Extra with:

* ``endpoint_url``: e.g. ``"http://s3g:9878"`` or ``"https://s3g:9879"``.
* ``verify``: ``false`` (development only) or a path to a CA bundle.
