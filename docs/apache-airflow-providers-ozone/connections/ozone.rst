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

The provider runs the ``ozone`` binary **locally on the worker** where the task executes. The
worker (or Airflow image) must have Java and the Ozone CLI installed and have access to the
Ozone client configuration files (for example via ``OZONE_CONF_DIR`` / ``HADOOP_CONF_DIR``).

The provider does not manage installation of the CLI itself; you must provision ``ozone`` and
its client configuration on all Airflow workers before running Ozone DAGs.

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
----------------------------------

Set ``ozone_security_enabled`` (or ``ozone.security.enabled``) to ``"true"`` and provide
keystore/truststore details. These values are mapped to environment variables used by the
Ozone CLI on the worker.

Common keys:

* ``ozone_security_enabled`` / ``ozone.security.enabled``: ``"true"`` to enable SSL/TLS.
* ``ozone_om_https_port``: Ozone Manager HTTPS port (e.g. ``"9879"``).
* ``ozone.scm.https.port``: SCM HTTPS port (e.g. ``"9861"``).
* ``ozone_ssl_keystore_location``: path to keystore.
* ``ozone_ssl_keystore_password``: keystore password (supports ``secret://...``).
* ``ozone_ssl_truststore_location``: path to truststore.
* ``ozone_ssl_truststore_password``: truststore password (supports ``secret://...``).

Native CLI: Kerberos fields (Extra)
-----------------------------------

Set ``hadoop_security_authentication`` (or ``hadoop.security.authentication``) to ``"kerberos"``
and provide principal/keytab. The provider configures Kerberos-related environment variables and
performs ``kinit`` on the worker before executing CLI commands.

Common keys:

* ``hadoop_security_authentication`` / ``hadoop.security.authentication``: ``"kerberos"``.
* ``kerberos_principal``: e.g. ``"user@REALM.COM"``.
* ``kerberos_keytab``: keytab path (supports ``secret://...``).
* ``kerberos_realm``: e.g. ``"REALM.COM"``.
* ``krb5_conf``: optional path to ``krb5.conf``.
* ``ozone_conf_dir`` / ``hadoop_conf_dir``: optional config directory for the CLI; if not set,
  the provider can still work if ``OZONE_CONF_DIR`` / ``HADOOP_CONF_DIR`` are provided via the
  environment.

Ozone S3 Gateway connection
---------------------------

S3 Gateway operations use the dedicated ``ozone_s3`` connection type. The provider does **not**
depend on the Amazon S3 hook; instead, it builds a boto3 S3 client directly from the connection
via the internal ``hooks/_s3_client.py`` layer.

Default Connection ID used by this provider:

* ``ozone_s3_default`` (connection type ``ozone_s3``)

Configure the connection fields as:

* **Login**: access key ID.
* **Password**: secret access key (can be a ``secret://`` reference).
* **Extra**:

  * ``endpoint_url``: e.g. ``"http://s3g:9878"`` or ``"https://s3g:9879"``.
  * ``verify``: ``false`` (development only), ``true`` (default), or a path to a CA bundle.
