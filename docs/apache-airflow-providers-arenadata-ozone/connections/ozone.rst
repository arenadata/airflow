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

The ``ozone`` connection type is used by the Ozone provider for Native CLI operations
(``ozone sh`` / ``ozone fs``) and SSL/TLS and Kerberos runtime settings.

The provider runs the ``ozone`` binary on the worker where the task executes.
Workers must have Java and Ozone CLI installed and access to Ozone client config
files (for example via ``OZONE_CONF_DIR`` or ``HADOOP_CONF_DIR``).

Default Connection IDs
----------------------

* ``ozone_default`` is used by ``OzoneCliHook`` and ``OzoneFsHook`` by default.
* ``ozone_admin_default`` is used by ``OzoneAdminHook`` by default.

Configuring the Connection
--------------------------

Host (required)
    Ozone Manager hostname.

Port (required)
    Ozone Manager port.

Extra
    JSON dictionary. The provider reads SSL and Kerberos configuration from this field.

Native CLI: SSL/TLS fields (Extra)
----------------------------------

Set ``ozone_security_enabled`` (or ``ozone.security.enabled``) to ``"true"`` and provide
keystore and truststore settings. These values are mapped to environment variables used
by Ozone CLI.

Common keys:

* ``ozone_security_enabled`` or ``ozone.security.enabled``.
* ``ozone_om_https_port``.
* ``ozone.scm.https.port``.
* ``ozone_ssl_keystore_location``.
* ``ozone_ssl_keystore_password`` (supports ``secret://...``).
* ``ozone_ssl_truststore_location``.
* ``ozone_ssl_truststore_password`` (supports ``secret://...``).

Native CLI: Kerberos fields (Extra)
-----------------------------------

Set ``hadoop_security_authentication`` (or ``hadoop.security.authentication``) to ``"kerberos"``
and provide principal and keytab. The provider configures Kerberos environment variables and
runs ``kinit`` before CLI commands.

Common keys:

* ``hadoop_security_authentication`` or ``hadoop.security.authentication``.
* ``kerberos_principal``.
* ``kerberos_keytab`` (supports ``secret://...``).
* ``kerberos_realm``.
* ``krb5_conf``.
* ``ozone_conf_dir`` or ``hadoop_conf_dir``.

When Kerberos is enabled, set ``ozone_conf_dir`` or ``hadoop_conf_dir`` explicitly
in the connection (or provide ``OZONE_CONF_DIR`` / ``HADOOP_CONF_DIR`` in the worker
environment). The provider no longer injects a default config directory.

Retry and timeout tuning
------------------------

Process execution helpers are centralized in
``airflow/providers/arenadata/ozone/utils/cli_runner.py``.

Provider defaults are defined in the CLI hook module:

* ``RETRY_ATTEMPTS = 3``
* ``FAST_TIMEOUT_SECONDS = 5 * 60``
* ``SLOW_TIMEOUT_SECONDS = 60 * 60``

Operators, sensors, and transfers pass ``timeout`` and ``retry_attempts``
to hook methods. Use ``FAST_TIMEOUT_SECONDS`` for quick CLI calls and
``SLOW_TIMEOUT_SECONDS`` for long-running operations.

Ozone S3 Gateway connection
---------------------------

S3 Gateway operations use the dedicated ``ozone_s3`` connection type.
The provider builds a boto3 S3 client directly from this connection
via ``airflow/providers/arenadata/ozone/utils/s3_client.py`` (``OzoneS3Client``).
S3 error classification and user-facing error translation are centralized in
``airflow/providers/arenadata/ozone/utils/errors.py`` (``OzoneS3Errors``).

Default S3 connection ID:

* ``ozone_s3_default`` (type ``ozone_s3``)

Connection fields:

* **Login**: optional access key ID (supports ``secret://...``).
* **Password**: optional secret access key (supports ``secret://...``).
  If login/password are omitted, boto3 default credential resolution chain is used.
* **Extra**:

  * ``endpoint_url``: for example ``http://s3g:9878`` or ``https://s3g:9879``.
  * ``verify``: ``false`` (development only), ``true`` (default), or path to CA bundle.
  * ``region_name``: optional; if omitted, boto3 default region resolution chain is used.

Testing connections in Airflow UI
---------------------------------

Both connection types implement ``test_connection()``:

* ``ozone``: runs a minimal CLI command against Ozone Manager.
* ``ozone_s3``: runs a minimal S3 API call against configured endpoint.
