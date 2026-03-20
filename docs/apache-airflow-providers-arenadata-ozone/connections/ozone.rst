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
files (via ``OZONE_CONF_DIR``).

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
    For this connection type, use Ozone-native keys (``ozone_*``, ``hadoop_security_authentication``,
    ``kerberos_*``, ``krb5_conf``, ``ozone_conf_dir``). HDFS transfer-specific keys (``hdfs_*``)
    belong to HDFS transfer connection scope and are not consumed by Ozone CLI hook runtime.

Native CLI: SSL/TLS fields (Extra)
----------------------------------

Set ``ozone_security_enabled`` to ``"true"`` and provide
keystore and truststore settings. These values are mapped to environment variables used
by Ozone CLI.

Common keys:

* ``ozone_security_enabled``.
* ``ozone_om_https_port``.
* ``ozone_ssl_keystore_location``.
* ``ozone_ssl_keystore_password`` (supports ``secret://...``).
* ``ozone_ssl_truststore_location``.
* ``ozone_ssl_truststore_password`` (supports ``secret://...``).

Native CLI: Kerberos fields (Extra)
-----------------------------------

Set ``hadoop_security_authentication`` to ``"kerberos"``
and provide principal and keytab. The provider configures Kerberos environment variables and
runs ``kinit`` before CLI commands.

Common keys:

* ``hadoop_security_authentication``.
* ``kerberos_principal``.
* ``kerberos_keytab`` (supports ``secret://...``).
* ``krb5_conf``.
* ``ozone_conf_dir``.

Set ``ozone_conf_dir`` explicitly in the connection for all modes (plain/SSL/Kerberos).
The provider derives ``HADOOP_CONF_DIR`` internally from ``ozone_conf_dir`` for subprocess execution.

Recommended Kerberos extra (explicit paths):

.. code-block:: json

  {
    "ozone_security_enabled": "true",
    "hadoop_security_authentication": "kerberos",
    "kerberos_principal": "testuser@EXAMPLE.COM",
    "kerberos_keytab": "/opt/airflow/keytabs/testuser.keytab",
    "krb5_conf": "/opt/airflow/kerberos-config/krb5.conf",
    "ozone_conf_dir": "/opt/airflow/ozone-conf"
  }

Retry and timeout tuning
------------------------

Process execution helpers are centralized in
``airflow/providers/arenadata/ozone/utils/cli_runner.py``.

Security runtime helpers are centralized in
``airflow/providers/arenadata/ozone/utils/security.py``.

Provider defaults are defined in provider params:

* ``RETRY_ATTEMPTS = 3``
* ``FAST_TIMEOUT_SECONDS = 5 * 60``
* ``SLOW_TIMEOUT_SECONDS = 60 * 60``

These constants are exported from:
``airflow/providers/arenadata/ozone/utils/params.py``.

Operators, sensors, and transfers pass ``timeout`` and ``retry_attempts``
to hook methods. Use ``FAST_TIMEOUT_SECONDS`` for quick CLI calls and
``SLOW_TIMEOUT_SECONDS`` for long-running operations.

Hook API coverage
-----------------

The main Native CLI hook module is now fully located in
``airflow/providers/arenadata/ozone/hooks/ozone.py``.

Key ``OzoneFsHook`` methods include:

* ``list_keys``, ``list_paths``, ``list_wildcard_matches``
* ``path_exists``, ``delete_key``, ``delete_path``
* ``create_path``, ``create_key``
* ``move``, ``copy_path``
* ``upload_key``, ``download_key``, ``set_key_property``

Key ``OzoneAdminHook`` methods include:

* ``create_volume``, ``delete_volume``
* ``create_bucket``, ``delete_bucket``
* ``set_quota``

Key ``OzoneAdminExtraHook`` methods include:

* ``set_bucket_replication_config``, ``create_bucket_link``
* ``get_container_report``

Testing connections in Airflow UI
---------------------------------

The ``ozone`` connection type implements ``test_connection()``:

* ``ozone``: runs a minimal CLI command against Ozone Manager.
