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

The ``ozone`` connection type is the main runtime contract for this provider.
It is used by hooks/operators/sensors/transfers for Native Ozone CLI execution
(``ozone sh`` / ``ozone fs``) and for security/runtime settings.

Provider runtime reads this connection via:
``airflow/providers/arenadata/ozone/utils/connection_schema.py``
(``OzoneConnSnapshot.from_connection(...)``).

The worker running a task must have:

* Java runtime
* Ozone CLI binary available in ``PATH``
* client config directory/files reachable from the task environment

Default Connection IDs
----------------------

* ``ozone_default`` is used by ``OzoneCliHook`` and ``OzoneFsHook`` by default.
* ``ozone_admin_default`` is used by ``OzoneAdminHook`` by default.

Connection fields
-----------------

Host (required in Ozone CLI scope)
    Ozone Manager hostname for CLI endpoint wiring.

Port (required in Ozone CLI scope)
    Ozone Manager port for CLI endpoint wiring.

Extra
    JSON dictionary with canonical provider keys. Built-in provider code reads
    only fields defined in ``OzoneConnSnapshot``.

    Additional custom keys are allowed and available to DAG developers through
    ``hook.connection_snapshot.raw_extra``.

Connection extra keys (canonical)
---------------------------------

Ozone SSL/TLS scope:

* ``ozone_security_enabled``
* ``ozone_om_https_port``
* ``ozone_ssl_keystore_location``
* ``ozone_ssl_keystore_password`` (supports ``secret://...``)
* ``ozone_ssl_keystore_type``
* ``ozone_ssl_truststore_location``
* ``ozone_ssl_truststore_password`` (supports ``secret://...``)
* ``ozone_ssl_truststore_type``

Ozone Kerberos scope:

* ``hadoop_security_authentication`` (set ``kerberos`` to enable)
* ``kerberos_principal``
* ``kerberos_keytab`` (supports ``secret://...``)
* ``krb5_conf`` (optional)
* ``ozone_conf_dir`` (recommended for all modes)

Connection Extra by scenario
----------------------------

Use one of the following templates depending on your runtime mode.

1) Plain Ozone (no SSL, no Kerberos)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

  {
    "ozone_conf_dir": "/opt/airflow/ozone-conf"
  }

2) Ozone with SSL/TLS
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

  {
    "ozone_security_enabled": "true",
    "ozone_om_https_port": "9879",
    "ozone_ssl_keystore_location": "/opt/airflow/ssl/ozone-keystore.jks",
    "ozone_ssl_keystore_password": "secret://vault/ozone/keystore_password",
    "ozone_ssl_keystore_type": "JKS",
    "ozone_ssl_truststore_location": "/opt/airflow/ssl/ozone-truststore.jks",
    "ozone_ssl_truststore_password": "secret://vault/ozone/truststore_password",
    "ozone_ssl_truststore_type": "JKS",
    "ozone_conf_dir": "/opt/airflow/ozone-conf"
  }

3) Ozone with SSL/TLS and Kerberos
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

  {
    "ozone_security_enabled": "true",
    "ozone_om_https_port": "9879",
    "ozone_ssl_keystore_location": "/opt/airflow/ssl/ozone-keystore.jks",
    "ozone_ssl_keystore_password": "secret://vault/ozone/keystore_password",
    "ozone_ssl_keystore_type": "JKS",
    "ozone_ssl_truststore_location": "/opt/airflow/ssl/ozone-truststore.jks",
    "ozone_ssl_truststore_password": "secret://vault/ozone/truststore_password",
    "ozone_ssl_truststore_type": "JKS",
    "hadoop_security_authentication": "kerberos",
    "kerberos_principal": "testuser@EXAMPLE.COM",
    "kerberos_keytab": "/opt/airflow/keytabs/testuser.keytab",
    "krb5_conf": "/opt/airflow/kerberos-config/krb5.conf",
    "ozone_conf_dir": "/opt/airflow/ozone-conf"
  }

Field-by-field explanation for SSL + Kerberos extra
----------------------------------------------------

``ozone_security_enabled``
    Enables Ozone security mode in provider runtime. Use ``"true"`` for SSL and
    SSL+Kerberos scenarios.

``ozone_om_https_port``
    HTTPS port used by Ozone OM in your cluster (for example ``9879`` in local
    demo setups). Must match actual OM SSL config.

``ozone_ssl_keystore_location`` / ``ozone_ssl_truststore_location``
    Absolute paths inside Airflow worker/container to keystore and truststore
    files used by Ozone CLI/JVM SSL layer.

``ozone_ssl_keystore_password`` / ``ozone_ssl_truststore_password``
    Passwords for the stores above. Prefer ``secret://...`` values so passwords
    are resolved from Airflow secrets backend instead of hardcoding plain text.

``ozone_ssl_keystore_type`` / ``ozone_ssl_truststore_type``
    Store format (typically ``JKS``). Must match how files were created.

``hadoop_security_authentication``
    Kerberos switch for provider runtime. Set exactly to ``"kerberos"`` to
    enable Kerberos env wiring and ``kinit`` flow.

``kerberos_principal``
    Principal used by tasks before CLI calls (for example
    ``testuser@EXAMPLE.COM``).

``kerberos_keytab``
    Absolute path to keytab file inside Airflow worker/container.
    The file must exist and be readable by the task process.

``krb5_conf``
    Optional explicit path to ``krb5.conf``. Recommended in containerized runs
    to avoid relying on image-level defaults.

``ozone_conf_dir``
    Directory with Ozone/Hadoop client configs (usually ``ozone-site.xml`` and
    ``core-site.xml``). This is one of the most important fields: provider uses
    it to set ``OZONE_CONF_DIR`` and (when needed) ``HADOOP_CONF_DIR``.

Common validation checklist for DAG developers
----------------------------------------------

* Paths in extra must be valid inside the runtime container/worker, not on your laptop.
* SSL store files and keytab must be mounted/readable for Airflow task user.
* Kerberos principal/keytab pair must be valid for your KDC realm.
* ``ozone_conf_dir`` must contain config files pointing to real OM/SCM endpoints.

Why ``ozone_conf_dir`` is important
-----------------------------------

``ozone_conf_dir`` points to the directory inside the worker/container where
Ozone/Hadoop client configs are available (usually ``ozone-site.xml`` and
``core-site.xml``).

The provider uses this value to prepare CLI runtime environment
(``OZONE_CONF_DIR`` and, when needed, ``HADOOP_CONF_DIR``), so ``ozone sh`` /
``ozone fs`` can resolve OM/SCM addresses and security policy.

If ``ozone_conf_dir`` is missing or points to the wrong path, commands may fail
even with correct connection host/port (for example: unresolved endpoints,
auth/security initialization failures, or generic Ozone CLI connection errors).

HDFS DistCp transfer scope (when ``hdfs_conn_id`` is provided)
---------------------------------------------------------------

``HdfsToOzoneOperator`` reads HDFS-specific security keys from the connection referenced by
``hdfs_conn_id`` and applies them only to DistCp subprocess environment.

HDFS SSL keys:

* ``hdfs_ssl_enabled``
* ``dfs_encrypt_data_transfer``
* ``hdfs_ssl_keystore_location``
* ``hdfs_ssl_keystore_password`` (supports ``secret://...``)
* ``hdfs_ssl_truststore_location``
* ``hdfs_ssl_truststore_password`` (supports ``secret://...``)

HDFS Kerberos keys:

* ``hdfs_kerberos_enabled``
* ``hdfs_kerberos_principal``
* ``hdfs_kerberos_keytab`` (supports ``secret://...``)
* optional ``krb5_conf`` for explicit Kerberos config path

Recommended runtime notes for DAG developers
--------------------------------------------

* Set ``ozone_conf_dir`` explicitly in connection extra for plain/SSL/Kerberos modes.
* Keep all security-sensitive values in Airflow Connection/Secrets backend,
  not in DAG source code.
* If you write custom operators/sensors/transfers, use
  ``hook.connection_snapshot`` for typed provider keys.
* For custom non-provider keys, read
  ``hook.connection_snapshot.raw_extra``.
* Do not rely on key aliases: only canonical key names are supported.

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

Provider defaults are defined in hook and connection modules:

* ``RETRY_ATTEMPTS = 3``
* ``FAST_TIMEOUT_SECONDS = 5 * 60``
* ``SLOW_TIMEOUT_SECONDS = 60 * 60``
* ``KINIT_TIMEOUT_SECONDS = 5 * 60``
* ``MAX_CONTENT_SIZE_BYTES = 1024 * 1024 * 1024`` (1 GiB)

These constants are exported from:

* ``airflow/providers/arenadata/ozone/hooks/ozone.py``
  (``RETRY_ATTEMPTS``, ``FAST_TIMEOUT_SECONDS``, ``SLOW_TIMEOUT_SECONDS``)
* ``airflow/providers/arenadata/ozone/utils/connection_schema.py``
  (``KINIT_TIMEOUT_SECONDS``, ``MAX_CONTENT_SIZE_BYTES`` and connection contract constants)

Upload/download size limit
--------------------------

The provider supports a connection-level limit for payload/file transfers:

* ``max_content_size_bytes`` (default: ``1073741824`` = 1 GiB)

This value is read from ``OzoneConnSnapshot`` and is used by:

* ``OzoneUploadContentOperator``
* ``OzoneUploadFileOperator``
* ``OzoneDownloadFileOperator`` (via ``OzoneFsHook.get_key_property(...)[\"data_size\"]``)

Each of these operators also accepts an optional ``max_content_size_bytes`` parameter
to override the connection-level value for that task.

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

* ``ozone``: runs a minimal CLI command against Ozone Manager
  and returns Airflow-compatible status/message.
