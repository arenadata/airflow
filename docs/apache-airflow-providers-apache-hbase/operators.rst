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



Apache HBase Operators
======================

`Apache HBase <https://hbase.apache.org/>`__ is a distributed, scalable, big data store built on Apache Hadoop. It provides random, real-time read/write access to your big data and is designed to host very large tables with billions of rows and millions of columns.

Prerequisite
------------

To use operators, you must configure an :doc:`HBase Connection <connections/hbase>`.

.. _howto/operator:HBaseCreateTableOperator:

Creating a Table
^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.arenadata.hbase.operators.hbase.HBaseCreateTableOperator` operator is used to create a new table in HBase.

Use the ``table_name`` parameter to specify the table name and ``families`` parameter to define the column families for the table.
The ``if_exists`` parameter controls behavior when the table already exists (default: ``ignore``).

.. code-block:: python

    create_table = HBaseCreateTableOperator(
        task_id="create_table",
        table_name="my_table",
        families={"cf1": {}, "cf2": {}},
        if_exists="ignore",
        hbase_conn_id="hbase_default",
    )

.. _howto/operator:HBasePutOperator:

Inserting Data
^^^^^^^^^^^^^^

The :class:`~airflow.providers.arenadata.hbase.operators.hbase.HBasePutOperator` operator is used to insert a single row into an HBase table.

Use the ``table_name`` parameter to specify the table, ``row_key`` for the row identifier, and ``data`` for the column values.

.. code-block:: python

    put_row = HBasePutOperator(
        task_id="put_row",
        table_name="my_table",
        row_key="row1",
        data={"cf1:col1": "value1", "cf1:col2": "value2"},
        hbase_conn_id="hbase_default",
    )

.. _howto/operator:HBaseBatchPutOperator:

Batch Insert Operations
^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.arenadata.hbase.operators.hbase.HBaseBatchPutOperator` operator is used to insert multiple rows into an HBase table in a single batch operation.

Use the ``table_name`` parameter to specify the table and ``rows`` parameter to provide a list of row data.
Configure ``batch_size`` (default: 200) to control the number of rows per Thrift request and ``max_workers`` (default: 4) to control parallelism.

.. code-block:: python

    batch_put = HBaseBatchPutOperator(
        task_id="batch_put",
        table_name="my_table",
        rows=[
            {"row_key": "row1", "cf1:col1": "value1"},
            {"row_key": "row2", "cf1:col1": "value2"},
        ],
        batch_size=200,
        max_workers=4,
        hbase_conn_id="hbase_default",
    )

.. note::
    ``batch_size`` controls the maximum number of rows per Thrift request.
    ``max_workers`` controls the number of parallel threads sending requests.
    For pooled connections, ensure ``connection_pool.size`` >= ``max_workers``.

.. _howto/operator:HBaseBatchGetOperator:

Batch Retrieve Operations
^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.arenadata.hbase.operators.hbase.HBaseBatchGetOperator` operator is used to retrieve multiple rows from an HBase table in a single batch operation.

Use the ``table_name`` parameter to specify the table, ``row_keys`` parameter to provide a list of row keys to retrieve,
and optional ``columns`` to limit which columns are returned.

.. code-block:: python

    batch_get = HBaseBatchGetOperator(
        task_id="batch_get",
        table_name="my_table",
        row_keys=["row1", "row2", "row3"],
        columns=["cf1:col1", "cf1:col2"],
        hbase_conn_id="hbase_default",
    )

.. _howto/operator:HBaseScanOperator:

Scanning Tables
^^^^^^^^^^^^^^^

The :class:`~airflow.providers.arenadata.hbase.operators.hbase.HBaseScanOperator` operator is used to scan and retrieve multiple rows from an HBase table based on specified criteria.

Use the ``table_name`` parameter to specify the table, and optional parameters ``row_start``, ``row_stop``, ``columns``, and ``limit`` to control the scan operation.

.. code-block:: python

    scan_table = HBaseScanOperator(
        task_id="scan_table",
        table_name="my_table",
        row_start="row_000",
        row_stop="row_100",
        columns=["cf1:col1"],
        limit=50,
        hbase_conn_id="hbase_default",
    )

.. _howto/operator:HBaseDeleteTableOperator:

Deleting a Table
^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.arenadata.hbase.operators.hbase.HBaseDeleteTableOperator` operator is used to delete an existing table from HBase.

Use the ``table_name`` parameter to specify the table to delete.
The ``if_not_exists`` parameter controls behavior when the table does not exist (default: ``ignore``).

.. code-block:: python

    delete_table = HBaseDeleteTableOperator(
        task_id="delete_table",
        table_name="my_table",
        if_not_exists="ignore",
        hbase_conn_id="hbase_default",
    )

Backup and Restore Operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

HBase provides built-in backup and restore functionality for data protection and disaster recovery.

.. _howto/operator:HBaseBackupSetOperator:

Managing Backup Sets
""""""""""""""""""""

The :class:`~airflow.providers.arenadata.hbase.operators.hbase.HBaseBackupSetOperator` operator is used to manage backup sets containing one or more tables.

Supported actions:

- ``add``: Create a new backup set with specified tables
- ``list``: List all existing backup sets

Use the ``action`` parameter to specify the operation, ``backup_set_name`` for the backup set name, and ``tables`` parameter to list the tables (for ``add`` action).

.. code-block:: python

    # Create a backup set
    create_backup_set = HBaseBackupSetOperator(
        task_id="create_backup_set",
        action="add",
        backup_set_name="my_backup_set",
        tables=["table1", "table2"],
        hbase_conn_id="hbase_default",
    )

    # List backup sets
    list_backup_sets = HBaseBackupSetOperator(
        task_id="list_backup_sets",
        action="list",
        hbase_conn_id="hbase_default",
    )

.. _howto/operator:HBaseCreateBackupOperator:

Creating Backups
""""""""""""""""

The :class:`~airflow.providers.arenadata.hbase.operators.hbase.HBaseCreateBackupOperator` operator is used to create full or incremental backups of HBase tables.

Use the ``backup_type`` parameter to specify ``full`` or ``incremental``, ``backup_path`` for the HDFS storage location, and either ``backup_set_name`` or ``tables`` to specify what to backup.
The ``backup_type`` parameter accepts both ``BackupType`` enum values and strings (e.g., from ``dag_run.conf``).

.. code-block:: python

    # Full backup using backup set
    full_backup = HBaseCreateBackupOperator(
        task_id="full_backup",
        backup_type="full",
        backup_path="hdfs://namenode:9000/hbase/backup",
        backup_set_name="my_backup_set",
        workers=4,
        hbase_conn_id="hbase_default",
    )

    # Incremental backup with specific tables
    incremental_backup = HBaseCreateBackupOperator(
        task_id="incremental_backup",
        backup_type=BackupType.INCREMENTAL,
        backup_path="hdfs:///hbase/backup",  # Same path as FULL
        backup_set_name="my_backup_set",
        workers=1,
        hbase_conn_id="hbase_default",
    )

.. _howto/operator:HBaseRestoreOperator:

Restoring from Backup
"""""""""""""""""""""

The :class:`~airflow.providers.arenadata.hbase.operators.hbase.HBaseRestoreOperator` operator is used to restore tables from a backup.

Use the ``backup_path`` parameter for the backup location, ``backup_id`` for the specific backup to restore,
and optionally ``backup_set_name`` or ``tables`` to filter which tables to restore.
Without ``backup_set_name`` or ``tables``, all tables from the backup are restored.

.. code-block:: python

    restore_backup = HBaseRestoreOperator(
        task_id="restore_backup",
        backup_path="hdfs://namenode:9000/hbase/backup",
        backup_id="backup_1234567890123",
        tables=["test_table_backup_v2"],
        overwrite=True,
        hbase_conn_id="hbase_default",
    )

.. _howto/operator:HBaseBackupHistoryOperator:

Viewing Backup History
""""""""""""""""""""""

The :class:`~airflow.providers.arenadata.hbase.operators.hbase.HBaseBackupHistoryOperator` operator is used to retrieve backup history information.

Use the ``backup_set_name`` parameter to filter history by backup set, or ``backup_path`` to filter by backup location.

.. code-block:: python

    # Get backup history for a backup set
    backup_history = HBaseBackupHistoryOperator(
        task_id="backup_history",
        backup_set_name="my_backup_set",
        hbase_conn_id="hbase_default",
    )

Reference
^^^^^^^^^

For further information, look at `HBase documentation <https://hbase.apache.org/book.html>`_ and `HBase Backup and Restore <https://hbase.apache.org/book.html#_backup_and_restore>`_.
