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

Changelog

1.0.0
.....

Initial version of the provider.

Operators
~~~~~~~~~

* ``HBaseCreateTableOperator`` - Operator for creating HBase tables with column families
* ``HBaseDeleteTableOperator`` - Operator for deleting HBase tables
* ``HBasePutOperator`` - Operator for inserting single rows into HBase tables
* ``HBaseBatchPutOperator`` - Operator for batch inserting multiple rows with parallel processing
* ``HBaseBatchGetOperator`` - Operator for batch retrieving multiple rows by keys
* ``HBaseScanOperator`` - Operator for scanning HBase tables with row range and column filters
* ``HBaseCreateBackupOperator`` - Operator for creating full or incremental HBase backups
* ``HBaseRestoreOperator`` - Operator for restoring HBase tables from backups
* ``HBaseBackupSetOperator`` - Operator for managing HBase backup sets
* ``HBaseBackupHistoryOperator`` - Operator for querying backup history

Hooks
~~~~~

* ``HBaseThriftHook`` - Hook for connecting to Apache HBase via Thrift2 protocol
* ``HBaseCLIHook`` - Hook for HBase CLI operations (backup/restore)

Sensors
~~~~~~~

* ``HBaseTableSensor`` - Sensor for checking HBase table existence
* ``HBaseRowSensor`` - Sensor for checking specific row existence

Datasets
~~~~~~~~

* ``hbase_table_dataset`` - Dataset support for HBase tables in Airflow lineage

Connection Features
~~~~~~~~~~~~~~~~~~~

* **Thrift2 Protocol** - Native Thrift2 client implementation without external dependencies
* **Connection Pooling** - Built-in connection pool for high-throughput operations with configurable pool size
* **Kerberos Authentication** - GSSAPI authentication with keytab support
* **Retry Logic** - Configurable retry mechanism with exponential backoff
* **Multiple Strategies** - ThriftStrategy for single connections, PooledThrift2Strategy for pooled connections

Performance Features
~~~~~~~~~~~~~~~~~~~~

* **Batch Operations** - Optimized batch put/get/delete operations with configurable batch sizes (default: 200)
* **Parallel Processing** - Multi-threaded batch operations with configurable worker count (default: 4)
* **Connection Reuse** - Global connection pool storage for efficient resource management
* **Chunking Support** - Automatic chunking for large datasets to prevent memory issues
