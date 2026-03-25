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



Apache HBase Sensors
====================

`Apache HBase <https://hbase.apache.org/>`__ sensors allow you to monitor the state of HBase tables and data.

Prerequisite
------------

To use sensors, you must configure an :doc:`HBase Connection <connections/hbase>`.

.. _howto/sensor:HBaseTableSensor:

Waiting for a Table to Exist
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.arenadata.hbase.sensors.hbase.HBaseTableSensor` sensor is used to check for the existence of a table in HBase.

Use the ``table_name`` parameter to specify the table to monitor.

.. code-block:: python

    from airflow.providers.arenadata.hbase.sensors.hbase import HBaseTableSensor

    wait_for_table = HBaseTableSensor(
        task_id="wait_for_table",
        table_name="my_table",
        hbase_conn_id="hbase_default",
        timeout=300,
        poke_interval=30,
    )

.. _howto/sensor:HBaseRowSensor:

Waiting for a Row to Exist
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.arenadata.hbase.sensors.hbase.HBaseRowSensor` sensor is used to check for the existence of a specific row in an HBase table.

Use the ``table_name`` parameter to specify the table and ``row_key`` parameter to specify the row to monitor.

.. code-block:: python

    from airflow.providers.arenadata.hbase.sensors.hbase import HBaseRowSensor

    wait_for_row = HBaseRowSensor(
        task_id="wait_for_row",
        table_name="my_table",
        row_key="row_123",
        hbase_conn_id="hbase_default",
        timeout=600,
        poke_interval=60,
    )

Reference
^^^^^^^^^

For further information, look at `HBase documentation <https://hbase.apache.org/book.html>`_.