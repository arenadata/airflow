#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import os

import pytest

from airflow.models import Connection
from airflow.providers.arenadata.hbase.hooks.hbase import HBaseThriftHook
from airflow.providers.arenadata.hbase.sensors.hbase import HBaseRowSensor, HBaseTableSensor
from airflow.utils import db

TABLE_NAME = "integration_test_sensors"
CONN_ID = "hbase_test_sensors"
HBASE_HOST = os.environ.get(
    "HBASE_HOST",
    "hbase" if os.environ.get("INTEGRATION_HBASE") == "true" else "localhost",
)


@pytest.mark.integration("hbase")
class TestHBaseTableSensorIntegration:

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id=CONN_ID,
                conn_type="hbase",
                host=HBASE_HOST,
                port=9090,
            )
        )
        self.hook = HBaseThriftHook(hbase_conn_id=CONN_ID)
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)

    def teardown_method(self):
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.close()

    def test_poke_table_exists(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})

        sensor = HBaseTableSensor(
            task_id="test_table_exists",
            table_name=TABLE_NAME,
            hbase_conn_id=CONN_ID,
        )
        assert sensor.poke({}) is True

    def test_poke_table_not_exists(self):
        sensor = HBaseTableSensor(
            task_id="test_table_not_exists",
            table_name="nonexistent_sensor_table",
            hbase_conn_id=CONN_ID,
        )
        assert sensor.poke({}) is False

    def test_poke_becomes_true_after_create(self):
        sensor = HBaseTableSensor(
            task_id="test_table_appears",
            table_name=TABLE_NAME,
            hbase_conn_id=CONN_ID,
        )
        assert sensor.poke({}) is False

        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        assert sensor.poke({}) is True


@pytest.mark.integration("hbase")
class TestHBaseRowSensorIntegration:

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id=CONN_ID,
                conn_type="hbase",
                host=HBASE_HOST,
                port=9090,
            )
        )
        self.hook = HBaseThriftHook(hbase_conn_id=CONN_ID)
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.create_table(TABLE_NAME, {"cf1": {}})

    def teardown_method(self):
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.close()

    def test_poke_row_exists(self):
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "value1"})

        sensor = HBaseRowSensor(
            task_id="test_row_exists",
            table_name=TABLE_NAME,
            row_key="row1",
            hbase_conn_id=CONN_ID,
        )
        assert sensor.poke({}) is True

    def test_poke_row_not_exists(self):
        sensor = HBaseRowSensor(
            task_id="test_row_not_exists",
            table_name=TABLE_NAME,
            row_key="no_such_row",
            hbase_conn_id=CONN_ID,
        )
        assert sensor.poke({}) is False

    def test_poke_becomes_true_after_put(self):
        sensor = HBaseRowSensor(
            task_id="test_row_appears",
            table_name=TABLE_NAME,
            row_key="row_late",
            hbase_conn_id=CONN_ID,
        )
        assert sensor.poke({}) is False

        self.hook.put_row(TABLE_NAME, "row_late", {"cf1:col1": "arrived"})
        assert sensor.poke({}) is True

    def test_poke_becomes_false_after_delete(self):
        self.hook.put_row(TABLE_NAME, "row_del", {"cf1:col1": "temp"})

        sensor = HBaseRowSensor(
            task_id="test_row_disappears",
            table_name=TABLE_NAME,
            row_key="row_del",
            hbase_conn_id=CONN_ID,
        )
        assert sensor.poke({}) is True

        self.hook.delete_row(TABLE_NAME, "row_del")
        assert sensor.poke({}) is False
