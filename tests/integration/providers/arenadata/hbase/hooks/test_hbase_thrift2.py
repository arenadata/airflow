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
from airflow.utils import db

TABLE_NAME = "integration_test_table"
CONN_ID = "hbase_test"
HBASE_HOST = os.environ.get("HBASE_HOST", "hbase" if os.environ.get("INTEGRATION_HBASE") == "true" else "localhost")


@pytest.mark.integration("hbase")
class TestHBaseThriftHookIntegration:

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
        # Cleanup before each test
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)

    def teardown_method(self):
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.close()

    def test_create_and_delete_table(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        assert self.hook.table_exists(TABLE_NAME)

        self.hook.delete_table(TABLE_NAME)
        assert not self.hook.table_exists(TABLE_NAME)

    def test_put_and_get_row(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "value1"})

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert result is not None

    def test_delete_row(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "value1"})
        self.hook.delete_row(TABLE_NAME, "row1")

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert not result

    def test_scan_table(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        for i in range(5):
            self.hook.put_row(TABLE_NAME, f"row_{i}", {"cf1:col1": f"val_{i}"})

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 5

    def test_scan_with_limit(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        for i in range(10):
            self.hook.put_row(TABLE_NAME, f"row_{i:02d}", {"cf1:col1": f"val_{i}"})

        results = self.hook.scan_table(TABLE_NAME, limit=3)
        assert len(results) == 3

    def test_scan_with_row_range(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        for i in range(10):
            self.hook.put_row(TABLE_NAME, f"row_{i:02d}", {"cf1:col1": f"val_{i}"})

        results = self.hook.scan_table(TABLE_NAME, row_start="row_03", row_stop="row_07")
        assert len(results) == 4

    def test_batch_put_and_get(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        rows = [{"row_key": f"row_{i}", "cf1:col1": f"val_{i}"} for i in range(10)]
        self.hook.batch_put_rows(TABLE_NAME, rows, batch_size=5)

        results = self.hook.batch_get_rows(TABLE_NAME, [f"row_{i}" for i in range(10)])
        assert len(results) == 10

    def test_batch_delete(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        rows = [{"row_key": f"row_{i}", "cf1:col1": f"val_{i}"} for i in range(5)]
        self.hook.batch_put_rows(TABLE_NAME, rows, batch_size=5)

        self.hook.batch_delete_rows(TABLE_NAME, [f"row_{i}" for i in range(5)])

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 0

    def test_table_not_exists(self):
        assert not self.hook.table_exists("nonexistent_table")

    def test_multiple_column_families(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "1", "cf2:b": "2"})

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert result is not None
