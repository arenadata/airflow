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
"""Integration tests for HBase edge cases."""
from __future__ import annotations

import os

import pytest

from airflow.models import Connection
from airflow.providers.arenadata.hbase.hooks.hbase import HBaseThriftHook
from airflow.utils import db

TABLE_NAME = "integration_test_edge"
CONN_ID = "hbase_test_edge"
HBASE_HOST = os.environ.get(
    "HBASE_HOST",
    "hbase" if os.environ.get("INTEGRATION_HBASE") == "true" else "localhost",
)


@pytest.mark.integration("hbase")
class TestHBaseEdgeCases:

    def setup_method(self):
        db.merge_conn(
            Connection(conn_id=CONN_ID, conn_type="hbase", host=HBASE_HOST, port=9090)
        )
        self.hook = HBaseThriftHook(hbase_conn_id=CONN_ID)
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)

    def teardown_method(self):
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.close()

    # get nonexistent row → empty dict
    def test_get_nonexistent_row_returns_empty(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        result = self.hook.get_row(TABLE_NAME, "no_such_row")
        assert result == {}

    # scan empty table → empty list
    def test_scan_empty_table_returns_empty_list(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        results = self.hook.scan_table(TABLE_NAME)
        assert results == []

    # delete nonexistent row (should not raise)
    def test_delete_nonexistent_row_does_not_raise(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.delete_row(TABLE_NAME, "row_that_never_existed")
        # reaching here means no exception was raised

    def test_delete_nonexistent_row_leaves_table_intact(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "v1"})

        self.hook.delete_row(TABLE_NAME, "row_missing")

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert result

    # delete specific columns (not entire row)
    def test_delete_specific_column_keeps_others(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "1", "cf1:b": "2", "cf2:c": "3"})

        self.hook.delete_row(TABLE_NAME, "row1", columns=["cf1:a"])

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert result
        keys = set(result.keys())
        assert "cf1:a" not in [
            k for k, v in result.items()
        ] or result.get("cf1:a") is None
        # cf1:b and cf2:c should still be present
        values = [v.decode() if isinstance(v, bytes) else v for v in result.values()]
        assert "2" in values
        assert "3" in values

    def test_delete_all_columns_leaves_no_data(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "1", "cf1:b": "2"})

        self.hook.delete_row(TABLE_NAME, "row1", columns=["cf1:a", "cf1:b"])

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert not result

    # put overwrites existing row
    def test_put_overwrites_value(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "old_value"})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "new_value"})

        result = self.hook.get_row(TABLE_NAME, "row1")
        values = [v.decode() if isinstance(v, bytes) else v for v in result.values()]
        assert "new_value" in values
        assert "old_value" not in values

    def test_put_adds_new_column_to_existing_row(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "1"})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:b": "2"})

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert len(result) == 2

    # batch_put with >1000 rows (chunking verification)
    def test_batch_put_over_1000_rows(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        rows = [{"row_key": f"row_{i:05d}", "cf1:col1": f"val_{i}"} for i in range(1500)]
        self.hook.batch_put_rows(TABLE_NAME, rows, batch_size=200)

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 1500

    def test_batch_put_over_1000_rows_small_batch(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        rows = [{"row_key": f"row_{i:05d}", "cf1:col1": f"val_{i}"} for i in range(1100)]
        self.hook.batch_put_rows(TABLE_NAME, rows, batch_size=50)

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 1100

    # get_row with columns filter
    def test_get_row_with_columns_filter(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "1", "cf1:b": "2", "cf2:c": "3"})

        result = self.hook.get_row(TABLE_NAME, "row1", columns=["cf1:a"])
        assert result
        assert len(result) == 1
        values = [v.decode() if isinstance(v, bytes) else v for v in result.values()]
        assert "1" in values

    def test_get_row_with_multiple_columns_filter(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "1", "cf1:b": "2", "cf2:c": "3"})

        result = self.hook.get_row(TABLE_NAME, "row1", columns=["cf1:a", "cf2:c"])
        assert len(result) == 2

    def test_get_row_with_nonexistent_column_filter(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "1"})

        result = self.hook.get_row(TABLE_NAME, "row1", columns=["cf1:zzz"])
        assert not result

    # scan with columns filter
    def test_scan_with_columns_filter(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        for i in range(5):
            self.hook.put_row(TABLE_NAME, f"row_{i:02d}", {"cf1:a": f"v{i}", "cf2:b": f"x{i}"})

        results = self.hook.scan_table(TABLE_NAME, columns=["cf1:a"])
        assert len(results) == 5
        for row_key, data in results:
            values_decoded = [v.decode() if isinstance(v, bytes) else v for v in data.values()]
            # should only contain cf1:a values, not cf2:b
            assert len(data) == 1

    def test_scan_with_columns_and_limit(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        for i in range(10):
            self.hook.put_row(TABLE_NAME, f"row_{i:02d}", {"cf1:a": f"v{i}", "cf2:b": f"x{i}"})

        results = self.hook.scan_table(TABLE_NAME, columns=["cf2:b"], limit=3)
        assert len(results) == 3
        for row_key, data in results:
            assert len(data) == 1

    def test_scan_with_columns_and_row_range(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        for i in range(10):
            self.hook.put_row(TABLE_NAME, f"row_{i:02d}", {"cf1:a": f"v{i}", "cf2:b": f"x{i}"})

        results = self.hook.scan_table(
            TABLE_NAME, row_start="row_02", row_stop="row_05", columns=["cf1:a"]
        )
        assert len(results) == 3
        for row_key, data in results:
            assert len(data) == 1
