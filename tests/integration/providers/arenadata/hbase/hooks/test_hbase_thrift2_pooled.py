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

import json
import os

import pytest

from airflow.models import Connection
from airflow.providers.arenadata.hbase.hooks.hbase import HBaseThriftHook
from airflow.providers.arenadata.hbase.hooks.hbase_strategy import PooledThrift2Strategy
from airflow.providers.arenadata.hbase import thrift2_pool
from airflow.utils import db

TABLE_NAME = "integration_test_pooled"
CONN_ID = "hbase_test_pooled"
HBASE_HOST = os.environ.get(
    "HBASE_HOST",
    "hbase" if os.environ.get("INTEGRATION_HBASE") == "true" else "localhost",
)
POOL_EXTRA = json.dumps({"connection_pool": {"enabled": True, "size": 4, "timeout": 30}})


def _clear_global_pool():
    """Remove the test pool from the global registry so the next test gets a fresh one."""
    with thrift2_pool._pool_lock:
        pool = thrift2_pool._thrift2_pools.pop(CONN_ID, None)
        if pool:
            pool.close_all()


@pytest.mark.integration("hbase")
class TestHBaseThriftHookPooledIntegration:

    def setup_method(self):
        _clear_global_pool()
        db.merge_conn(
            Connection(
                conn_id=CONN_ID,
                conn_type="hbase",
                host=HBASE_HOST,
                port=9090,
                extra=POOL_EXTRA,
            )
        )
        self.hook = HBaseThriftHook(hbase_conn_id=CONN_ID)
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)

    def teardown_method(self):
        try:
            if self.hook.table_exists(TABLE_NAME):
                self.hook.delete_table(TABLE_NAME)
        finally:
            _clear_global_pool()

    # Verify that the hook actually uses PooledThrift2Strategy
    def test_strategy_is_pooled(self):
        assert isinstance(self.hook._get_strategy(), PooledThrift2Strategy)

    # CRUD operations through pool
    def test_create_and_delete_table(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        assert self.hook.table_exists(TABLE_NAME)

        self.hook.delete_table(TABLE_NAME)
        assert not self.hook.table_exists(TABLE_NAME)

    def test_put_and_get_row(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "value1"})

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert result
        values = [v.decode() if isinstance(v, bytes) else v for v in result.values()]
        assert "value1" in values

    def test_get_nonexistent_row(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        result = self.hook.get_row(TABLE_NAME, "no_such_row")
        assert not result

    def test_delete_row(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "value1"})
        self.hook.delete_row(TABLE_NAME, "row1")

        assert not self.hook.get_row(TABLE_NAME, "row1")

    def test_put_overwrites_existing_row(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "old"})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "new"})

        result = self.hook.get_row(TABLE_NAME, "row1")
        values = [v.decode() if isinstance(v, bytes) else v for v in result.values()]
        assert "new" in values

    def test_multiple_column_families(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "1", "cf2:b": "2"})

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert len(result) == 2

    # Scan
    def test_scan_table(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        for i in range(5):
            self.hook.put_row(TABLE_NAME, f"row_{i}", {"cf1:col1": f"val_{i}"})

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 5

    def test_scan_empty_table(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        assert self.hook.scan_table(TABLE_NAME) == []

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

    # Batch put (parallel workers)
    def test_batch_put_and_get(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        rows = [{"row_key": f"row_{i}", "cf1:col1": f"val_{i}"} for i in range(20)]
        self.hook.batch_put_rows(TABLE_NAME, rows, batch_size=5, max_workers=4)

        results = self.hook.batch_get_rows(TABLE_NAME, [f"row_{i}" for i in range(20)])
        assert len(results) == 20

    def test_batch_put_single_worker(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        rows = [{"row_key": f"row_{i}", "cf1:col1": f"val_{i}"} for i in range(10)]
        self.hook.batch_put_rows(TABLE_NAME, rows, batch_size=5, max_workers=1)

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 10

    def test_batch_put_large(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        rows = [{"row_key": f"row_{i:04d}", "cf1:col1": f"val_{i}"} for i in range(500)]
        self.hook.batch_put_rows(TABLE_NAME, rows, batch_size=100, max_workers=4)

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 500

    # Batch delete
    def test_batch_delete(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        rows = [{"row_key": f"row_{i}", "cf1:col1": f"val_{i}"} for i in range(10)]
        self.hook.batch_put_rows(TABLE_NAME, rows, batch_size=5, max_workers=2)

        self.hook.batch_delete_rows(TABLE_NAME, [f"row_{i}" for i in range(10)])

        assert self.hook.scan_table(TABLE_NAME) == []

    def test_batch_delete_partial(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        rows = [{"row_key": f"row_{i}", "cf1:col1": f"val_{i}"} for i in range(10)]
        self.hook.batch_put_rows(TABLE_NAME, rows, batch_size=10)

        self.hook.batch_delete_rows(TABLE_NAME, [f"row_{i}" for i in range(5)])

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 5

    # Close / reuse
    def test_close_and_reuse(self):
        """After close() the hook must recreate the pool and work again."""
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "v1"})

        # close destroys the strategy; clear global pool so a fresh one is created
        self.hook.close()
        _clear_global_pool()

        assert self.hook.table_exists(TABLE_NAME)
        result = self.hook.get_row(TABLE_NAME, "row1")
        assert result

    def test_table_not_exists(self):
        assert not self.hook.table_exists("nonexistent_table_pooled")
