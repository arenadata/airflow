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
from airflow.providers.arenadata.hbase.operators.hbase import (
    HBaseBatchGetOperator,
    HBaseBatchPutOperator,
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBasePutOperator,
    HBaseScanOperator,
    IfExistsAction,
    IfNotExistsAction,
)
from airflow.utils import db

TABLE_NAME = "integration_test_operators"
CONN_ID = "hbase_test_operators"
HBASE_HOST = os.environ.get(
    "HBASE_HOST",
    "hbase" if os.environ.get("INTEGRATION_HBASE") == "true" else "localhost",
)


@pytest.mark.integration("hbase")
class TestHBasePutOperatorIntegration:

    def setup_method(self):
        db.merge_conn(
            Connection(conn_id=CONN_ID, conn_type="hbase", host=HBASE_HOST, port=9090)
        )
        self.hook = HBaseThriftHook(hbase_conn_id=CONN_ID)
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.create_table(TABLE_NAME, {"cf1": {}})

    def teardown_method(self):
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.close()

    def test_execute_inserts_row(self):
        op = HBasePutOperator(
            task_id="put_row",
            table_name=TABLE_NAME,
            row_key="row1",
            data={"cf1:col1": "value1"},
            hbase_conn_id=CONN_ID,
        )
        op.execute()

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert result
        values = [v.decode() if isinstance(v, bytes) else v for v in result.values()]
        assert "value1" in values

    def test_execute_overwrites_row(self):
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:col1": "old"})

        op = HBasePutOperator(
            task_id="put_overwrite",
            table_name=TABLE_NAME,
            row_key="row1",
            data={"cf1:col1": "new"},
            hbase_conn_id=CONN_ID,
        )
        op.execute()

        result = self.hook.get_row(TABLE_NAME, "row1")
        values = [v.decode() if isinstance(v, bytes) else v for v in result.values()]
        assert "new" in values


@pytest.mark.integration("hbase")
class TestHBaseCreateTableOperatorIntegration:

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

    def test_execute_creates_table(self):
        op = HBaseCreateTableOperator(
            task_id="create_table",
            table_name=TABLE_NAME,
            families={"cf1": {}, "cf2": {}},
            hbase_conn_id=CONN_ID,
        )
        op.execute()

        assert self.hook.table_exists(TABLE_NAME)

    def test_execute_if_exists_ignore(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})

        op = HBaseCreateTableOperator(
            task_id="create_ignore",
            table_name=TABLE_NAME,
            families={"cf1": {}},
            if_exists=IfExistsAction.IGNORE,
            hbase_conn_id=CONN_ID,
        )
        op.execute()  # should not raise

        assert self.hook.table_exists(TABLE_NAME)

    def test_execute_if_exists_error(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})

        op = HBaseCreateTableOperator(
            task_id="create_error",
            table_name=TABLE_NAME,
            families={"cf1": {}},
            if_exists=IfExistsAction.ERROR,
            hbase_conn_id=CONN_ID,
        )
        with pytest.raises(ValueError, match="already exists"):
            op.execute()


@pytest.mark.integration("hbase")
class TestHBaseDeleteTableOperatorIntegration:

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

    def test_execute_deletes_table(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})

        op = HBaseDeleteTableOperator(
            task_id="delete_table",
            table_name=TABLE_NAME,
            hbase_conn_id=CONN_ID,
        )
        op.execute()

        assert not self.hook.table_exists(TABLE_NAME)

    def test_execute_if_not_exists_ignore(self):
        op = HBaseDeleteTableOperator(
            task_id="delete_ignore",
            table_name=TABLE_NAME,
            if_not_exists=IfNotExistsAction.IGNORE,
            hbase_conn_id=CONN_ID,
        )
        op.execute()  # should not raise

    def test_execute_if_not_exists_error(self):
        op = HBaseDeleteTableOperator(
            task_id="delete_error",
            table_name=TABLE_NAME,
            if_not_exists=IfNotExistsAction.ERROR,
            hbase_conn_id=CONN_ID,
        )
        with pytest.raises(ValueError, match="does not exist"):
            op.execute()


@pytest.mark.integration("hbase")
class TestHBaseScanOperatorIntegration:

    def setup_method(self):
        db.merge_conn(
            Connection(conn_id=CONN_ID, conn_type="hbase", host=HBASE_HOST, port=9090)
        )
        self.hook = HBaseThriftHook(hbase_conn_id=CONN_ID)
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        for i in range(5):
            self.hook.put_row(TABLE_NAME, f"row_{i:02d}", {"cf1:a": f"v{i}", "cf2:b": f"x{i}"})

    def teardown_method(self):
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.close()

    def test_execute_returns_serializable_list(self):
        op = HBaseScanOperator(
            task_id="scan_all",
            table_name=TABLE_NAME,
            hbase_conn_id=CONN_ID,
        )
        result = op.execute()

        assert isinstance(result, list)
        assert len(result) == 5
        # Each item must be a plain dict with string keys (JSON-serializable)
        for row in result:
            assert isinstance(row, dict)
            assert "row_key" in row

    def test_execute_with_limit(self):
        op = HBaseScanOperator(
            task_id="scan_limit",
            table_name=TABLE_NAME,
            limit=2,
            hbase_conn_id=CONN_ID,
        )
        result = op.execute()
        assert len(result) == 2

    def test_execute_with_row_range(self):
        op = HBaseScanOperator(
            task_id="scan_range",
            table_name=TABLE_NAME,
            row_start="row_01",
            row_stop="row_04",
            hbase_conn_id=CONN_ID,
        )
        result = op.execute()
        assert len(result) == 3

    def test_execute_values_are_strings(self):
        op = HBaseScanOperator(
            task_id="scan_strings",
            table_name=TABLE_NAME,
            limit=1,
            hbase_conn_id=CONN_ID,
        )
        result = op.execute()
        row = result[0]
        for key, val in row.items():
            assert isinstance(key, str)
            assert isinstance(val, str)

    def test_execute_empty_table(self):
        # Delete all rows
        for i in range(5):
            self.hook.delete_row(TABLE_NAME, f"row_{i:02d}")

        op = HBaseScanOperator(
            task_id="scan_empty",
            table_name=TABLE_NAME,
            hbase_conn_id=CONN_ID,
        )
        result = op.execute()
        assert result == []


@pytest.mark.integration("hbase")
class TestHBaseBatchPutOperatorIntegration:

    def setup_method(self):
        db.merge_conn(
            Connection(conn_id=CONN_ID, conn_type="hbase", host=HBASE_HOST, port=9090)
        )
        self.hook = HBaseThriftHook(hbase_conn_id=CONN_ID)
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.create_table(TABLE_NAME, {"cf1": {}})

    def teardown_method(self):
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.close()

    def test_execute_inserts_rows(self):
        rows = [{"row_key": f"row_{i}", "cf1:col1": f"val_{i}"} for i in range(10)]
        op = HBaseBatchPutOperator(
            task_id="batch_put",
            table_name=TABLE_NAME,
            rows=rows,
            batch_size=5,
            hbase_conn_id=CONN_ID,
        )
        op.execute()

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 10

    def test_execute_empty_rows(self):
        op = HBaseBatchPutOperator(
            task_id="batch_put_empty",
            table_name=TABLE_NAME,
            rows=[],
            hbase_conn_id=CONN_ID,
        )
        op.execute()  # should not raise

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 0


@pytest.mark.integration("hbase")
class TestHBaseBatchGetOperatorIntegration:

    def setup_method(self):
        db.merge_conn(
            Connection(conn_id=CONN_ID, conn_type="hbase", host=HBASE_HOST, port=9090)
        )
        self.hook = HBaseThriftHook(hbase_conn_id=CONN_ID)
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        for i in range(5):
            self.hook.put_row(TABLE_NAME, f"row_{i}", {"cf1:a": f"v{i}", "cf2:b": f"x{i}"})

    def teardown_method(self):
        if self.hook.table_exists(TABLE_NAME):
            self.hook.delete_table(TABLE_NAME)
        self.hook.close()

    def test_execute_returns_serializable_list(self):
        op = HBaseBatchGetOperator(
            task_id="batch_get",
            table_name=TABLE_NAME,
            row_keys=[f"row_{i}" for i in range(5)],
            hbase_conn_id=CONN_ID,
        )
        result = op.execute()

        assert isinstance(result, list)
        assert len(result) == 5
        for row in result:
            assert isinstance(row, dict)

    def test_execute_with_columns_filter(self):
        op = HBaseBatchGetOperator(
            task_id="batch_get_cols",
            table_name=TABLE_NAME,
            row_keys=["row_0", "row_1"],
            columns=["cf1:a"],
            hbase_conn_id=CONN_ID,
        )
        result = op.execute()

        assert len(result) == 2
        for row in result:
            assert "cf1:a" in row
            assert "cf2:b" not in row

    def test_execute_values_are_strings(self):
        op = HBaseBatchGetOperator(
            task_id="batch_get_strings",
            table_name=TABLE_NAME,
            row_keys=["row_0"],
            hbase_conn_id=CONN_ID,
        )
        result = op.execute()
        for row in result:
            for key, val in row.items():
                assert isinstance(key, str)
                assert isinstance(val, str)

    def test_execute_nonexistent_rows(self):
        op = HBaseBatchGetOperator(
            task_id="batch_get_missing",
            table_name=TABLE_NAME,
            row_keys=["no_row_1", "no_row_2"],
            hbase_conn_id=CONN_ID,
        )
        result = op.execute()
        assert result == []
