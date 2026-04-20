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

from airflow.providers.arenadata.hbase.client.thrift2_client import HBaseThrift2Client

TABLE_NAME = "integration_test_client"
HBASE_HOST = os.environ.get(
    "HBASE_HOST",
    "hbase" if os.environ.get("INTEGRATION_HBASE") == "true" else "localhost",
)


def _make_client(**kwargs) -> HBaseThrift2Client:
    defaults = dict(host=HBASE_HOST, port=9090, retry_max_attempts=2, retry_delay=0.5)
    defaults.update(kwargs)
    return HBaseThrift2Client(**defaults)


@pytest.mark.integration("hbase")
class TestThrift2ClientOpenClose:

    def test_open_and_close(self):
        client = _make_client()
        client.open()
        assert client._client is not None
        client.close()
        assert client._client is None

    def test_context_manager(self):
        with _make_client() as client:
            assert client._client is not None
            assert client.table_exists(TABLE_NAME) is not None  # just check it doesn't blow up

    def test_close_without_open(self):
        client = _make_client()
        client.close()  # should not raise

    def test_double_close(self):
        client = _make_client()
        client.open()
        client.close()
        client.close()  # should not raise


@pytest.mark.integration("hbase")
class TestThrift2ClientListTables:

    def setup_method(self):
        self.client = _make_client()
        self.client.open()
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)

    def teardown_method(self):
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.close()

    def test_list_tables_returns_list(self):
        result = self.client.list_tables()
        assert isinstance(result, list)

    def test_list_tables_contains_created_table(self):
        self.client.create_table(TABLE_NAME, {"cf1": {}})
        tables = self.client.list_tables()
        assert TABLE_NAME in tables

    def test_list_tables_does_not_contain_deleted_table(self):
        self.client.create_table(TABLE_NAME, {"cf1": {}})
        self.client.delete_table(TABLE_NAME)
        tables = self.client.list_tables()
        assert TABLE_NAME not in tables


@pytest.mark.integration("hbase")
class TestThrift2ClientPutMultiple:

    def setup_method(self):
        self.client = _make_client()
        self.client.open()
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.create_table(TABLE_NAME, {"cf1": {}})

    def teardown_method(self):
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.close()

    def test_put_multiple_and_get(self):
        puts = [
            ("row1", {"cf1:a": "v1"}),
            ("row2", {"cf1:a": "v2"}),
            ("row3", {"cf1:a": "v3"}),
        ]
        self.client.put_multiple(TABLE_NAME, puts)

        for row_key, data in puts:
            result = self.client.get(TABLE_NAME, row_key)
            assert result
            assert result["row"] == row_key

    def test_put_multiple_empty_list(self):
        self.client.put_multiple(TABLE_NAME, [])
        results = self.client.scan(TABLE_NAME)
        assert results == []


@pytest.mark.integration("hbase")
class TestThrift2ClientGetMultiple:

    def setup_method(self):
        self.client = _make_client()
        self.client.open()
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        self.client.put(TABLE_NAME, "row1", {"cf1:a": "1", "cf2:b": "2"})
        self.client.put(TABLE_NAME, "row2", {"cf1:a": "3", "cf2:b": "4"})
        self.client.put(TABLE_NAME, "row3", {"cf1:a": "5"})

    def teardown_method(self):
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.close()

    def test_get_multiple_all(self):
        results = self.client.get_multiple(TABLE_NAME, ["row1", "row2", "row3"])
        non_empty = [r for r in results if r]
        assert len(non_empty) == 3

    def test_get_multiple_with_columns_filter(self):
        results = self.client.get_multiple(TABLE_NAME, ["row1", "row2"], columns=["cf1:a"])
        for r in results:
            if not r:
                continue
            assert "cf1:a" in r["columns"]
            assert "cf2:b" not in r["columns"]

    def test_get_multiple_nonexistent_rows(self):
        results = self.client.get_multiple(TABLE_NAME, ["no_row_1", "no_row_2"])
        non_empty = [r for r in results if r]
        assert len(non_empty) == 0

    def test_get_multiple_mixed(self):
        results = self.client.get_multiple(TABLE_NAME, ["row1", "no_row", "row3"])
        non_empty = [r for r in results if r]
        assert len(non_empty) == 2


@pytest.mark.integration("hbase")
class TestThrift2ClientDeleteMultiple:

    def setup_method(self):
        self.client = _make_client()
        self.client.open()
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})

    def teardown_method(self):
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.close()

    def test_delete_multiple_entire_rows(self):
        self.client.put(TABLE_NAME, "row1", {"cf1:a": "1"})
        self.client.put(TABLE_NAME, "row2", {"cf1:a": "2"})
        self.client.put(TABLE_NAME, "row3", {"cf1:a": "3"})

        self.client.delete_multiple(TABLE_NAME, [("row1", None), ("row2", None)])

        assert not self.client.get(TABLE_NAME, "row1")
        assert not self.client.get(TABLE_NAME, "row2")
        assert self.client.get(TABLE_NAME, "row3")

    def test_delete_multiple_specific_columns(self):
        self.client.put(TABLE_NAME, "row1", {"cf1:a": "1", "cf2:b": "2"})

        self.client.delete_multiple(TABLE_NAME, [("row1", ["cf1:a"])])

        result = self.client.get(TABLE_NAME, "row1")
        assert result
        assert "cf1:a" not in result["columns"]
        assert "cf2:b" in result["columns"]

    def test_delete_multiple_empty_list(self):
        self.client.put(TABLE_NAME, "row1", {"cf1:a": "1"})
        self.client.delete_multiple(TABLE_NAME, [])
        assert self.client.get(TABLE_NAME, "row1")


@pytest.mark.integration("hbase")
class TestThrift2ClientScanWithColumns:

    def setup_method(self):
        self.client = _make_client()
        self.client.open()
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        for i in range(5):
            self.client.put(TABLE_NAME, f"row_{i:02d}", {"cf1:a": f"v{i}", "cf2:b": f"x{i}"})

    def teardown_method(self):
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.close()

    def test_scan_returns_all_columns(self):
        results = self.client.scan(TABLE_NAME)
        assert len(results) == 5
        for r in results:
            assert "cf1:a" in r["columns"]
            assert "cf2:b" in r["columns"]

    def test_scan_with_single_column_filter(self):
        results = self.client.scan(TABLE_NAME, columns=["cf1:a"])
        assert len(results) == 5
        for r in results:
            assert "cf1:a" in r["columns"]
            assert "cf2:b" not in r["columns"]

    def test_scan_with_multiple_column_filter(self):
        results = self.client.scan(TABLE_NAME, columns=["cf1:a", "cf2:b"])
        assert len(results) == 5
        for r in results:
            assert "cf1:a" in r["columns"]
            assert "cf2:b" in r["columns"]

    def test_scan_with_start_stop_and_columns(self):
        results = self.client.scan(
            TABLE_NAME, start_row="row_01", stop_row="row_04", columns=["cf2:b"]
        )
        assert len(results) == 3
        for r in results:
            assert "cf2:b" in r["columns"]
            assert "cf1:a" not in r["columns"]

    def test_scan_with_limit_and_columns(self):
        results = self.client.scan(TABLE_NAME, columns=["cf1:a"], limit=2)
        assert len(results) == 2


@pytest.mark.integration("hbase")
class TestThrift2ClientNamespace:
    """Test _resolve_table_name with namespace != 'default'.

    Note: creating custom namespaces requires admin API which Thrift2 does not
    expose directly, so we test the resolution logic by writing/reading through
    the default namespace explicitly qualified as 'default:table'.
    """

    def setup_method(self):
        self.client = _make_client()
        self.client.open()
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.create_table(TABLE_NAME, {"cf1": {}})

    def teardown_method(self):
        if self.client.table_exists(TABLE_NAME):
            self.client.delete_table(TABLE_NAME)
        self.client.close()

    def test_resolve_default_namespace(self):
        assert self.client._resolve_table_name(TABLE_NAME) == TABLE_NAME.encode()

    def test_resolve_explicit_namespace(self):
        fq_name = f"default:{TABLE_NAME}"
        assert self.client._resolve_table_name(fq_name) == fq_name.encode()

    def test_resolve_custom_namespace_prepends_prefix(self):
        client = _make_client(namespace="production")
        assert client._resolve_table_name("users") == b"production:users"

    def test_fully_qualified_name_ignores_namespace(self):
        client = _make_client(namespace="production")
        assert client._resolve_table_name("staging:users") == b"staging:users"

    def test_put_and_get_via_fully_qualified_name(self):
        fq_name = f"default:{TABLE_NAME}"
        self.client.put(fq_name, "row_ns", {"cf1:a": "ns_val"})

        result = self.client.get(TABLE_NAME, "row_ns")
        assert result
        assert result["row"] == "row_ns"
