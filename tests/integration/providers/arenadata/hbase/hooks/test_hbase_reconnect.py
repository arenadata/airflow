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
"""Integration tests for hook close and reconnect behaviour."""
from __future__ import annotations

import os

import pytest

from airflow.models import Connection
from airflow.providers.arenadata.hbase.hooks.hbase import HBaseThriftHook
from airflow.providers.arenadata.hbase.hooks.hbase_strategy import Thrift2Strategy
from airflow.utils import db

TABLE_NAME = "integration_test_reconnect"
CONN_ID = "hbase_test_reconnect"
HBASE_HOST = os.environ.get(
    "HBASE_HOST",
    "hbase" if os.environ.get("INTEGRATION_HBASE") == "true" else "localhost",
)


@pytest.mark.integration("hbase")
class TestHookCloseAndReuse:
    """Verify that hook recreates strategy after close() and keeps working."""

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

    def test_close_sets_strategy_to_none(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        assert self.hook._strategy is not None

        self.hook.close()
        assert self.hook._strategy is None

    def test_operations_work_after_close(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "v1"})

        self.hook.close()

        # Hook should lazily recreate strategy on next call
        assert self.hook.table_exists(TABLE_NAME)
        result = self.hook.get_row(TABLE_NAME, "row1")
        assert result

    def test_multiple_close_reuse_cycles(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})

        for i in range(3):
            self.hook.put_row(TABLE_NAME, f"row_{i}", {"cf1:a": f"v{i}"})
            self.hook.close()

        results = self.hook.scan_table(TABLE_NAME)
        assert len(results) == 3

    def test_close_on_fresh_hook(self):
        """close() on a hook that was never used should not raise."""
        fresh_hook = HBaseThriftHook(hbase_conn_id=CONN_ID)
        fresh_hook.close()

    def test_double_close(self):
        self.hook.create_table(TABLE_NAME, {"cf1": {}})
        self.hook.close()
        self.hook.close()  # should not raise


@pytest.mark.integration("hbase")
class TestHookReconnectAfterBrokenTransport:
    """Verify that hook recovers when the underlying transport is killed."""

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

    def test_recover_after_transport_closed(self):
        """Kill the transport, then close+reopen the hook — operations should work."""
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "v1"})

        # Forcefully kill the underlying transport
        strategy = self.hook._get_strategy()
        assert isinstance(strategy, Thrift2Strategy)
        strategy.client._transport.close()

        # close() resets strategy; next call recreates it
        self.hook.close()

        result = self.hook.get_row(TABLE_NAME, "row1")
        assert result

    def test_recover_after_client_set_to_none(self):
        """Simulate a fully dead client reference."""
        self.hook.put_row(TABLE_NAME, "row1", {"cf1:a": "v1"})

        strategy = self.hook._get_strategy()
        assert isinstance(strategy, Thrift2Strategy)
        strategy.client._client = None
        strategy.client._transport = None

        self.hook.close()

        assert self.hook.table_exists(TABLE_NAME)
        result = self.hook.get_row(TABLE_NAME, "row1")
        assert result

    def test_write_after_reconnect(self):
        """After reconnect, writes should persist."""
        strategy = self.hook._get_strategy()
        assert isinstance(strategy, Thrift2Strategy)
        strategy.client._transport.close()

        self.hook.close()

        self.hook.put_row(TABLE_NAME, "row_after", {"cf1:a": "reconnected"})
        result = self.hook.get_row(TABLE_NAME, "row_after")
        assert result
        values = [v.decode() if isinstance(v, bytes) else v for v in result.values()]
        assert "reconnected" in values
