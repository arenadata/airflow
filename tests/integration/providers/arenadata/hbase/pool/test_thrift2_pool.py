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
import concurrent.futures

import pytest

from airflow.providers.arenadata.hbase.thrift2_pool import Thrift2ConnectionPool

TABLE_NAME = "integration_test_pool"
HBASE_HOST = os.environ.get(
    "HBASE_HOST",
    "hbase" if os.environ.get("INTEGRATION_HBASE") == "true" else "localhost",
)


def _make_pool(size=4, **kwargs) -> Thrift2ConnectionPool:
    defaults = dict(
        host=HBASE_HOST,
        port=9090,
        retry_max_attempts=2,
        retry_delay=0.5,
        borrow_timeout=10.0,
    )
    defaults.update(kwargs)
    return Thrift2ConnectionPool(size=size, **defaults)


@pytest.mark.integration("hbase")
class TestPoolBorrowReturn:

    def setup_method(self):
        self.pool = _make_pool(size=2)

    def teardown_method(self):
        self.pool.close_all()

    def test_borrow_returns_working_client(self):
        with self.pool.connection() as client:
            tables = client.list_tables()
            assert isinstance(tables, list)

    def test_borrow_and_return_does_not_leak(self):
        for _ in range(10):
            with self.pool.connection() as client:
                client.list_tables()
        # If connections leaked, the pool would be exhausted and hang.
        # Reaching here means all 10 iterations completed within borrow_timeout.

    def test_returned_connection_is_reusable(self):
        with self.pool.connection() as client:
            first_id = id(client)
            client.list_tables()

        with self.pool.connection() as client:
            client.list_tables()
            # The pool has size=2 and only one was used, so we should get the same object back.
            assert id(client) == first_id


@pytest.mark.integration("hbase")
class TestPoolReuse:

    def setup_method(self):
        self.pool = _make_pool(size=2)

    def teardown_method(self):
        self.pool.close_all()

    def test_sequential_reuse_same_connection(self):
        seen_ids = set()
        for _ in range(5):
            with self.pool.connection() as client:
                seen_ids.add(id(client))
                client.list_tables()
        # With sequential access and pool size=2, only 1 connection should be created.
        assert len(seen_ids) == 1

    def test_pool_creates_up_to_size_connections(self):
        """Borrow two connections simultaneously — pool must create two distinct clients."""
        clients = []
        # Manually borrow without context manager to hold both at once.
        gen1 = self.pool.connection()
        c1 = gen1.__enter__()
        gen2 = self.pool.connection()
        c2 = gen2.__enter__()

        try:
            assert c1 is not c2
            c1.list_tables()
            c2.list_tables()
        finally:
            gen2.__exit__(None, None, None)
            gen1.__exit__(None, None, None)


@pytest.mark.integration("hbase")
class TestPoolConcurrency:

    def setup_method(self):
        self.pool = _make_pool(size=4)
        # Ensure test table exists
        with self.pool.connection() as client:
            if not client.table_exists(TABLE_NAME):
                client.create_table(TABLE_NAME, {"cf1": {}})

    def teardown_method(self):
        with self.pool.connection() as client:
            if client.table_exists(TABLE_NAME):
                client.delete_table(TABLE_NAME)
        self.pool.close_all()

    def test_concurrent_reads(self):
        # Seed data
        with self.pool.connection() as client:
            for i in range(10):
                client.put(TABLE_NAME, f"row_{i:02d}", {"cf1:a": f"v{i}"})

        errors = []

        def read_task(row_key):
            try:
                with self.pool.connection() as client:
                    result = client.get(TABLE_NAME, row_key)
                    assert result, f"Expected data for {row_key}"
            except Exception as exc:
                errors.append(exc)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(read_task, f"row_{i:02d}") for i in range(10)]
            concurrent.futures.wait(futures)

        assert not errors, f"Concurrent reads failed: {errors}"

    def test_concurrent_writes(self):
        errors = []

        def write_task(idx):
            try:
                with self.pool.connection() as client:
                    client.put(TABLE_NAME, f"cw_row_{idx}", {"cf1:a": f"val_{idx}"})
            except Exception as exc:
                errors.append(exc)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(write_task, i) for i in range(20)]
            concurrent.futures.wait(futures)

        assert not errors, f"Concurrent writes failed: {errors}"

        with self.pool.connection() as client:
            results = client.scan(TABLE_NAME, start_row="cw_row_")
            assert len(results) >= 20


@pytest.mark.integration("hbase")
class TestPoolReconnect:

    def test_reconnects_dead_connection(self):
        pool = _make_pool(size=1)
        try:
            # Borrow, use, return
            with pool.connection() as client:
                client.list_tables()

            # Kill the transport of the returned connection to simulate a dead link
            queued_client = pool._pool.get_nowait()
            queued_client._transport.close()
            queued_client._transport = None
            pool._pool.put(queued_client)

            # Next borrow should detect the dead connection and reconnect
            with pool.connection() as client:
                tables = client.list_tables()
                assert isinstance(tables, list)
        finally:
            pool.close_all()


@pytest.mark.integration("hbase")
class TestPoolCloseAll:

    def test_close_all_empties_pool(self):
        pool = _make_pool(size=3)
        # Create 3 connections and return them to the pool
        conns = []
        gens = []
        for _ in range(3):
            g = pool.connection()
            c = g.__enter__()
            c.list_tables()
            conns.append(c)
            gens.append(g)
        for g in gens:
            g.__exit__(None, None, None)

        assert not pool._pool.empty()

        pool.close_all()

        assert pool._pool.empty()

    def test_close_all_on_empty_pool(self):
        pool = _make_pool(size=2)
        pool.close_all()  # should not raise

    def test_operations_work_after_close_all_with_fresh_pool(self):
        """After close_all a new pool can serve requests normally."""
        pool = _make_pool(size=2)
        with pool.connection() as client:
            client.list_tables()

        pool.close_all()

        # Create a fresh pool (simulates what hook does after close)
        pool2 = _make_pool(size=2)
        try:
            with pool2.connection() as client:
                tables = client.list_tables()
                assert isinstance(tables, list)
        finally:
            pool2.close_all()
