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
class TestPoolConnectionReuse:
    """Verify that alive connections are reused without reconnect."""

    def test_no_reconnect_over_multiple_borrows(self):
        """Multiple sequential borrows must never trigger reconnect on a healthy pool."""
        pool = _make_pool(size=1)
        try:
            # Warm up: create the connection
            with pool.connection() as client:
                client.list_tables()

            # Patch
            queued_client = pool._pool.get_nowait()
            open_call_count = 0
            original_open = queued_client.open

            def counting_open():
                nonlocal open_call_count
                open_call_count += 1
                original_open()

            queued_client.open = counting_open
            pool._pool.put(queued_client)

            for _ in range(10):
                with pool.connection() as client:
                    client.list_tables()

            assert open_call_count == 0, (
                f"open() was called {open_call_count} time(s) over 10 borrows. "
                "Alive connections should be reused without reconnect."
            )
        finally:
            pool.close_all()
