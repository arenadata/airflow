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
"""Test DuckDB provider metadata."""

from __future__ import annotations


class TestDuckDbProvider:
    """Test DuckDB provider registration."""

    def test_get_provider_info(self):
        """Test that get_provider_info returns valid metadata."""
        from airflow.providers.arenadata.duckdb.get_provider_info import get_provider_info

        provider_info = get_provider_info()

        assert provider_info is not None
        assert isinstance(provider_info, dict)

        assert provider_info["package-name"] == "apache-airflow-providers-arenadata-duckdb"
        assert provider_info["name"] == "Arenadata DuckDB"

        assert len(provider_info["hooks"]) == 1
        assert len(provider_info["operators"]) == 1
        assert len(provider_info["sensors"]) == 1
        assert len(provider_info["connection-types"]) == 1
        assert provider_info["connection-types"][0]["connection-type"] == "duckdb"

    def test_provider_version(self):
        """Test that provider version is defined."""
        from airflow.providers.arenadata.duckdb import __version__

        assert __version__ == "1.0.0"
