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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.arenadata.ozone.transfers.ozone_to_hive import OzoneToHiveOperator


class TestOzoneToHiveOperator:
    """Unit tests for OzoneToHiveOperator."""

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_hive._run_hive_cli_with_env")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_hive.HiveCliHook")
    def test_execute_add_partition(self, mock_hive_hook: MagicMock, mock_run_hive: MagicMock):
        """Test that OzoneToHiveOperator adds a partition correctly."""

        mock_hive_instance = mock_hive_hook.return_value
        mock_hive_instance.conn = MagicMock()
        mock_hive_instance.conn.schema = None
        mock_hive_instance.conn.extra_dejson = {}
        mock_run_hive.return_value = "OK"

        operator = OzoneToHiveOperator(
            task_id="test_hive",
            ozone_path="ofs://vol1/bucket1/data/year=2024/month=01",
            table_name="test_table",
            partition_spec={"year": "2024", "month": "01"},
        )
        operator.execute(context={})

        mock_hive_hook.assert_called_once()
        assert mock_run_hive.called
        call_args = mock_run_hive.call_args[1]["hql"]
        assert "year='2024'" in call_args or "year=2024" in call_args
        assert "month='01'" in call_args or "month=01" in call_args

    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_hive.HiveCliHook")
    @patch("airflow.providers.arenadata.ozone.transfers.ozone_to_hive.BaseHook.get_connection")
    def test_execute_no_partition(self, mock_get_connection: MagicMock, mock_hive_hook: MagicMock):
        """Test that OzoneToHiveOperator requires partition spec."""

        mock_hive_instance = mock_hive_hook.return_value
        mock_hive_instance.run_cli = MagicMock()

        mock_conn = MagicMock()
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        operator = OzoneToHiveOperator(
            task_id="test_hive", ozone_path="ofs://vol1/bucket1/data", table_name="test_table"
        )

        with pytest.raises(ValueError, match="Parameter 'partition_spec' is required"):
            operator.execute(context={})
