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

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone import HdfsToOzoneOperator


class TestHdfsToOzoneOperator:
    @patch(
        "airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone.shutil.which",
        return_value="/usr/bin/hadoop",
    )
    @patch("airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute(self, mock_run_with_retry: MagicMock, _mock_which: MagicMock):
        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_test",
            source_path="hdfs://nn:8020/user/data",
            dest_path="ofs://om:9862/vol1/bucket1/data",
        )
        operator.execute(context={})
        expected_cmd = [
            "hadoop",
            "distcp",
            "-update",
            "-skipcrccheck",
            "hdfs://nn:8020/user/data",
            "ofs://om:9862/vol1/bucket1/data",
        ]
        mock_run_with_retry.assert_called_once()
        call_args = mock_run_with_retry.call_args.args[0]
        assert call_args == expected_cmd

    @patch("airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone.shutil.which", return_value=None)
    @patch("airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute_missing_hadoop_fails_fast(self, mock_run_process: MagicMock, _mock_which: MagicMock):
        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_missing_hadoop",
            source_path="hdfs://nn:8020/user/data",
            dest_path="ofs://om:9862/vol1/bucket1/data",
        )
        with pytest.raises(AirflowException, match="executable 'hadoop' was not found in PATH"):
            operator.execute(context={})
        mock_run_process.assert_not_called()

    @patch(
        "airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone.shutil.which",
        return_value="/usr/bin/hadoop",
    )
    @patch("airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute_empty_source_fails_before_runner(
        self, mock_run_process: MagicMock, _mock_which: MagicMock
    ):
        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_empty_source",
            source_path="   ",
            dest_path="ofs://om:9862/vol1/bucket1/data",
        )
        with pytest.raises(AirflowException, match="requires non-empty source_path"):
            operator.execute(context={})
        mock_run_process.assert_not_called()

    @patch(
        "airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone.shutil.which",
        return_value="/usr/bin/hadoop",
    )
    @patch("airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone.CliRunner.run_process")
    def test_execute_empty_dest_fails_before_runner(
        self, mock_run_process: MagicMock, _mock_which: MagicMock
    ):
        operator = HdfsToOzoneOperator(
            task_id="hdfs_to_ozone_empty_dest",
            source_path="hdfs://nn:8020/user/data",
            dest_path="",
        )
        with pytest.raises(AirflowException, match="requires non-empty dest_path"):
            operator.execute(context={})
        mock_run_process.assert_not_called()

    def test_init_invalid_optional_conn_type(self):
        with pytest.raises(ValueError, match="hdfs_conn_id parameter cannot be an empty string"):
            HdfsToOzoneOperator(
                task_id="hdfs_to_ozone_test_invalid",
                source_path="hdfs://nn:8020/user/data",
                dest_path="ofs://om:9862/vol1/bucket1/data",
                hdfs_conn_id=123,  # type: ignore[arg-type]
            )
