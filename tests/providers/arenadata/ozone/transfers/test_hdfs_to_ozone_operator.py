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

from airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone import HdfsToOzoneOperator


class TestHdfsToOzoneOperator:
    """Unit tests for HdfsToOzoneOperator."""

    @patch("airflow.providers.arenadata.ozone.transfers.hdfs_to_ozone.HdfsToOzoneOperator._execute_distcp")
    def test_execute(self, mock_execute_distcp: MagicMock):
        """Verify that the operator calls _execute_distcp with the correct distcp command."""

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

        mock_execute_distcp.assert_called_once()
        call_args = mock_execute_distcp.call_args[0][0]
        assert call_args == expected_cmd
