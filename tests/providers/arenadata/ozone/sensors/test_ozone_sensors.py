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
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneCliTransientError, OzoneFsHook
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor, OzoneS3KeySensor


class TestOzoneKeySensor:
    """Unit tests for OzoneKeySensor."""

    @patch("airflow.providers.arenadata.ozone.sensors.ozone.OzoneFsHook")
    def test_poke_key_exists(self, mock_ozone_hook: MagicMock):
        """Test that poke returns True when key exists."""

        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.exists = MagicMock(return_value=True)

        sensor = OzoneKeySensor(task_id="test_sensor", path="ofs://vol1/bucket1/key1")
        result = sensor.poke(context={})

        assert result is True
        mock_ozone_hook.assert_called_once_with(OzoneFsHook.default_conn_name)
        mock_hook_instance.exists.assert_called_once_with("ofs://vol1/bucket1/key1")

    @patch("airflow.providers.arenadata.ozone.sensors.ozone.OzoneFsHook")
    def test_poke_raises_when_cli_missing(self, mock_ozone_hook: MagicMock):
        """Test that poke re-raises when Ozone CLI is not available."""

        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.exists.side_effect = AirflowException(
            "Ozone CLI not found. Please ensure Ozone is installed and available in PATH."
        )

        sensor = OzoneKeySensor(task_id="test_sensor", path="ofs://vol1/bucket1/key1")

        with pytest.raises(AirflowException):
            sensor.poke(context={})

    @patch("airflow.providers.arenadata.ozone.sensors.ozone.OzoneFsHook")
    def test_poke_transient_error_returns_false(self, mock_ozone_hook: MagicMock):
        """Test that poke returns False on OzoneCliTransientError (triggers retry)."""

        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.exists.side_effect = OzoneCliTransientError("Connection reset")

        sensor = OzoneKeySensor(task_id="test_sensor", path="ofs://vol1/bucket1/key1")
        result = sensor.poke(context={})

        assert result is False


class TestOzoneS3KeySensor:
    """Unit tests for OzoneS3KeySensor (BaseSensorOperator + OzoneS3Hook)."""

    @patch("airflow.providers.arenadata.ozone.sensors.ozone.OzoneS3Hook")
    def test_poke_multiple_keys_all_exist(self, mock_ozone_hook: MagicMock):
        """Test that poke returns True when all keys in list exist."""
        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.check_for_key = MagicMock(return_value=True)

        sensor = OzoneS3KeySensor(
            task_id="test_s3_sensor",
            bucket_name="mybucket",
            bucket_key=["a.txt", "b.txt"],
        )
        result = sensor.poke(context={})

        assert result is True
        assert mock_hook_instance.check_for_key.call_count == 2

    @patch("airflow.providers.arenadata.ozone.sensors.ozone.OzoneS3Hook")
    def test_bucket_name_optional_with_s3_url(self, mock_ozone_hook: MagicMock):
        """If bucket_name is None, a full s3://bucket/key URL should be supported."""

        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.check_for_key = MagicMock(return_value=True)

        sensor = OzoneS3KeySensor(
            task_id="test_s3_sensor",
            bucket_name=None,
            bucket_key="s3://mybucket/path/file.txt",
        )
        result = sensor.poke(context={})

        assert result is True
        mock_hook_instance.check_for_key.assert_called_once_with("path/file.txt", "mybucket")

    def test_bucket_name_required_for_plain_key(self):
        """If bucket_name is None and key is not s3:// URL, sensor should fail fast."""

        sensor = OzoneS3KeySensor(
            task_id="test_s3_sensor",
            bucket_name=None,
            bucket_key="plain_key.txt",
        )

        with pytest.raises(AirflowException, match="bucket_name is required"):
            sensor.poke(context={})
