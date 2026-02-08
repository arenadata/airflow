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
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneCliTransientError
from airflow.providers.arenadata.ozone.hooks.ozone_fs import OzoneFsHook
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor


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
    def test_poke_key_not_exists(self, mock_ozone_hook: MagicMock):
        """Test that poke returns False when key does not exist."""

        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.exists = MagicMock(return_value=False)

        sensor = OzoneKeySensor(task_id="test_sensor", path="ofs://vol1/bucket1/key1")
        result = sensor.poke(context={})

        assert result is False

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

    @patch("airflow.providers.arenadata.ozone.sensors.ozone.OzoneFsHook")
    def test_poke_unexpected_error(self, mock_ozone_hook: MagicMock):
        """Test that poke re-raises unexpected errors."""

        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.exists.side_effect = FileNotFoundError("ozone command not found")

        sensor = OzoneKeySensor(task_id="test_sensor", path="ofs://vol1/bucket1/key1")

        with pytest.raises(FileNotFoundError):
            sensor.poke(context={})


class TestOzoneS3KeySensor:
    """
    Unit tests for OzoneS3KeySensor.

    Note: Tests for poke() method are skipped due to complex inheritance hierarchy
    (OzoneS3KeySensor -> S3KeySensor -> AwsBaseSensor -> BaseSensorOperator)
    that makes mocking difficult. OzoneS3KeySensor is a wrapper around S3KeySensor
    and relies on the parent class for parameter validation.
    """
