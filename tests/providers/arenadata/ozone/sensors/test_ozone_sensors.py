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
from airflow.providers.arenadata.ozone.hooks.ozone import OzoneFsHook
from airflow.providers.arenadata.ozone.sensors.ozone import OzoneKeySensor, OzoneS3KeySensor
from airflow.providers.arenadata.ozone.utils.errors import OzoneCliError


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
        mock_ozone_hook.assert_called_once()
        assert mock_ozone_hook.call_args.kwargs["ozone_conn_id"] == OzoneFsHook.default_conn_name
        mock_hook_instance.exists.assert_called_once()
        assert mock_hook_instance.exists.call_args.args[0] == "ofs://vol1/bucket1/key1"

    @patch("airflow.providers.arenadata.ozone.sensors.ozone.OzoneFsHook")
    def test_poke_retryable_error_returns_false(self, mock_ozone_hook: MagicMock):
        """Test that poke returns False on retryable OzoneCliError (triggers retry)."""

        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.exists.side_effect = OzoneCliError("Connection reset", retryable=True)

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
        mock_hook_instance.check_for_key.assert_called_once()
        check_args = mock_hook_instance.check_for_key.call_args.args
        assert check_args == ("path/file.txt", "mybucket")

    def test_bucket_name_required_for_plain_key(self):
        """If bucket_name is None and key is not s3:// URL, sensor should fail fast."""

        sensor = OzoneS3KeySensor(
            task_id="test_s3_sensor",
            bucket_name=None,
            bucket_key="plain_key.txt",
        )

        with pytest.raises(AirflowException, match="bucket_name is required"):
            sensor.poke(context={})

    @patch("airflow.providers.arenadata.ozone.sensors.ozone.OzoneS3Hook")
    def test_wildcard_match_uses_metadata_listing(self, mock_ozone_hook: MagicMock):
        """wildcard_match should search metadata and match glob patterns."""
        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.get_file_metadata.return_value = [
            {"Key": "logs/2025-01-01.json"},
            {"Key": "logs/2025-01-01.txt"},
        ]
        sensor = OzoneS3KeySensor(
            task_id="test_s3_sensor_wildcard",
            bucket_name="mybucket",
            bucket_key="logs/*.json",
            wildcard_match=True,
        )
        assert sensor.poke(context={}) is True
        mock_hook_instance.get_file_metadata.assert_called_once()
        metadata_args = mock_hook_instance.get_file_metadata.call_args.args
        assert metadata_args == ("logs/", "mybucket")

    @patch("airflow.providers.arenadata.ozone.sensors.ozone.OzoneS3Hook")
    def test_invalid_regex_pattern_raises(self, mock_ozone_hook: MagicMock):
        """use_regex should fail fast on invalid regular expression."""
        mock_hook_instance = mock_ozone_hook.return_value
        mock_hook_instance.get_file_metadata.return_value = [{"Key": "logs/2025-01-01.json"}]
        sensor = OzoneS3KeySensor(
            task_id="test_s3_sensor_regex",
            bucket_name="mybucket",
            bucket_key="logs/[invalid",
            use_regex=True,
        )
        with pytest.raises(AirflowException, match="Invalid regex pattern"):
            sensor.poke(context={})
