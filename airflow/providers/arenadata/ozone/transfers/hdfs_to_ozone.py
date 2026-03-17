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

import logging
import shutil
from functools import cached_property

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.hooks.ozone import (
    RETRY_ATTEMPTS,
    SLOW_TIMEOUT_SECONDS,
)
from airflow.providers.arenadata.ozone.utils.cli_runner import CliRunner
from airflow.providers.arenadata.ozone.utils.helpers import TypeNormalizationHelper
from airflow.providers.arenadata.ozone.utils.security import (
    SSLConfig,
)

log = logging.getLogger(__name__)
DISTCP_BASE_COMMAND = ["hadoop", "distcp", "-update", "-skipcrccheck"]


class HdfsToOzoneOperator(BaseOperator):
    """
    Migrate data from HDFS to Ozone using DistCp.

    Supports SSL/TLS configuration via HDFS connection Extra:
    - hdfs_ssl_enabled: Enable SSL/TLS for HDFS connections
    - dfs_encrypt_data_transfer: Enable data transfer encryption
    - hdfs_ssl_keystore_location: Path to keystore file
    - hdfs_ssl_keystore_password: Keystore password
    - hdfs_ssl_truststore_location: Path to truststore file
    - hdfs_ssl_truststore_password: Truststore password
    """

    def __init__(
        self,
        source_path: str,
        dest_path: str,
        hdfs_conn_id: str | None = None,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_path = source_path
        self.dest_path = dest_path
        self.hdfs_conn_id = TypeNormalizationHelper.require_optional_non_empty(
            hdfs_conn_id, "hdfs_conn_id parameter cannot be an empty string (use None instead)"
        )
        self.retry_attempts = retry_attempts
        self.timeout = timeout

        self.log.debug(
            "Initializing HdfsToOzoneOperator - source: %s, destination: %s", self.source_path, self.dest_path
        )

    @cached_property
    def _hdfs_ssl_env(self) -> dict[str, str] | None:
        """Load SSL/TLS configuration from HDFS connection Extra lazily."""
        if not self.hdfs_conn_id:
            return None
        try:
            conn = BaseHook.get_connection(self.hdfs_conn_id)
            return SSLConfig.load_from_connection(
                conn,
                conn_id=self.hdfs_conn_id,
                enabled_flag_keys=("hdfs_ssl_enabled", "dfs.encrypt.data.transfer"),
            )
        except AirflowException as err:
            # Connection might not exist yet, that's OK
            self.log.debug("Could not load HDFS SSL configuration (connection may not exist): %s", str(err))
        return None

    def _build_distcp_command(self) -> list[str]:
        """Build the DistCp command for the current transfer."""
        return [*DISTCP_BASE_COMMAND, self.source_path, self.dest_path]

    def _validate_runtime_inputs(self) -> None:
        """Validate operator inputs right before DistCp execution."""
        if not isinstance(self.source_path, str):
            raise AirflowException("HdfsToOzoneOperator requires source_path to be a string")
        if not self.source_path.strip():
            raise AirflowException("HdfsToOzoneOperator requires non-empty source_path")

        if not isinstance(self.dest_path, str):
            raise AirflowException("HdfsToOzoneOperator requires dest_path to be a string")
        if not self.dest_path.strip():
            raise AirflowException("HdfsToOzoneOperator requires non-empty dest_path")

    def _validate_hadoop_runtime(self) -> str:
        """Check that the Hadoop DistCp runtime binary is available."""
        hadoop_bin = shutil.which("hadoop")
        if not hadoop_bin:
            raise AirflowException(
                "HdfsToOzoneOperator is loaded, but Hadoop DistCp runtime is unavailable: "
                "executable 'hadoop' was not found in PATH. "
                "Install Hadoop client tools or provide 'hadoop' in PATH on the worker that runs this task"
            )
        return hadoop_bin

    def _validate_runtime_dependencies(self) -> None:
        """Run fail-first runtime validation for DistCp prerequisites."""
        self._validate_runtime_inputs()
        self._validate_hadoop_runtime()

    def execute(self, context):
        self.log.info("Starting DistCp migration: %s -> %s", self.source_path, self.dest_path)

        self._validate_runtime_dependencies()
        cmd = self._build_distcp_command()

        if self._hdfs_ssl_env:
            self.log.debug("Applying SSL environment variables for HDFS DistCp")

        CliRunner.run_process(
            cmd,
            env_overrides=self._hdfs_ssl_env,
            timeout=self.timeout,
            retry_attempts=self.retry_attempts,
            check=True,
            log_output=True,
        )
        self.log.info("DistCp migration completed")
