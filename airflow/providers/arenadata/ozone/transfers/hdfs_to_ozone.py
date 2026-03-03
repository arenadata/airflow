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
import shlex
import subprocess
from functools import cached_property

from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.utils.common import (
    require_optional_non_empty,
    run_subprocess,
)
from airflow.providers.arenadata.ozone.utils.security import (
    load_ssl_env_from_connection,
)
from airflow.utils.log.secrets_masker import redact

# Get the logger for tenacity to use
log = logging.getLogger(__name__)
DISTCP_BASE_COMMAND = ["hadoop", "distcp", "-update", "-skipcrccheck"]


class HdfsToOzoneOperator(BaseOperator):
    """
    Highly efficient HDFS to Ozone migration using DistCp.

    Essential for ADH 4.1.0 environments. Supports SSL/TLS configuration via HDFS connection Extra:
    - hdfs_ssl_enabled: Enable SSL/TLS for HDFS connections
    - dfs_encrypt_data_transfer: Enable data transfer encryption
    - hdfs_ssl_keystore_location: Path to keystore file
    - hdfs_ssl_keystore_password: Keystore password
    - hdfs_ssl_truststore_location: Path to truststore file
    - hdfs_ssl_truststore_password: Truststore password
    """

    def __init__(self, source_path: str, dest_path: str, hdfs_conn_id: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.source_path = source_path
        self.dest_path = dest_path
        self.hdfs_conn_id = require_optional_non_empty(
            hdfs_conn_id, "hdfs_conn_id parameter cannot be an empty string (use None instead)"
        )

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
            return load_ssl_env_from_connection(
                conn,
                conn_id=self.hdfs_conn_id,
                logger=self.log,
                enabled_flag_keys=("hdfs_ssl_enabled", "dfs.encrypt.data.transfer"),
            )
        except AirflowException as err:
            # Connection might not exist yet, that's OK
            self.log.debug("Could not load HDFS SSL configuration (connection may not exist): %s", str(err))
        return None

    @retry(
        wait=wait_exponential(multiplier=3, min=5, max=120),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(AirflowException),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )
    def _execute_distcp(self, cmd, env: dict | None = None):
        """
        Execute DistCp command with retry logic.

        DistCp operations can fail due to network issues, cluster load, or transient errors.

        :param cmd: Command to execute
        :param env: Optional environment variables dictionary
        """
        masked_cmd = redact(shlex.join(cmd))
        self.log.debug("Executing DistCp command (with retry): %s", masked_cmd)
        try:
            run_subprocess(
                cmd,
                env_overrides=env,
                check=True,
            )
            self.log.debug("DistCp command completed successfully")
        except subprocess.CalledProcessError as e:
            raw_error = e.stderr if e.stderr else str(e)
            masked_error = redact(raw_error)
            self.log.warning("DistCp command failed (will retry): %s", masked_error)
            raise AirflowException(f"DistCp command failed: {masked_error}")

    def execute(self, context):
        self.log.info("Starting HDFS to Ozone data migration using DistCp")
        self.log.info("Source path (HDFS): %s", self.source_path)
        self.log.info("Destination path (Ozone): %s", self.dest_path)
        self.log.debug("DistCp options: -update (incremental), -skipcrccheck (skip CRC verification)")

        cmd = [*DISTCP_BASE_COMMAND, self.source_path, self.dest_path]
        masked_cmd = redact(shlex.join(cmd))
        self.log.debug("Executing DistCp command: %s", masked_cmd)

        if self._hdfs_ssl_env:
            self.log.debug("Applying SSL environment variables for HDFS DistCp")

        self._execute_distcp(cmd, env=self._hdfs_ssl_env)
        self.log.info("Successfully completed HDFS to Ozone migration")
        self.log.info("Source: %s -> Destination: %s", self.source_path, self.dest_path)
