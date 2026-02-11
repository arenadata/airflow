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
import os

from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.utils.security import (
    apply_ssl_env_vars,
    get_ssl_env_vars,
)
from airflow.utils.log.secrets_masker import redact

# Get the logger for tenacity to use
log = logging.getLogger(__name__)


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
        if not source_path or not source_path.strip():
            raise ValueError("source_path parameter cannot be empty")
        if not dest_path or not dest_path.strip():
            raise ValueError("dest_path parameter cannot be empty")
        if hdfs_conn_id is not None and not str(hdfs_conn_id).strip():
            raise ValueError("hdfs_conn_id parameter cannot be an empty string (use None instead)")

        self.source_path = source_path.strip()
        self.dest_path = dest_path.strip()
        self.hdfs_conn_id = hdfs_conn_id.strip() if isinstance(hdfs_conn_id, str) else hdfs_conn_id
        self._hdfs_ssl_env = None

        self.log.debug(
            "Initializing HdfsToOzoneOperator - source: %s, destination: %s", self.source_path, self.dest_path
        )

        if self.hdfs_conn_id:
            self._load_hdfs_ssl_config()

    def _load_hdfs_ssl_config(self):
        """Load SSL/TLS configuration from HDFS connection Extra."""
        try:
            hook = BaseHook()
            conn = hook.get_connection(self.hdfs_conn_id)
            extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}

            ssl_env_vars = get_ssl_env_vars(extra, conn_id=self.hdfs_conn_id)
            if ssl_env_vars:
                self._hdfs_ssl_env = apply_ssl_env_vars(ssl_env_vars)
                self.log.debug(
                    "SSL/TLS configuration loaded from HDFS connection: %s", list(ssl_env_vars.keys())
                )
                if (
                    extra.get("hdfs_ssl_enabled") == "true"
                    or extra.get("dfs.encrypt.data.transfer") == "true"
                ):
                    self.log.info("SSL/TLS enabled for HDFS connections")
            else:
                self.log.debug("No SSL/TLS configuration found in HDFS connection Extra")
        except Exception as e:
            # Connection might not exist yet, that's OK
            self.log.debug("Could not load HDFS SSL configuration (connection may not exist): %s", str(e))

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
        masked_cmd = redact(" ".join(cmd))
        self.log.debug("Executing DistCp command (with retry): %s", masked_cmd)
        try:
            import subprocess

            if env:
                # Merge with current environment
                full_env = os.environ.copy()
                full_env.update(env)
                subprocess.run(cmd, env=full_env, check=True, capture_output=True, text=True)
            else:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.log.debug("DistCp command completed successfully")
        except subprocess.CalledProcessError as e:
            raw_error = e.stderr if e.stderr else str(e)
            masked_error = redact(raw_error)
            self.log.warning("DistCp command failed (will retry): %s", masked_error)
            raise AirflowException(f"DistCp command failed: {masked_error}")
        except Exception as e:
            raw_error = str(e)
            masked_error = redact(raw_error)
            self.log.warning("DistCp command failed (will retry): %s", masked_error)
            raise AirflowException(f"DistCp command failed: {masked_error}")

    def execute(self, context):
        self.log.info("Starting HDFS to Ozone data migration using DistCp")
        self.log.info("Source path (HDFS): %s", self.source_path)
        self.log.info("Destination path (Ozone): %s", self.dest_path)
        self.log.debug("DistCp options: -update (incremental), -skipcrccheck (skip CRC verification)")

        cmd = ["hadoop", "distcp", "-update", "-skipcrccheck", self.source_path, self.dest_path]
        masked_cmd = redact(" ".join(cmd))
        self.log.debug("Executing DistCp command: %s", masked_cmd)

        try:
            if self._hdfs_ssl_env:
                self.log.debug("Applying SSL environment variables for HDFS DistCp")

            self._execute_distcp(cmd, env=self._hdfs_ssl_env)
            self.log.info("Successfully completed HDFS to Ozone migration")
            self.log.info("Source: %s -> Destination: %s", self.source_path, self.dest_path)
        except Exception as e:
            self.log.error("HDFS to Ozone migration failed after retries")
            self.log.error("Source: %s, Destination: %s", self.source_path, self.dest_path)
            self.log.error("Error: %s", str(e))
            raise
