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
import re
import shlex
import subprocess
from functools import cached_property
from pathlib import Path
from tempfile import TemporaryDirectory

from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.arenadata.ozone.utils.common import get_connection_extra, merge_env_overrides
from airflow.providers.arenadata.ozone.utils.security import load_ssl_env_from_connection
from airflow.utils.context import Context  # noqa: TCH001
from airflow.utils.log.secrets_masker import redact

# Get the logger for tenacity to use
log = logging.getLogger(__name__)
INVALID_SCHEMA_CHARS_PATTERN = r"[^a-z0-9_]"


def _run_hive_cli_with_env(
    *,
    hive_hook: HiveCliHook,
    hql: str,
    env: dict[str, str],
    schema: str | None = None,
    verbose: bool = True,
    hive_conf: dict[str, str] | None = None,
) -> str:
    """
    Run Hive CLI using the HiveCliHook command builder, but with an explicit environment.

    This avoids mutating global os.environ (important for multi-task / multi-threaded workers).
    """
    conn = hive_hook.conn
    schema = schema or conn.schema or ""

    invalid_chars_list = re.findall(INVALID_SCHEMA_CHARS_PATTERN, schema)
    if invalid_chars_list:
        invalid_chars = "".join(invalid_chars_list)
        raise RuntimeError(f"The schema `{schema}` contains invalid characters: {invalid_chars}")

    if schema:
        hql = f"USE {schema};\n{hql}"

    with TemporaryDirectory(prefix="airflow_hiveop_") as tmp_dir:
        hql += "\n"
        hql_file = Path(tmp_dir) / "query.hql"
        hql_file.write_text(hql, encoding="utf-8")

        hive_cmd = hive_hook._prepare_cli_cmd()

        # Only extend the hive_conf if it is defined.
        env_context: dict[str, str] = {}
        if hive_conf:
            env_context.update(hive_conf)

        hive_conf_params = hive_hook._prepare_hiveconf(env_context)
        if hive_hook.mapred_queue:
            hive_conf_params.extend(
                [
                    "-hiveconf",
                    f"mapreduce.job.queuename={hive_hook.mapred_queue}",
                    "-hiveconf",
                    f"mapred.job.queue.name={hive_hook.mapred_queue}",
                    "-hiveconf",
                    f"tez.queue.name={hive_hook.mapred_queue}",
                ]
            )

        if hive_hook.mapred_queue_priority:
            hive_conf_params.extend(
                ["-hiveconf", f"mapreduce.job.priority={hive_hook.mapred_queue_priority}"]
            )

        if hive_hook.mapred_job_name:
            hive_conf_params.extend(["-hiveconf", f"mapred.job.name={hive_hook.mapred_job_name}"])

        hive_cmd.extend(hive_conf_params)
        hive_cmd.extend(["-f", str(hql_file)])

        if verbose:
            hive_hook.log.info("%s", shlex.join(hive_cmd))

        sub_process = subprocess.Popen(
            hive_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=tmp_dir,
            close_fds=True,
            env=env,
            text=True,
        )
        hive_hook.sub_process = sub_process

        stdout_lines: list[str] = []
        if sub_process.stdout is None:
            raise AirflowException("Hive CLI subprocess has no stdout")
        for line in sub_process.stdout:
            stdout_lines.append(line)
            if verbose:
                hive_hook.log.info(line.strip())

        sub_process.wait()
        stdout = "".join(stdout_lines)
        if sub_process.returncode:
            raise AirflowException(stdout)

        return stdout


class OzoneToHiveOperator(BaseOperator):
    """
    Register an Ozone path as an external Hive table.

    Useful for making data in Ozone queryable via SQL engines like Hive, Spark, or Presto.
    Supports SSL/TLS configuration via Hive connection Extra:
    - hive_ssl_enabled: Enable SSL/TLS for Hive connections
    - hive_ssl_keystore_path: Path to keystore file
    - hive_ssl_keystore_password: Keystore password
    - hive_ssl_truststore_path: Path to truststore file
    - hive_ssl_truststore_password: Truststore password
    """

    template_fields = ("ozone_path", "table_name", "partition_spec")

    def __init__(
        self,
        *,
        ozone_path: str,
        table_name: str,
        partition_spec: dict[str, str] | None = None,
        hive_cli_conn_id: str = "hive_cli_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ozone_path = ozone_path
        self.table_name = table_name
        # Allow partition_spec to be provided later and validate in execute()
        self.partition_spec = partition_spec or {}
        self.hive_cli_conn_id = hive_cli_conn_id

    @cached_property
    def _hive_ssl_env(self) -> dict[str, str] | None:
        """Load SSL/TLS configuration from Hive connection Extra lazily."""
        try:
            conn = BaseHook.get_connection(self.hive_cli_conn_id)
            return load_ssl_env_from_connection(
                conn,
                conn_id=self.hive_cli_conn_id,
                logger=self.log,
                enabled_flag_keys=("hive_ssl_enabled", "hive.ssl.enabled"),
            )
        except AirflowException as err:
            # Connection might not exist yet, that's OK
            self.log.debug("Could not load Hive SSL configuration (connection may not exist): %s", str(err))
        return None

    def _build_hive_env(self, hive_hook: HiveCliHook) -> dict[str, str]:
        """Build process environment for Hive CLI execution."""
        extra_env = get_connection_extra(hive_hook.conn).get("env", {})
        env = dict(extra_env) if isinstance(extra_env, dict) else {}
        base_env = merge_env_overrides(env)
        if self._hive_ssl_env:
            base_env.update(self._hive_ssl_env)
        return base_env

    @staticmethod
    def _escape_hive_string(value: str) -> str:
        """Escape backslashes and single quotes in interpolated Hive literals."""
        return value.replace("\\", "\\\\").replace("'", "''")

    def _build_partition_hql(self) -> tuple[str, str]:
        """Build escaped partition clause and HQL statement."""
        partition_clause = ", ".join(
            [f"{k}='{self._escape_hive_string(str(v))}'" for k, v in self.partition_spec.items()]
        )
        escaped_location = self._escape_hive_string(str(self.ozone_path))
        hql = (
            f"ALTER TABLE {self.table_name} ADD IF NOT EXISTS PARTITION ({partition_clause}) "
            f"LOCATION '{escaped_location}';"
        )
        return partition_clause, hql

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=60),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(AirflowException),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )
    def _execute_hql_with_retry(self, hive_hook: HiveCliHook, hql: str):
        """
        Execute HQL statement with retry logic.

        Hive operations can fail due to connection issues, metastore load, or transient errors.
        """
        masked_hql = redact(hql)
        self.log.debug("Executing HQL statement (with retry): %s", masked_hql)
        try:
            _run_hive_cli_with_env(hive_hook=hive_hook, hql=hql, env=self._build_hive_env(hive_hook))
            self.log.debug("HQL statement executed successfully")
        except AirflowException as err:
            self.log.warning("HQL execution failed (will retry): %s", str(err))
            raise AirflowException(f"HQL execution failed: {str(err)}") from err

    def execute(self, context: Context):
        """Execute the HQL to add the partition."""
        self.log.info("Starting Ozone to Hive partition registration")
        self.log.info("Table: %s, Ozone path: %s", self.table_name, self.ozone_path)
        self.log.debug("Partition spec: %s", self.partition_spec)
        self.log.debug("Hive CLI connection: %s", self.hive_cli_conn_id)

        if not self.partition_spec:
            self.log.error("Partition specification is required but not provided")
            raise ValueError("Parameter 'partition_spec' is required for partitioning.")

        partition_clause, hql = self._build_partition_hql()

        self.log.info("Executing HQL to register partition")
        masked_hql = redact(hql)
        self.log.debug("HQL statement: %s", masked_hql)

        hive_hook = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id)
        try:
            self._execute_hql_with_retry(hive_hook, hql)
        except AirflowException:
            self.log.error("Failed to register partition in Hive table after retries")
            self.log.error(
                "Table: %s, Partition: %s, Location: %s", self.table_name, partition_clause, self.ozone_path
            )
            raise

        self.log.info("Successfully registered partition in Hive table")
        self.log.info(
            "Table: %s, Partition: %s, Location: %s", self.table_name, partition_clause, self.ozone_path
        )
