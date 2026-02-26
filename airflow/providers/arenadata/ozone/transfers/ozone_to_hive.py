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
from typing import TYPE_CHECKING

from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.arenadata.ozone.utils.security import apply_ssl_env_vars, get_ssl_env_vars
from airflow.utils.log.secrets_masker import redact

if TYPE_CHECKING:
    from airflow.utils.context import Context

try:
    # Import lazily to avoid breaking DAG parsing when the Hive provider
    # is not installed in the Airflow environment. The operator will still
    # fail at runtime with a clear error if used without the dependency.
    from airflow.providers.apache.hive.hooks.hive import HiveCliHook  # type: ignore[attr-defined]
except ModuleNotFoundError as exc:  # pragma: no cover - environment dependent
    HiveCliHook = None  # type: ignore[assignment]
    _hive_import_error = exc
else:
    _hive_import_error = None


# Get the logger for tenacity to use
log = logging.getLogger(__name__)


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
    # Lazy import to avoid adding hard dependency when hive provider isn't installed.
    import re
    import subprocess
    from tempfile import NamedTemporaryFile, TemporaryDirectory

    conn = hive_hook.conn
    schema = schema or conn.schema or ""

    invalid_chars_list = re.findall(r"[^a-z0-9_]", schema)
    if invalid_chars_list:
        invalid_chars = "".join(invalid_chars_list)
        raise RuntimeError(f"The schema `{schema}` contains invalid characters: {invalid_chars}")

    if schema:
        hql = f"USE {schema};\n{hql}"

    with TemporaryDirectory(prefix="airflow_hiveop_") as tmp_dir, NamedTemporaryFile(dir=tmp_dir) as f:
        hql += "\n"
        f.write(hql.encode("UTF-8"))
        f.flush()

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
        hive_cmd.extend(["-f", f.name])

        if verbose:
            hive_hook.log.info("%s", " ".join(hive_cmd))

        sub_process = subprocess.Popen(
            hive_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=tmp_dir,
            close_fds=True,
            env=env,
        )
        hive_hook.sub_process = sub_process

        stdout = ""
        for line_raw in iter(sub_process.stdout.readline, b""):
            line = line_raw.decode()
            stdout += line
            if verbose:
                hive_hook.log.info(line.strip())

        sub_process.wait()
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
        if HiveCliHook is None:
            raise ImportError(
                "HiveCliHook not available. Install apache-airflow-providers-apache-hive"
            ) from _hive_import_error

        super().__init__(**kwargs)
        self.ozone_path = ozone_path
        self.table_name = table_name
        # Allow partition_spec to be provided later and validate in execute()
        self.partition_spec = partition_spec or {}
        self.hive_cli_conn_id = hive_cli_conn_id
        self._hive_ssl_env = None
        self._load_hive_ssl_config()

    def _load_hive_ssl_config(self):
        """Load SSL/TLS configuration from Hive connection Extra."""
        try:
            hook = BaseHook()
            conn = hook.get_connection(self.hive_cli_conn_id)
            extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}

            ssl_env_vars = get_ssl_env_vars(extra, conn_id=self.hive_cli_conn_id)
            if ssl_env_vars:
                self._hive_ssl_env = apply_ssl_env_vars(ssl_env_vars)
                self.log.debug(
                    "SSL/TLS configuration loaded from Hive connection: %s", list(ssl_env_vars.keys())
                )
                if extra.get("hive_ssl_enabled") == "true" or extra.get("hive.ssl.enabled") == "true":
                    self.log.info("SSL/TLS enabled for Hive connections")
            else:
                self.log.debug("No SSL/TLS configuration found in Hive connection Extra")
        except Exception as e:
            # Connection might not exist yet, that's OK
            self.log.debug("Could not load Hive SSL configuration (connection may not exist): %s", str(e))

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
            env = dict(hive_hook.conn.extra_dejson.get("env", {})) if hasattr(hive_hook, "conn") else {}
            base_env = os.environ.copy()
            if env:
                base_env.update({str(k): str(v) for k, v in env.items()})
            if self._hive_ssl_env:
                base_env.update(self._hive_ssl_env)

            _run_hive_cli_with_env(hive_hook=hive_hook, hql=hql, env=base_env)
            self.log.debug("HQL statement executed successfully")
        except Exception as e:
            self.log.warning("HQL execution failed (will retry): %s", str(e))
            raise AirflowException(f"HQL execution failed: {str(e)}")

    def execute(self, context: Context):
        """Execute the HQL to add the partition."""
        self.log.info("Starting Ozone to Hive partition registration")
        self.log.info("Table: %s, Ozone path: %s", self.table_name, self.ozone_path)
        self.log.debug("Partition spec: %s", self.partition_spec)
        self.log.debug("Hive CLI connection: %s", self.hive_cli_conn_id)

        if not self.partition_spec:
            self.log.error("Partition specification is required but not provided")
            raise ValueError("Parameter 'partition_spec' is required for partitioning.")

        def _escape_hive_string(value: str) -> str:
            # Basic escaping for Hive string literals: escape backslashes and single quotes.
            # This mirrors the common pattern in other providers where user input is interpolated
            # into SQL strings.
            return value.replace("\\", "\\\\").replace("'", "''")

        partition_clause = ", ".join(
            [f"{k}='{_escape_hive_string(str(v))}'" for k, v in self.partition_spec.items()]
        )
        escaped_location = _escape_hive_string(str(self.ozone_path))
        hql = (
            f"ALTER TABLE {self.table_name} ADD IF NOT EXISTS PARTITION ({partition_clause}) "
            f"LOCATION '{escaped_location}';"
        )

        self.log.info("Executing HQL to register partition")
        masked_hql = redact(hql)
        self.log.debug("HQL statement: %s", masked_hql)

        # Fail fast with a clear message if Hive provider is missing
        if HiveCliHook is None:
            raise AirflowException(
                "HiveCliHook could not be imported. "
                "Please install the 'apache-airflow-providers-apache-hive' package "
                "in your Airflow environment to use OzoneToHiveOperator."
            ) from _hive_import_error

        try:
            hive_hook = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id)
            self._execute_hql_with_retry(hive_hook, hql)
            self.log.info("Successfully registered partition in Hive table")
            self.log.info(
                "Table: %s, Partition: %s, Location: %s", self.table_name, partition_clause, self.ozone_path
            )
        except Exception as e:
            self.log.error("Failed to register partition in Hive table after retries")
            self.log.error(
                "Table: %s, Partition: %s, Location: %s", self.table_name, partition_clause, self.ozone_path
            )
            self.log.error("Error: %s", str(e))
            raise
