#
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
"""HBase Administration Hook for backup/restore operations."""

from __future__ import annotations

import logging
import os
import re
import socket
import subprocess
from typing import TYPE_CHECKING

from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models import Connection

logger = logging.getLogger(__name__)


class HBaseCLIHook(BaseHook):
    """
    Hook for HBase administrative operations (backup/restore).

    This hook will use HBase Admin client API for backup/restore operations.
    Currently not implemented - placeholder for future implementation.

    :param hbase_conn_id: Connection ID for HBase.
    """

    conn_name_attr = "hbase_conn_id"
    default_conn_name = "hbase_default"
    conn_type = "hbase"
    hook_name = "HBase Administration"

    def __init__(
        self,
        hbase_conn_id: str = default_conn_name,
        hbase_cmd: str | None = None,
        java_home: str | None = None,
        hbase_home: str | None = None,
    ) -> None:
        super().__init__()
        self.hbase_conn_id = hbase_conn_id
        self.hbase_cmd = hbase_cmd or os.getenv('HBASE_CMD', 'hbase')
        self.java_home = java_home or os.getenv('JAVA_HOME', '/usr/lib/jvm/java-arenadata-openjdk-8')
        self.hbase_home = hbase_home or os.getenv('HBASE_HOME', '/usr/lib/hbase')
        self._connection: Connection | None = None

    def get_conn(self) -> Connection:
        """Get HBase connection."""
        if self._connection is None:
            self._connection = self.get_connection(self.hbase_conn_id)
        return self._connection

    def _execute_hbase_command(self, command: str) -> str:
        """Execute HBase CLI command.

        :param command: HBase command to execute (e.g., "backup create full /backup -t table1")
        :return: Command output
        """
        # Get connection for Kerberos settings
        conn = self.get_conn()
        extra = conn.extra_dejson if conn.extra else {}
        
        # Check if Kerberos authentication is needed
        kerberos_keytab = extra.get('kerberos_keytab')
        
        # Use hbase_home to construct full command path
        hbase_command = f"{self.hbase_home}/bin/{self.hbase_cmd} {command}"
        
        # Run as hbase user if Kerberos is enabled (hbase user has access to WALs)
        if kerberos_keytab:
            # Get hbase service keytab and principal from connection extra
            hbase_keytab = extra.get('hbase_service_keytab', '/etc/security/keytabs/hbase.service.keytab')
            hbase_principal = extra.get('hbase_service_principal')
            
            if not hbase_principal:
                # Construct default principal if not provided
                hostname = socket.getfqdn()
                realm = extra.get('kerberos_realm', 'KRB5-TEST')
                hbase_principal = f"hbase/{hostname}@{realm}"
            
            # Do kinit as hbase user, then run command
            kinit_cmd = f"sudo -u hbase kinit -kt {hbase_keytab} {hbase_principal}"
            full_command = f"{kinit_cmd} && sudo -u hbase {hbase_command}"
            logger.info(f"Running as hbase user with Kerberos: {hbase_principal}")
        else:
            full_command = hbase_command

        # Set environment variables
        env = os.environ.copy()
        env['JAVA_HOME'] = self.java_home

        logger.info(f"Executing HBase command: {self._mask_sensitive(full_command)}")
        logger.info(f"Using JAVA_HOME: {self.java_home}")
        logger.info(f"Using HBASE_HOME: {self.hbase_home}")

        try:
            result = subprocess.run(
                full_command,
                shell=True,
                capture_output=True,
                text=True,
                check=True,
                env=env
            )
            logger.info(f"Command completed successfully")
            # Filter out SLF4J warnings from output
            output = self._filter_slf4j_warnings(result.stdout)
            return output
        except subprocess.CalledProcessError as e:
            # Filter SLF4J warnings from stderr to show real error
            stderr_filtered = self._filter_slf4j_warnings(e.stderr)
            stdout_filtered = self._filter_slf4j_warnings(e.stdout)

            error_msg = stderr_filtered or stdout_filtered or "Unknown error"
            logger.error(f"Command failed with exit code {e.returncode}")
            logger.error(f"Error output: {error_msg}")

            raise RuntimeError(f"HBase command failed (exit code {e.returncode}): {error_msg}") from e

    def _filter_slf4j_warnings(self, text: str) -> str:
        """Filter out SLF4J warnings from output."""
        if not text:
            return text

        lines = text.split('\n')
        filtered_lines = [
            line for line in lines
            if not line.startswith('SLF4J:')
        ]
        return '\n'.join(filtered_lines).strip()

    def _mask_sensitive(self, text: str) -> str:
        """Mask sensitive information in logs."""
        # Mask potential paths that might contain sensitive info
        text = re.sub(r'(/[\w/.-]*\.keytab)', '***KEYTAB***', text)
        return text

    def create_backup_set(self, backup_set_name: str, tables: list[str]) -> str:
        """
        Create backup set.

        :param backup_set_name: Name of the backup set.
        :param tables: List of tables to include in backup set.
        :return: Result message.
        """
        tables_str = ",".join(tables)
        command = f"backup set add {backup_set_name} {tables_str}"
        return self._execute_hbase_command(command)

    def list_backup_sets(self) -> str:
        """
        List all backup sets.

        :return: List of backup sets.
        """
        command = "backup set list"
        return self._execute_hbase_command(command)

    def create_full_backup(
        self,
        backup_root: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int | None = None,
    ) -> str:
        """
        Create full backup.

        :param backup_root: Root directory for backup.
        :param backup_set_name: Name of backup set to backup.
        :param tables: List of tables to backup (alternative to backup_set_name).
        :param workers: Number of workers for backup operation.
        :return: Backup ID.
        """
        command = f"backup create full {backup_root}"

        if backup_set_name:
            command += f" -s {backup_set_name}"
        elif tables:
            tables_str = ",".join(tables)
            command += f" -t {tables_str}"
        else:
            raise ValueError("Either backup_set_name or tables must be provided")

        if workers:
            command += f" -w {workers}"

        return self._execute_hbase_command(command)

    def create_incremental_backup(
        self,
        backup_root: str,
        backup_set_name: str | None = None,
        tables: list[str] | None = None,
        workers: int | None = None,
    ) -> str:
        """
        Create incremental backup.

        :param backup_root: Root directory for backup.
        :param backup_set_name: Name of backup set to backup.
        :param tables: List of tables to backup (alternative to backup_set_name).
        :param workers: Number of workers for backup operation.
        :return: Backup ID.
        """
        command = f"backup create incremental {backup_root}"

        if backup_set_name:
            command += f" -s {backup_set_name}"
        elif tables:
            tables_str = ",".join(tables)
            command += f" -t {tables_str}"
        else:
            raise ValueError("Either backup_set_name or tables must be provided")

        if workers:
            command += f" -w {workers}"

        return self._execute_hbase_command(command)

    def get_backup_history(self, backup_set_name: str | None = None) -> str:
        """
        Get backup history.

        :param backup_set_name: Name of backup set (optional).
        :return: Backup history.
        """
        command = "backup history"
        if backup_set_name:
            command += f" -s {backup_set_name}"
        return self._execute_hbase_command(command)

    def describe_backup(self, backup_id: str) -> str:
        """
        Describe backup.

        :param backup_id: Backup ID.
        :return: Backup description.
        """
        command = f"backup describe {backup_id}"
        return self._execute_hbase_command(command)

    def restore_backup(
        self,
        backup_root: str,
        backup_id: str,
        tables: list[str] | None = None,
        overwrite: bool = False,
    ) -> str:
        """
        Restore backup.

        :param backup_root: Root directory where backup is stored.
        :param backup_id: Backup ID to restore.
        :param tables: List of tables to restore (optional).
        :param overwrite: Whether to overwrite existing tables.
        :return: Result message.
        """
        command = f"restore {backup_root} {backup_id}"

        if tables:
            tables_str = ",".join(tables)
            command += f" -t {tables_str}"

        if overwrite:
            command += " -o"

        return self._execute_hbase_command(command)

    def execute_command(self, command: str) -> str:
        """
        Execute arbitrary HBase CLI command.

        This method allows executing any HBase CLI command that is not wrapped
        in a specialized method.

        :param command: HBase command to execute (e.g., "backup create full /backup -t table1")
        :return: Command output
        """
        return self._execute_hbase_command(command)
