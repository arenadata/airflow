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
"""HBase authentication base classes."""

from __future__ import annotations

import base64
import logging
import os
import subprocess
import tempfile
from abc import ABC, abstractmethod
from typing import Any

from airflow.models import Variable

log = logging.getLogger(__name__)


class HBaseAuthenticator(ABC):  # pylint: disable=too-few-public-methods
    """Base class for HBase authentication methods."""

    @abstractmethod
    def authenticate(self, config: dict[str, Any]) -> dict[str, Any]:
        """
        Perform authentication and return connection kwargs.

        :param config: Connection configuration from extras
        :return: Additional connection kwargs
        """


class SimpleAuthenticator(HBaseAuthenticator):  # pylint: disable=too-few-public-methods
    """Simple authentication (no authentication)."""

    def authenticate(self, config: dict[str, Any]) -> dict[str, Any]:
        """No authentication needed."""
        return {}


class KerberosAuthenticator(HBaseAuthenticator):  # pylint: disable=too-few-public-methods
    """Kerberos authentication using kinit."""

    def authenticate(self, config: dict[str, Any]) -> dict[str, Any]:
        """Perform Kerberos authentication via kinit."""
        principal = config.get("principal")
        if not principal:
            raise ValueError("Kerberos principal is required when auth_method=kerberos")

        # Get keytab from secrets backend or file
        keytab_secret_key = config.get("keytab_secret_key")
        keytab_path = config.get("keytab_path")

        if keytab_secret_key:
            # Get keytab from Airflow secrets backend
            keytab_content = self._get_secret(keytab_secret_key)
            if not keytab_content:
                raise ValueError(f"Keytab not found in secrets backend: {keytab_secret_key}")

            # Create temporary keytab file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".keytab") as f:
                keytab_bytes: bytes
                if isinstance(keytab_content, str):
                    # Assume base64 encoded
                    keytab_bytes = base64.b64decode(keytab_content)
                else:
                    keytab_bytes = keytab_content
                f.write(keytab_bytes)
                keytab_path = f.name

        if not keytab_path or not os.path.exists(keytab_path):
            raise ValueError(f"Keytab file not found: {keytab_path}")

        # Perform kinit
        try:
            cmd = ["kinit", "-kt", keytab_path, principal]
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            # Log success but don't expose sensitive info
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Kerberos authentication failed: {e.stderr}") from e
        finally:
            # Clean up temporary keytab file if created
            if keytab_secret_key and keytab_path:
                try:
                    os.unlink(keytab_path)
                except OSError as e:
                    log.warning("Failed to cleanup temporary keytab file: %s", e)

        return {}  # kinit handles authentication, use default transport

    def _get_secret(self, secret_key: str) -> str | None:
        """Get secret from Airflow secrets backend."""
        try:
            return Variable.get(secret_key, default_var=None)
        except KeyError:
            # Fallback to environment variable
            return os.environ.get(secret_key)
