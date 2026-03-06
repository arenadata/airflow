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

from airflow.configuration import ensure_secrets_loaded
from airflow.utils.log.secrets_masker import mask_secret

log = logging.getLogger(__name__)


class SecretResolver:
    """Resolve `secret://` references via Airflow Secrets Backend."""

    @classmethod
    def get_secret_value(cls, value: str, conn_id: str | None = None) -> str:
        """
        Resolve secret from Airflow Secrets Backend or return value as-is.

        If ``value`` starts with ``secret://``, each configured backend is queried
        via ``get_config(value)`` and the first non-None value is returned.
        """
        if not value:
            return value

        if not (isinstance(value, str) and value.startswith("secret://")):
            return value

        for backend in ensure_secrets_loaded():
            try:
                secret_value = backend.get_config(value)
            except (AttributeError, TypeError) as err:
                log.error("Secrets Backend returned invalid response for %s: %s", value, str(err))
                raise ValueError(f"Failed to retrieve secret '{value}': {err}") from err

            if secret_value is not None:
                if conn_id:
                    log.debug(
                        "Resolved secret from Secrets Backend for connection %s: %s",
                        conn_id,
                        value.split("/")[-1] if "/" in value else value,
                    )
                mask_secret(secret_value)
                return secret_value
        raise ValueError(f"Secret not found: {value}")

    @classmethod
    def resolve_secret(cls, value: str, conn_id: str | None = None) -> str:
        """Resolve ``secret://...`` values and return plain values unchanged."""
        if isinstance(value, str) and value.startswith("secret://"):
            return cls.get_secret_value(value, conn_id=conn_id)
        return value
