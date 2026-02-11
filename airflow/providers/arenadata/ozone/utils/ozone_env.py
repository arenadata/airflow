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
import shutil
import tempfile
import xml.sax.saxutils as saxutils
from dataclasses import dataclass
from typing import Callable

from airflow.providers.arenadata.ozone.utils.security import (
    apply_kerberos_env_vars,
    apply_ssl_env_vars,
    get_kerberos_env_vars,
    get_ssl_env_vars,
    kinit_from_env_vars,
)


@dataclass(frozen=True)
class OzoneConnSnapshot:
    """Lightweight connection snapshot to reduce repeated connection reads."""

    host: str
    port: int
    extra: dict


class OzoneEnv:
    """
    SSL/Kerberos + environment builder for Ozone CLI.

    Responsibilities:
    - Lazy load SSL/TLS and Kerberos configuration from Airflow connection Extra.
    - Generate minimal ozone-site.xml/core-site.xml (optional) if ozone_scm_address is provided.
    - Build subprocess environment for `ozone` CLI (OZONE_OM_ADDRESS, OZONE_CONF_DIR, SSL, Kerberos).

    This module intentionally does NOT run ozone CLI commands and does NOT implement retries.
    That logic stays in OzoneHook.
    """

    def __init__(
        self,
        *,
        conn_id: str,
        get_connection: Callable[[str], object],
        logger: logging.Logger,
    ):
        self.conn_id = conn_id
        self._get_connection = get_connection
        self.log = logger

        self._ssl_env: dict[str, str] | None = None
        self._kerberos_env: dict[str, str] | None = None
        self._ssl_loaded = False
        self._kerberos_loaded = False

        self._client_conf_dir: str | None = None
        self._conn_snapshot: OzoneConnSnapshot | None = None

    def get_connection_snapshot(self) -> OzoneConnSnapshot:
        if self._conn_snapshot is not None:
            return self._conn_snapshot

        conn = self._get_connection(self.conn_id)
        extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}

        host = str(getattr(conn, "host", None) or "localhost")
        port = int(getattr(conn, "port", None) or 9862)

        self._conn_snapshot = OzoneConnSnapshot(host=host, port=port, extra=extra)
        return self._conn_snapshot

    def load_ssl(self) -> None:
        """Load SSL/TLS configuration from connection Extra (lazy)."""
        if self._ssl_loaded:
            return

        try:
            snap = self.get_connection_snapshot()
            ssl_env_vars = get_ssl_env_vars(snap.extra, conn_id=self.conn_id)
            if ssl_env_vars:
                self._ssl_env = apply_ssl_env_vars(ssl_env_vars)
                self.log.debug("SSL/TLS configuration loaded from connection: %s", list(ssl_env_vars.keys()))
                if (
                    snap.extra.get("ozone_security_enabled") == "true"
                    or snap.extra.get("ozone.security.enabled") == "true"
                ):
                    self.log.debug("SSL/TLS enabled for Ozone Native CLI")
            else:
                self.log.debug("No SSL/TLS configuration found in connection Extra")
        except Exception as e:
            self.log.debug("Could not load SSL configuration (connection may not exist): %s", str(e))
        finally:
            self._ssl_loaded = True

    def load_kerberos(self) -> None:
        """Load Kerberos configuration from connection Extra (lazy)."""
        if self._kerberos_loaded:
            return

        try:
            snap = self.get_connection_snapshot()
            kerberos_env_vars = get_kerberos_env_vars(snap.extra, conn_id=self.conn_id)
            if kerberos_env_vars:
                # Side-effect is explicit and happens exactly once (lazy) during Kerberos load.
                kinit_from_env_vars(kerberos_env_vars, existing_env=os.environ)

                # Build process env overrides (pure).
                self._kerberos_env = apply_kerberos_env_vars(kerberos_env_vars)

                self.log.debug(
                    "Kerberos configuration loaded from connection: %s",
                    list(kerberos_env_vars.keys()),
                )
                if snap.extra.get("hadoop_security_authentication") == "kerberos":
                    self.log.debug("Kerberos authentication enabled for Ozone Native CLI")
            else:
                self.log.debug("No Kerberos configuration found in connection Extra")
        except Exception as e:
            self.log.debug("Could not load Kerberos configuration (connection may not exist): %s", str(e))
        finally:
            self._kerberos_loaded = True

    def ensure_loaded(self) -> None:
        """Load SSL/Kerberos config lazily (first command execution)."""
        self.load_ssl()
        self.load_kerberos()

    @property
    def ssl_env(self) -> dict[str, str] | None:
        return self._ssl_env

    @property
    def kerberos_env(self) -> dict[str, str] | None:
        return self._kerberos_env

    def kerberos_enabled(self) -> bool:
        """
        Return True if Kerberos is effectively enabled.

        We treat presence of kerberos env + explicit HADOOP_SECURITY_AUTHENTICATION=kerberos as enabled.
        """
        if not self._kerberos_env:
            return False
        return str(self._kerberos_env.get("HADOOP_SECURITY_AUTHENTICATION", "")).lower() == "kerberos"

    def get_config_dir(self) -> str | None:
        """Return Kerberos config directory (OZONE_CONF_DIR/HADOOP_CONF_DIR) from kerberos env."""
        if not self._kerberos_env:
            return None
        conf_dir = self._kerberos_env.get("OZONE_CONF_DIR") or self._kerberos_env.get("HADOOP_CONF_DIR")
        return conf_dir or None

    def get_or_create_client_conf_dir(self) -> str | None:
        """
        If connection Extra has Ozone client addresses, build minimal ozone-site.xml and return temp dir.

        Extra keys:
        - ozone_scm_address: required to avoid CLI hanging in some environments
        - ozone_recon_address: optional
        """
        if self._client_conf_dir is not None:
            return self._client_conf_dir

        try:
            snap = self.get_connection_snapshot()
        except Exception as e:
            self.log.debug("Could not get connection for client conf: %s", e)
            return None

        ozone_scm_address = snap.extra.get("ozone_scm_address")
        if not ozone_scm_address:
            return None

        tmpdir = tempfile.mkdtemp(prefix="airflow_ozone_conf_")
        try:
            om_addr = f"{snap.host}:{snap.port}"
            scm_addr = str(ozone_scm_address or "scm").strip()
            recon_addr = str(snap.extra.get("ozone_recon_address") or "").strip()

            om_esc = saxutils.escape(om_addr)
            scm_esc = saxutils.escape(scm_addr)
            recon_esc = saxutils.escape(recon_addr) if recon_addr else ""

            lines = [
                '<?xml version="1.0"?>',
                "<configuration>",
                f"  <property><name>ozone.om.address</name><value>{om_esc}</value></property>",
                f"  <property><name>ozone.scm.client.address</name><value>{scm_esc}</value></property>",
                f"  <property><name>ozone.scm.block.client.address</name><value>{scm_esc}</value></property>",
            ]
            if recon_esc:
                lines.append(
                    f"  <property><name>ozone.recon.address</name><value>{recon_esc}</value></property>"
                )
            lines.append("</configuration>")

            with open(os.path.join(tmpdir, "ozone-site.xml"), "w") as f:
                f.write("\n".join(lines))

            with open(os.path.join(tmpdir, "core-site.xml"), "w") as f:
                f.write('<?xml version="1.0"?>\n<configuration>\n</configuration>\n')

            self.log.debug("Generated minimal ozone-site.xml (om=%s, scm=%s)", om_addr, scm_addr)

            self._client_conf_dir = tmpdir
            return tmpdir
        except Exception as e:
            self.log.warning("Failed to create Ozone client conf from Extra: %s", e)
            try:
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass
            return None

    def build_env(self) -> dict[str, str]:
        """
        Build subprocess env for Ozone CLI.

        - OZONE_OM_ADDRESS from connection
        - OZONE_CONF_DIR from generated minimal conf (optional)
        - SSL env overrides (optional)
        - Kerberos env overrides (optional)
        """
        env = os.environ.copy()

        try:
            snap = self.get_connection_snapshot()
            env["OZONE_OM_ADDRESS"] = f"{snap.host}:{snap.port}"
        except Exception as e:
            self.log.debug("Could not set OZONE_OM_ADDRESS from connection: %s", e)

        conf_dir = self.get_or_create_client_conf_dir()
        if conf_dir:
            env["OZONE_CONF_DIR"] = conf_dir

        if self._ssl_env:
            env.update(self._ssl_env)
        if self._kerberos_env:
            env.update(self._kerberos_env)

        return env
