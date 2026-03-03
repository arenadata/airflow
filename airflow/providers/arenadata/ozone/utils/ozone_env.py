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

import atexit
import contextlib
import logging
import os
import shutil
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Callable

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.utils.common import get_connection_extra, merge_env_overrides
from airflow.providers.arenadata.ozone.utils.security import (
    apply_kerberos_env_vars,
    get_kerberos_env_vars,
    kinit_from_env_vars,
    load_ssl_env_from_connection,
)

_LOG = logging.getLogger(__name__)
_CONF_DIRS: set[str] = set()


def _cleanup_conf_dirs() -> None:
    """Cleanup temporary client configuration directories."""
    for conf_dir in list(_CONF_DIRS):
        conf_path = Path(conf_dir)
        try:
            with contextlib.suppress(FileNotFoundError):
                shutil.rmtree(conf_path)
        except OSError as exc:
            _LOG.warning("Failed to clean temporary Ozone conf directory %s: %s", conf_path, exc)
        finally:
            if not conf_path.exists():
                _LOG.debug("Temporary Ozone conf directory already removed: %s", conf_path)
            _CONF_DIRS.discard(conf_dir)


atexit.register(_cleanup_conf_dirs)


@dataclass(frozen=True)
class OzoneConnSnapshot:
    """Lightweight connection snapshot to reduce repeated connection reads."""

    host: str
    port: int
    extra: dict[str, object]


class OzoneEnv:
    """
    SSL/Kerberos + environment builder for Ozone CLI.

    Responsibilities:
    - Lazy load SSL/TLS and Kerberos configuration from Airflow connection Extra.
    - Generate minimal ozone-site.xml/core-site.xml (optional) if ozone_scm_address is provided.
    - Build subprocess environment for `ozone` CLI (OZONE_OM_ADDRESS, OZONE_CONF_DIR, SSL, Kerberos).

    This module does not run ozone CLI commands and does not implement retries.
    Command execution and retry logic are implemented in OzoneCliHook.
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

    @cached_property
    def connection(self) -> object:
        return self._get_connection(self.conn_id)

    @cached_property
    def connection_snapshot(self) -> OzoneConnSnapshot:
        conn = self.connection
        extra = get_connection_extra(conn)
        host = str(getattr(conn, "host", None) or "localhost")
        port = int(getattr(conn, "port", None) or 9862)
        return OzoneConnSnapshot(host=host, port=port, extra=extra)

    @cached_property
    def ssl_env(self) -> dict[str, str] | None:
        """SSL/TLS configuration from connection Extra (lazy)."""
        try:
            return load_ssl_env_from_connection(
                self.connection,
                conn_id=self.conn_id,
                logger=self.log,
                enabled_flag_keys=("ozone_security_enabled", "ozone.security.enabled"),
            )
        except AirflowException as err:
            self.log.debug("Could not load SSL configuration (connection may not exist): %s", str(err))
        return None

    @cached_property
    def kerberos_env(self) -> dict[str, str] | None:
        """Kerberos configuration from connection Extra (lazy)."""
        try:
            kerberos_env_vars = get_kerberos_env_vars(self.connection_snapshot.extra, conn_id=self.conn_id)
            if kerberos_env_vars:
                kinit_from_env_vars(kerberos_env_vars, existing_env=os.environ)
                kerberos_env = apply_kerberos_env_vars(kerberos_env_vars)

                self.log.debug(
                    "Kerberos configuration loaded from connection: %s",
                    list(kerberos_env_vars.keys()),
                )
                if self.connection_snapshot.extra.get("hadoop_security_authentication") == "kerberos":
                    self.log.debug("Kerberos authentication enabled for Ozone Native CLI")
                return kerberos_env
            self.log.debug("No Kerberos configuration found in connection Extra")
        except AirflowException as err:
            self.log.debug("Could not load Kerberos configuration (connection may not exist): %s", str(err))
        except ValueError as err:
            raise AirflowException(
                f"Invalid Kerberos configuration in connection '{self.conn_id}': {err}"
            ) from err
        return None

    def kerberos_enabled(self) -> bool:
        """
        Return True if Kerberos is effectively enabled.

        We treat presence of kerberos env + explicit HADOOP_SECURITY_AUTHENTICATION=kerberos as enabled.
        """
        if not self.kerberos_env:
            return False
        return str(self.kerberos_env.get("HADOOP_SECURITY_AUTHENTICATION", "")).lower() == "kerberos"

    @cached_property
    def effective_config_dir(self) -> str | None:
        """Return effective config dir used by env and CLI `--config`."""
        if self.kerberos_env:
            conf_dir = self.kerberos_env.get("OZONE_CONF_DIR") or self.kerberos_env.get("HADOOP_CONF_DIR")
            if conf_dir:
                return conf_dir
        return self.client_conf_dir

    @cached_property
    def client_conf_dir(self) -> str | None:
        """
        If connection Extra has Ozone client addresses, build minimal ozone-site.xml and return temp dir.

        Extra keys:
        - ozone_scm_address: required to avoid CLI hanging in some environments
        - ozone_recon_address: optional
        """
        tmpdir: str | None = None
        try:
            snap = self.connection_snapshot
        except AirflowException as err:
            self.log.debug("Could not get connection for client conf: %s", err)
            return None

        ozone_scm_address = snap.extra.get("ozone_scm_address")
        if not ozone_scm_address:
            return None

        tmpdir = tempfile.mkdtemp(prefix="airflow_ozone_conf_")
        try:
            om_addr = f"{snap.host}:{snap.port}"
            scm_addr = str(ozone_scm_address or "scm").strip()
            recon_addr = str(snap.extra.get("ozone_recon_address") or "").strip()

            tmp_path = Path(tmpdir)

            ozone_root = ET.Element("configuration")
            ozone_props = [
                ("ozone.om.address", om_addr),
                ("ozone.scm.client.address", scm_addr),
                ("ozone.scm.block.client.address", scm_addr),
            ]
            if recon_addr:
                ozone_props.append(("ozone.recon.address", recon_addr))

            for prop_name, prop_value in ozone_props:
                prop = ET.SubElement(ozone_root, "property")
                ET.SubElement(prop, "name").text = prop_name
                ET.SubElement(prop, "value").text = prop_value

            ET.ElementTree(ozone_root).write(
                tmp_path / "ozone-site.xml",
                encoding="utf-8",
                xml_declaration=True,
            )
            ET.ElementTree(ET.Element("configuration")).write(
                tmp_path / "core-site.xml",
                encoding="utf-8",
                xml_declaration=True,
            )

            self.log.debug("Generated minimal ozone-site.xml (om=%s, scm=%s)", om_addr, scm_addr)

            _CONF_DIRS.add(tmpdir)
            return tmpdir
        except (OSError, TypeError, ValueError) as err:
            self.log.error("Failed to create Ozone client conf from Extra: %s", err)
            if tmpdir:
                tmp_path = Path(tmpdir)
                try:
                    with contextlib.suppress(FileNotFoundError):
                        shutil.rmtree(tmp_path)
                except OSError as cleanup_error:
                    self.log.warning(
                        "Failed to clean temporary Ozone conf directory %s: %s",
                        tmpdir,
                        cleanup_error,
                    )
            raise AirflowException(
                f"Failed to create temporary Ozone client configuration for connection '{self.conn_id}'"
            ) from err

    def build_env(self) -> dict[str, str]:
        """
        Build subprocess env for Ozone CLI.

        - OZONE_OM_ADDRESS from connection
        - OZONE_CONF_DIR from generated minimal conf (optional)
        - SSL env overrides (optional)
        - Kerberos env overrides (optional)
        """
        env = merge_env_overrides(None)

        try:
            snap = self.connection_snapshot
            env["OZONE_OM_ADDRESS"] = f"{snap.host}:{snap.port}"
        except AirflowException as err:
            self.log.debug("Could not set OZONE_OM_ADDRESS from connection '%s': %s", self.conn_id, err)

        conf_dir = self.effective_config_dir
        if conf_dir:
            env["OZONE_CONF_DIR"] = conf_dir

        if self.ssl_env:
            env.update(self.ssl_env)
        if self.kerberos_env:
            env.update(self.kerberos_env)

        return env
