from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.arenadata.ozone.utils.connection_schema import (
    CORE_SITE_XML,
    KINIT_TIMEOUT_SECONDS,
    MAX_CONTENT_SIZE_BYTES,
    OZONE_SITE_XML,
    OzoneConnSnapshot,
)


def _conn(host: str | None, port: int | None, extra: object) -> MagicMock:
    conn = MagicMock()
    conn.host = host
    conn.port = port
    conn.extra_dejson = extra
    return conn


@pytest.mark.parametrize(
    "host,port,error_substring",
    [
        (None, 9862, "must define host"),
        ("om", None, "must define port"),
    ],
)
def test_from_connection_requires_host_and_port_in_strict_mode(
    host: str | None,
    port: int | None,
    error_substring: str,
) -> None:
    with pytest.raises(AirflowException, match=error_substring):
        OzoneConnSnapshot.from_connection(
            _conn(host, port, {}),
            conn_id="ozone_default",
            require_host_port=True,
        )


def test_from_connection_relaxed_mode_defaults_and_non_dict_extra() -> None:
    snapshot = OzoneConnSnapshot.from_connection(
        _conn(None, None, "not-a-dict"),
        conn_id="hdfs_default",
        require_host_port=False,
    )
    assert snapshot.host == ""
    assert snapshot.port == 0
    assert snapshot.kinit_timeout_seconds == KINIT_TIMEOUT_SECONDS
    assert snapshot.core_site_xml == CORE_SITE_XML
    assert snapshot.ozone_site_xml == OZONE_SITE_XML
    assert snapshot.max_content_size_bytes == MAX_CONTENT_SIZE_BYTES


def test_from_connection_parses_security_contract_and_runtime_overrides() -> None:
    snapshot = OzoneConnSnapshot.from_connection(
        _conn(
            "om",
            9862,
            {
                "ozone_security_enabled": "true",
                "hdfs_ssl_enabled": "1",
                "hadoop_security_authentication": "kerberos",
                "kerberos_principal": "user@REALM",
                "kerberos_keytab": "/tmp/user.keytab",
                "krb5_conf": "/tmp/krb5.conf",
                "ozone_conf_dir": "/opt/airflow/ozone-conf",
                "kinit_timeout_seconds": "42",
                "core_site_xml": "core-custom.xml",
                "ozone_site_xml": "ozone-custom.xml",
                "max_content_size_bytes": "2048",
            },
        ),
        conn_id="ozone_default",
        require_host_port=True,
    )
    assert snapshot.ozone_security_enabled is True
    assert snapshot.hdfs_ssl_enabled is True
    assert snapshot.hadoop_security_authentication == "kerberos"
    assert snapshot.kerberos_principal == "user@REALM"
    assert snapshot.kerberos_keytab == "/tmp/user.keytab"
    assert snapshot.krb5_conf == "/tmp/krb5.conf"
    assert snapshot.ozone_conf_dir == "/opt/airflow/ozone-conf"
    assert snapshot.kinit_timeout_seconds == 42
    assert snapshot.core_site_xml == "core-custom.xml"
    assert snapshot.ozone_site_xml == "ozone-custom.xml"
    assert snapshot.max_content_size_bytes == 2048


@pytest.mark.parametrize(
    "extra,expected_timeout,expected_size_limit",
    [
        ({"kinit_timeout_seconds": "not-int", "max_content_size_bytes": "2048"}, KINIT_TIMEOUT_SECONDS, 2048),
        ({"kinit_timeout_seconds": "15", "max_content_size_bytes": "-1"}, 15, MAX_CONTENT_SIZE_BYTES),
    ],
)
def test_from_connection_invalid_numeric_overrides_fall_back_to_defaults(
    extra: dict[str, str],
    expected_timeout: int,
    expected_size_limit: int,
) -> None:
    snapshot = OzoneConnSnapshot.from_connection(
        _conn("om", 9862, extra),
        conn_id="ozone_default",
        require_host_port=True,
    )
    assert snapshot.kinit_timeout_seconds == expected_timeout
    assert snapshot.max_content_size_bytes == expected_size_limit
