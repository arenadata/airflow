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

from unittest.mock import patch

from airflow.models.connection import Connection
from airflow.providers.arenadata.ozone.utils import s3_client


@patch("airflow.providers.arenadata.ozone.utils.s3_client.boto3.client")
@patch("airflow.providers.arenadata.ozone.utils.s3_client.SecretResolver.resolve_secret")
def test_get_s3_client_resolves_login_password_with_conn_id(mock_resolve_secret, mock_boto_client):
    mock_resolve_secret.side_effect = ["access-resolved", "secret-resolved"]
    conn = Connection(
        conn_id="ozone_s3_default",
        conn_type="ozone_s3",
        login="secret://vault/s3/access",
        password="secret://vault/s3/secret",
    )

    s3_client.OzoneS3Client.get_s3_client(conn)

    assert mock_resolve_secret.call_count == 2
    assert mock_resolve_secret.call_args_list[0].kwargs["conn_id"] == "ozone_s3_default"
    assert mock_resolve_secret.call_args_list[1].kwargs["conn_id"] == "ozone_s3_default"
    assert mock_boto_client.called


@patch("airflow.providers.arenadata.ozone.utils.s3_client.boto3.client")
@patch("airflow.providers.arenadata.ozone.utils.s3_client.SecretResolver.resolve_secret")
def test_get_s3_client_does_not_pass_empty_credentials(mock_resolve_secret, mock_boto_client):
    mock_resolve_secret.side_effect = ["", ""]
    conn = Connection(conn_id="ozone_s3_default", conn_type="ozone_s3", login="", password="")

    s3_client.OzoneS3Client.get_s3_client(conn)

    called_kwargs = mock_boto_client.call_args.kwargs
    assert "aws_access_key_id" not in called_kwargs
    assert "aws_secret_access_key" not in called_kwargs
