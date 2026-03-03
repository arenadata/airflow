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

from airflow.providers.arenadata.ozone.utils.security.kerberos import (
    KerberosConfig,
    apply_kerberos_env_vars,
    get_kerberos_env_vars,
    kinit_from_env_vars,
    kinit_with_keytab,
)
from airflow.providers.arenadata.ozone.utils.security.secret_resolver import get_secret_value, is_secret_ref
from airflow.providers.arenadata.ozone.utils.security.ssl import (
    SSLConfig,
    apply_ssl_env_vars,
    get_ssl_env_vars,
    load_ssl_env_from_connection,
)

__all__ = [
    "get_secret_value",
    "is_secret_ref",
    "SSLConfig",
    "get_ssl_env_vars",
    "apply_ssl_env_vars",
    "load_ssl_env_from_connection",
    "KerberosConfig",
    "get_kerberos_env_vars",
    "apply_kerberos_env_vars",
    "kinit_with_keytab",
    "kinit_from_env_vars",
]
