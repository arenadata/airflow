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

from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import Connection, Variable
    from airflow.sdk.bases.hook import BaseHook
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.bases.sensor import BaseSensorOperator
    from airflow.sdk.definitions.context import Context
else:
    from airflow.hooks.base import BaseHook
    from airflow.models import BaseOperator, Connection, Variable
    from airflow.sensors.base import BaseSensorOperator
    from airflow.utils.context import Context

__all__ = [
    "AIRFLOW_V_3_0_PLUS",
    "BaseHook",
    "BaseOperator",
    "BaseSensorOperator",
    "Connection",
    "Variable",
    "Context",
]
