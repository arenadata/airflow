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

from enum import Enum


class ReplicationType(str, Enum):
    """
    Enum for Ozone Replication Types.

    Provides a convenient way to specify replication policies in operators.
    """

    RATIS = "RATIS"
    EC = "EC"  # Erasure Coding


class ReplicationFactor(int, Enum):
    """Enum for standard Ozone Replication Factors for RATIS type."""

    ONE = 1
    THREE = 3


def get_replication_config_args(
    replication_type: ReplicationType | None = None,
    replication_factor: ReplicationFactor | None = None,
) -> list[str]:
    """Build the CLI argument list for replication settings."""
    args = []
    if replication_type:
        args.extend(["--type", replication_type.value])
    if replication_factor:
        # Note: Ozone CLI uses '--factor' for RATIS and '--data' for EC.
        # This helper assumes RATIS for simplicity here.
        args.extend(["--factor", str(replication_factor.value)])
    return args
