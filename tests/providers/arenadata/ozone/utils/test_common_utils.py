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

import pytest

from airflow.providers.arenadata.ozone.utils.helpers import (
    EnvHelper,
    SecretHelper,
    TypeNormalizationHelper,
)


def test_normalize_optional_str_strips_and_handles_empty():
    assert TypeNormalizationHelper.normalize_optional_str("  value  ") == "value"
    assert TypeNormalizationHelper.normalize_optional_str("   ") is None
    assert TypeNormalizationHelper.normalize_optional_str(None) is None


def test_require_optional_non_empty_preserves_type_contract():
    assert TypeNormalizationHelper.require_optional_non_empty("  abc ", "err") == "abc"
    assert TypeNormalizationHelper.require_optional_non_empty(None, "err") is None
    with pytest.raises(ValueError, match="err"):
        TypeNormalizationHelper.require_optional_non_empty(123, "err")
    with pytest.raises(ValueError, match="err"):
        TypeNormalizationHelper.require_optional_non_empty("   ", "err")


def test_normalize_bool_or_passthrough():
    assert TypeNormalizationHelper.normalize_bool_or_passthrough("true", default=False) is True
    assert TypeNormalizationHelper.normalize_bool_or_passthrough("No", default=True) is False
    assert TypeNormalizationHelper.normalize_bool_or_passthrough("keep-me", default=False) == "keep-me"
    assert TypeNormalizationHelper.normalize_bool_or_passthrough(None, default=True) is True


def test_is_true_flag_matches_previous_semantics():
    assert TypeNormalizationHelper.is_true_flag({"a": "true"}, "a")
    assert TypeNormalizationHelper.is_true_flag({"a": True}, "a")
    assert not TypeNormalizationHelper.is_true_flag({"a": "false"}, "a")
    assert not TypeNormalizationHelper.is_true_flag({"a": 1}, "a")


def test_build_mapped_env_handles_plain_and_secret_values():
    extra = {"plain": "x", "secret": "raw_secret"}
    env = EnvHelper.build_mapped_env(
        extra,
        (("plain", "PLAIN_ENV", False), ("secret", "SECRET_ENV", True)),
        resolve_secret=lambda value: f"resolved:{value}",
    )
    assert env == {"PLAIN_ENV": "x", "SECRET_ENV": "resolved:raw_secret"}


def test_build_mapped_env_requires_secret_resolver_for_secret_fields():
    with pytest.raises(ValueError, match="resolve_secret"):
        EnvHelper.build_mapped_env({"secret": "x"}, (("secret", "SECRET_ENV", True),))


def test_resolve_secret_masked_delegates_and_returns_value():
    assert (
        SecretHelper.resolve_secret_masked("secret://x", lambda value: f"resolved:{value}")
        == "resolved:secret://x"
    )
