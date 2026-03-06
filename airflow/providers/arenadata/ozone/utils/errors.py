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

from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from botocore.exceptions import ClientError


class OzoneCliError(AirflowException):
    """Ozone CLI error with retryable marker."""

    def __init__(
        self,
        message: str,
        *,
        command: list[str] | None = None,
        stderr: str | None = None,
        returncode: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.command = command
        self.stderr = stderr
        self.returncode = returncode
        self.retryable = retryable


class OzoneS3Error(AirflowException):
    """Ozone S3 error with retryable marker."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class OzoneCliErrors:
    """Error classifier for Ozone CLI operations."""

    non_retryable_return_codes = (
        2,  # CLI usage / invalid arguments
        126,  # command invoked cannot execute
        127,  # command not found
        130,  # interrupted by user (SIGINT)
    )
    retryable_return_codes = (
        143,  # terminated (often timeout/orchestration kill)
        255,  # common transport/session failures in CLI tools
    )

    non_retryable_errors = (
        "already_exists",
        "not_found",
        "does not exist",
        "accessdenied",
        "access denied",
        "permission denied",
        "invalid",
        "illegalargumentexception",
        "authentication failed",
        "unauthorized",
    )

    retryable_errors = (
        "timed out",
        "timeout",
        "connection refused",
        "connection reset",
        "connection closed",
        "no route to host",
        "temporarily unavailable",
        "service unavailable",
        "unknownhostexception",
        "could not resolve",
        "sockettimeoutexception",
        "connectexception",
        "eofexception",
        "broken pipe",
        "too many requests",
        "throttl",
        "503",
    )

    @classmethod
    def is_retryable_failure(cls, return_code: int | None, stderr: str) -> bool:
        """Return True when CLI failure matches retryable return code or markers."""
        if return_code in cls.non_retryable_return_codes:
            return False
        if return_code in cls.retryable_return_codes:
            return True

        normalized = (stderr or "").lower()
        if any(marker in normalized for marker in cls.non_retryable_errors):
            return False
        return any(marker in normalized for marker in cls.retryable_errors)


class OzoneS3Errors:
    """Error classifier for Ozone S3 operations."""

    NO_SUCH_BUCKET = "NoSuchBucket"
    NO_SUCH_KEY = "NoSuchKey"
    ACCESS_DENIED = "AccessDenied"
    BUCKET_ALREADY_EXISTS = "BucketAlreadyExists"
    BUCKET_ALREADY_OWNED_BY_YOU = "BucketAlreadyOwnedByYou"
    INVALID_ACCESS_KEY_ID = "InvalidAccessKeyId"
    SIGNATURE_DOES_NOT_MATCH = "SignatureDoesNotMatch"
    INVALID_TOKEN = "InvalidToken"
    ACCESS_DENIED_EXCEPTION = "AccessDeniedException"

    non_retryable_http_codes = (
        400,  # bad request / invalid API parameters
        401,  # unauthorized (invalid or missing auth)
        403,  # forbidden (access denied by policy/ACL)
        404,  # not found (bucket/key does not exist)
        405,  # method not allowed for endpoint/resource
    )
    retryable_http_codes = (
        408,  # request timeout
        409,  # conflict (often transient in eventually-consistent flows)
        425,  # too early / retry later
        429,  # throttling / rate limit exceeded
        500,  # internal server error
        502,  # bad gateway
        503,  # service unavailable
        504,  # gateway timeout
    )

    non_retryable_errors = (
        NO_SUCH_KEY,
        NO_SUCH_BUCKET,
        ACCESS_DENIED,
        BUCKET_ALREADY_EXISTS,
        BUCKET_ALREADY_OWNED_BY_YOU,
        INVALID_ACCESS_KEY_ID,
        SIGNATURE_DOES_NOT_MATCH,
        INVALID_TOKEN,
        ACCESS_DENIED_EXCEPTION,
    )

    retryable_errors = (
        "RequestTimeout",
        "RequestTimeoutException",
        "InternalError",
        "ServiceUnavailable",
        "SlowDown",
        "Throttling",
        "ThrottlingException",
        "TooManyRequestsException",
    )

    @classmethod
    def map_s3_error_to_ozone(cls, error: ClientError) -> str:
        """Translate a boto3 ClientError into an Ozone-specific, readable message."""
        error_code = error.response.get("Error", {}).get("Code", "Unknown")

        if error_code == cls.NO_SUCH_BUCKET:
            return "Ozone S3 Gateway Error: The specified bucket does not exist."
        if error_code == cls.NO_SUCH_KEY:
            return "Ozone S3 Gateway Error: The specified key does not exist."
        if error_code == cls.ACCESS_DENIED:
            return "Ozone S3 Gateway Error: Access Denied. Check Ozone ACLs or Ranger policies."

        return f"Ozone S3 Gateway reported an unhandled error: {error_code}"

    @classmethod
    def is_retryable_failure(cls, return_code: int | None, stderr: str) -> bool:
        """Return True when S3 failure matches retryable HTTP/code class."""
        if return_code in cls.non_retryable_http_codes:
            return False
        if return_code in cls.retryable_http_codes:
            return True
        if return_code is not None:
            if 500 <= return_code <= 599:
                return True
            if 400 <= return_code <= 499:
                return False

        error_code = (stderr or "").strip()
        if error_code in cls.non_retryable_errors:
            return False
        return error_code in cls.retryable_errors
