#!/usr/bin/env sh
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
#
# Download DuckDB CLI into the host-mounted /files/bin directory when missing.
# /files survives `docker compose down --volumes` used by breeze testing.
set -eu

DUCKDB_VERSION="${DUCKDB_VERSION:-1.5.3}"
DEST_DIR="${DEST_DIR:-/files/bin}"
DEST_BIN="${DEST_DIR}/duckdb"
DEST_VERSION_FILE="${DEST_DIR}/duckdb.version"

# Official GitHub release assets (glibc). Checked against v1.5.3 release
SHA256_AMD64="35caef1fecbc8d7e2c07de4fd2cdefc5189ec9ba9e1cca228fb1a1c48cc52a8a"
SHA256_ARM64="5e2399428793642e994f1584c47d49f4c58b7b4ec2297ea4a522353a6c553835"

resolve_arch() {
    if [ -n "${TARGETARCH:-}" ]; then
        case "${TARGETARCH}" in
            amd64 | arm64) echo "${TARGETARCH}"; return ;;
        esac
    fi
    case "$(uname -m)" in
        x86_64 | amd64) echo "amd64" ;;
        aarch64 | arm64) echo "arm64" ;;
        *)
            echo "Unsupported architecture: $(uname -m)" >&2
            exit 1
            ;;
    esac
}

download_with_retry() {
    url="$1"
    out="$2"
    attempt=1
    max_attempts=5
    while [ "${attempt}" -le "${max_attempts}" ]; do
        if curl -fsSL --retry 3 --retry-delay 2 -o "${out}" "${url}"; then
            return 0
        fi
        echo "Download failed (attempt ${attempt}/${max_attempts}): ${url}" >&2
        attempt=$((attempt + 1))
        sleep $((attempt * 2))
    done
    echo "Exhausted retries downloading ${url}" >&2
    exit 1
}

ARCH="$(resolve_arch)"
case "${ARCH}" in
    amd64) EXPECTED_SHA="${SHA256_AMD64}" ;;
    arm64) EXPECTED_SHA="${SHA256_ARM64}" ;;
esac

if [ -x "${DEST_BIN}" ] && [ -f "${DEST_VERSION_FILE}" ] && [ "$(cat "${DEST_VERSION_FILE}")" = "${DUCKDB_VERSION}" ]; then
    echo "DuckDB CLI ${DUCKDB_VERSION} already present at ${DEST_BIN}"
    exit 0
fi

echo "Fetching DuckDB CLI ${DUCKDB_VERSION} (${ARCH}) into ${DEST_DIR}"
mkdir -p "${DEST_DIR}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

ZIP_PATH="${TMP_DIR}/duckdb_cli-linux-${ARCH}.zip"
URL="https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${ARCH}.zip"
download_with_retry "${URL}" "${ZIP_PATH}"

ACTUAL_SHA="$(sha256sum "${ZIP_PATH}" | awk '{print $1}')"
if [ "${ACTUAL_SHA}" != "${EXPECTED_SHA}" ]; then
    echo "SHA256 mismatch for ${ZIP_PATH}" >&2
    echo "expected: ${EXPECTED_SHA}" >&2
    echo "actual:   ${ACTUAL_SHA}" >&2
    exit 1
fi

unzip -o -q "${ZIP_PATH}" -d "${TMP_DIR}"
if [ ! -f "${TMP_DIR}/duckdb" ]; then
    echo "Zip did not contain duckdb binary" >&2
    exit 1
fi

install -m 0755 "${TMP_DIR}/duckdb" "${DEST_BIN}"
printf '%s\n' "${DUCKDB_VERSION}" > "${DEST_VERSION_FILE}"

# do not execute the binary here:official linux-* zips are glibc-linked and
# this fetch container is Alpine (musl).Execution is validated in the Airflow CI image
if [ ! -x "${DEST_BIN}" ]; then
    echo "Installed binary is not executable: ${DEST_BIN}" >&2
    exit 1
fi
echo "DuckDB CLI ready: ${DEST_BIN} (${DUCKDB_VERSION}, ${ARCH})"
