#!/usr/bin/env bash
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

# All-in-one Ozone startup: SCM + OM + Datanode in a single container.

set -e

OZONE_HOME="${OZONE_HOME:-/opt/hadoop}"
OZONE_BIN="${OZONE_HOME}/bin/ozone"
METADATA_DIR="/data/metadata"
HDDS_DIR="/data/hdds"

# Limit JVM heap to keep total memory under control
export OZONE_SCM_OPTS="-Xmx512m"
export OZONE_OM_OPTS="-Xmx384m"
export HDDS_DATANODE_OPTS="-Xmx384m"

mkdir -p "${METADATA_DIR}" "${HDDS_DIR}"

# Generate ozone-site.xml from environment variables (OZONE-SITE.XML_*)
/usr/local/bin/entrypoint.sh echo "Config generated" > /dev/null 2>&1 || true

echo "Initializing SCM..."
if [ ! -d "${METADATA_DIR}/scm/current" ]; then
    ${OZONE_BIN} scm --init 2>&1
fi

echo "Starting SCM..."
${OZONE_BIN} scm &
SCM_PID=$!
sleep 10

echo "Initializing OM..."
if [ ! -d "${METADATA_DIR}/om/current" ]; then
    ${OZONE_BIN} om --init 2>&1
fi

echo "=== Starting OM ==="
${OZONE_BIN} om &
OM_PID=$!
sleep 10

echo "Starting Datanode..."
${OZONE_BIN} datanode &
DN_PID=$!
sleep 15

echo "Waiting for Datanode registration..."
for i in $(seq 1 30); do
    if ${OZONE_BIN} admin datanode list 2>/dev/null | grep -q "Healthy"; then
        echo "Datanode registered successfully"
        break
    fi
    echo "Waiting for datanode... ($i/30)"
    sleep 2
done

echo "Ozone all-in-one started (SCM=${SCM_PID}, OM=${OM_PID}, DN=${DN_PID})"

# Wait for any process to exit
wait -n ${SCM_PID} ${OM_PID} ${DN_PID}
echo "One of the Ozone processes exited, shutting down"
kill ${SCM_PID} ${OM_PID} ${DN_PID} 2>/dev/null
wait
