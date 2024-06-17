#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Fail fast
set -e

# If a distro is not provided, run service normally. If it is, run the service with instrumentation.
if [[ "${DO_INSTRUMENT}" == "true" ]]; then
    opentelemetry-instrument gunicorn --config gunicorn_config.py app:app &
    #opentelemetry-instrument python3 -u ./requests_server.py &
else
    gunicorn --config gunicorn_config.py app:app &
    #python3 -u ./requests_server.py &
fi

if [[ "${PROFILE}" == "true"  ]]; then
    PID=$!
    sleep 3
    py-spy record -d $DURATION -r 33 -o /results/profile-$TEST_NAME.svg --pid $PID
fi

wait