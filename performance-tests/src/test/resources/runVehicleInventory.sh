#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Fail fast
set -e

# Set up service
python3 manage.py migrate --noinput
python3 manage.py collectstatic --noinput

# If a distro is not provided, run service normally. If it is, run the service with instrumentation.
if [[ -z "${DO_INSTRUMENT}" ]]; then
    python3 manage.py runserver 0.0.0.0:$PORT --noreload
else
   opentelemetry-instrument python3 manage.py runserver 0.0.0.0:$PORT --noreload
fi
