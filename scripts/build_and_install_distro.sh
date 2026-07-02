#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Fail fast
set -e

# Check script is running in contract-tests
current_path=`pwd`
current_dir="${current_path##*/}"
if [ "$current_dir" != "aws-otel-python-instrumentation" ]; then
  echo "Please run from aws-otel-python-instrumentation dir"
  exit
fi

# Setup - update dependencies and create/empty dist dir
python3 -m pip install --upgrade pip setuptools wheel packaging build
mkdir -p dist
rm -rf dist/aws_opentelemetry_distro* dist/aws_opentelemetry_application_signals* dist/aws_opentelemetry_serviceevents*

for pkg in aws-opentelemetry-application-signals aws-opentelemetry-serviceevents aws-opentelemetry-distro; do
  (cd "$pkg" && python3 -m build --outdir ../dist)
done

cd dist
DISTRO=(aws_opentelemetry_distro-*-py3-none-any.whl)
python3 -m pip install $DISTRO --force-reinstall --find-links .
cd ..