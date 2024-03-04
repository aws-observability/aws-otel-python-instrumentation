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
pip install --upgrade pip setuptools wheel packaging build
mkdir -p dist
rm -rf dist/aws_opentelemetry_distro*

# Build distro
cd aws-opentelemetry-distro
python3 -m build --outdir ../dist

# Install distro
cd ../dist
DISTRO=(aws_opentelemetry_distro-*-py3-none-any.whl)
pip install $DISTRO --force-reinstall
cd ..