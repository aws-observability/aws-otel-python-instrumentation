#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Prepares the host environment to run the contract-test pytest suite: installs the python test
# dependencies and builds + installs the `mock_collector` and `contract_tests` host wheels (which
# the pytest process imports). This builds NO docker images — use build-contract-test-images.sh for
# that. Run after the application images are built/available and before invoking pytest.

set -e

# Check the script is running from the repository root.
current_path=$(pwd)
current_dir="${current_path##*/}"
if [ "$current_dir" != "aws-otel-python-instrumentation" ]; then
  echo "Please run from aws-otel-python-instrumentation dir"
  exit 1
fi

# Remove old wheels (excluding the distro whl, which is built separately and consumed by images).
rm -rf dist/mock_collector*
rm -rf dist/contract_tests*

# Install python dependencies used by the test runner and the testcontainers DB drivers.
# `build` is needed to produce the mock_collector / contract_tests wheels below.
python3 -m pip install build pytest testcontainers typing_extensions
python3 -m pip install pymysql
python3 -m pip install cryptography
python3 -m pip install mysql-connector-python

# To be clear, installing the binary for psycopg2 has no negative influence on otel here
# since Otel-Instrumentation runs in a container that installs psycopg2 from source.
python3 -m pip install sqlalchemy psycopg2-binary

# Build and install mock-collector host wheel.
pushd contract-tests/images/mock-collector > /dev/null
python3 -m build --outdir ../../../dist
popd > /dev/null
python3 -m pip install dist/mock_collector-1.0.0-py3-none-any.whl --force-reinstall

# Build and install contract-tests host wheel.
pushd contract-tests/tests > /dev/null
python3 -m build --outdir ../../dist
popd > /dev/null
# --force-reinstall causes `ERROR: No matching distribution found for mock-collector==1.0.0`, but
# uninstalling and reinstalling works pretty reliably.
python3 -m pip uninstall contract-tests -y
python3 -m pip install dist/contract_tests-1.0.0-py3-none-any.whl
