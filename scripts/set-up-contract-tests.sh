#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Backward-compatible orchestrator for the contract-test setup. Kept for local dev, the README
# flow, and callers such as main-build.yml. It now delegates to two focused scripts:
#   1. build-contract-test-images.sh — builds the application + mock-collector docker images.
#   2. run-contract-tests.sh         — installs host deps and the mock_collector/contract_tests wheels.
# After running this, invoke `pytest contract-tests/tests` (or a subset).
#
# Usage: set-up-contract-tests.sh [PYTHON_VERSION]
#   PYTHON_VERSION  Optional. Builds every application image against this python base (e.g. 3.13).
#                   When omitted, each Dockerfile keeps its own default base (previous behavior).
#                   Previously this positional arg was silently ignored; it is now honored.

set -e

# Check the script is running from the repository root.
current_path=$(pwd)
current_dir="${current_path##*/}"
if [ "$current_dir" != "aws-otel-python-instrumentation" ]; then
  echo "Please run from aws-otel-python-instrumentation dir"
  exit 1
fi

PYTHON_VERSION="${1:-}"

bash scripts/build-contract-test-images.sh "$PYTHON_VERSION"
bash scripts/run-contract-tests.sh
