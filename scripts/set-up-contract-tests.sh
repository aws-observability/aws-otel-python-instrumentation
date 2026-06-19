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

PYTHON_VERSION="${1:-}"
shift || true
APPS=("$@")

if [ "${#APPS[@]}" -eq 1 ] && { [ "${APPS[0]}" = "di" ] || [ "${APPS[0]}" = "serviceevents" ]; }; then
  prefix="${APPS[0]}"
  APPS=()
  for dir in contract-tests/images/applications/"${prefix}"-*/; do
    [ -f "${dir}Dockerfile" ] && APPS+=("$(basename "$dir")")
  done
fi

if [ "${#APPS[@]}" -eq 0 ]; then
  for dir in contract-tests/images/applications/*/; do
    [ -f "${dir}Dockerfile" ] && APPS+=("$(basename "$dir")")
    for subdir in "${dir}"*/; do
      [ -f "${subdir}Dockerfile" ] && APPS+=("$(basename "$subdir")")
    done
  done
fi

# Remove old whl files (excluding distro whl)
rm -rf dist/mock_collector*
rm -rf dist/contract_tests*

# Install python dependency for contract-test
python3 -m pip install build pytest testcontainers typing_extensions
python3 -m pip install pymysql
python3 -m pip install cryptography
python3 -m pip install mysql-connector-python

# To be clear, install binary for psycopg2 have no negative influence on otel here
# since Otel-Instrumentation running in container that install psycopg2 from source
python3 -m pip install sqlalchemy psycopg2-binary

# Create mock-collector image
mock_cache=()
if [ "$GITHUB_ACTIONS" = "true" ]; then
  mock_cache=(--cache-from "type=gha,scope=contract-mock-collector" --cache-to "type=gha,mode=max,scope=contract-mock-collector")
fi
docker buildx build contract-tests/images/mock-collector --load \
  -t aws-application-signals-mock-collector-python "${mock_cache[@]}"

# Find and store aws_opentelemetry_distro whl file
distro_whls=(dist/aws_opentelemetry_distro-*-py3-none-any.whl)
if [ ! -f "${distro_whls[0]}" ]; then
  echo "Could not find aws_opentelemetry_distro whl file in dist dir."
  exit 1
fi
DISTRO="$(basename "${distro_whls[0]}")"

# Create application images
for app in "${APPS[@]}"; do
  dockerfile="contract-tests/images/applications/${app}/Dockerfile"
  [ -f "$dockerfile" ] || dockerfile="contract-tests/images/applications/gen_ai/${app}/Dockerfile"
  if [ ! -f "$dockerfile" ]; then
    echo "Could not find Dockerfile for application ${app}"
    exit 1
  fi

  build_args=(--build-arg "DISTRO=${DISTRO}")
  [ -n "$PYTHON_VERSION" ] && build_args+=(--build-arg "PYTHON_VERSION=${PYTHON_VERSION}")

  cache=()
  if [ "$GITHUB_ACTIONS" = "true" ]; then
    scope="contract-${app}-py${PYTHON_VERSION}"
    cache=(--cache-from "type=gha,scope=${scope}" --cache-to "type=gha,mode=max,scope=${scope}")
  fi

  docker buildx build . --load -t "aws-application-signals-tests-${app}-app" \
    -f "$dockerfile" "${build_args[@]}" "${cache[@]}"
done

# Build and install mock-collector
cd contract-tests/images/mock-collector
python3 -m build --outdir ../../../dist
cd ../../../dist
python3 -m pip install mock_collector-1.0.0-py3-none-any.whl --force-reinstall

# Build and install contract-tests
cd ../contract-tests/tests
python3 -m build --outdir ../../dist
cd ../../dist
# --force-reinstall causes `ERROR: No matching distribution found for mock-collector==1.0.0`, but uninstalling and reinstalling works pretty reliably.
python3 -m pip uninstall contract-tests -y
python3 -m pip install contract_tests-1.0.0-py3-none-any.whl
