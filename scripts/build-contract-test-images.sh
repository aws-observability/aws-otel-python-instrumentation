#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Builds the contract-test application images (and the mock collector) with `docker buildx bake`,
# replacing the previous sequential `docker build` loop. Targets are built in parallel and, when
# CACHE_BACKEND=gha is set (CI), layers are cached per (app, python-version) across runs.
#
# Usage:
#   build-contract-test-images.sh [PYTHON_VERSION] [BAKE_TARGET ...]
#
#   PYTHON_VERSION  Optional. Python base image tag (e.g. 3.13) to build every app image against.
#                   When empty, each Dockerfile keeps its own default base (preserves the previous
#                   non-matrix behavior for local dev and main-build).
#   BAKE_TARGET     Optional list of bake targets/groups (e.g. "di", "serviceevents", "botocore").
#                   Defaults to the "default" group (all images). The mock-collector target is
#                   always appended so every caller gets the collector image.

set -e

# Check the script is running from the repository root.
current_path=$(pwd)
current_dir="${current_path##*/}"
if [ "$current_dir" != "aws-otel-python-instrumentation" ]; then
  echo "Please run from aws-otel-python-instrumentation dir"
  exit 1
fi

PYTHON_VERSION="${1:-}"
shift || true
BAKE_TARGETS=("$@")

BAKE_FILE="contract-tests/images/docker-bake.hcl"

# Find and export the prebuilt aws_opentelemetry_distro wheel filename (consumed by the app images
# as the DISTRO build-arg). The wheel must already exist in dist/ (built by artifacts_build,
# build_and_install_distro.sh, or `python -m build`).
pushd dist > /dev/null
shopt -s nullglob
wheels=(aws_opentelemetry_distro-*-py3-none-any.whl)
shopt -u nullglob
if [ "${#wheels[@]}" -eq 0 ]; then
  echo "Could not find aws_opentelemetry_distro whl file in dist dir."
  exit 1
fi
export DISTRO="${wheels[0]}"
popd > /dev/null

# Default to building everything when no targets are given. Always include the mock collector.
if [ "${#BAKE_TARGETS[@]}" -eq 0 ]; then
  BAKE_TARGETS=("default")
else
  BAKE_TARGETS+=("mock-collector")
fi

# Inject the python version build-arg only when requested, so the empty case reproduces each
# Dockerfile's native FROM. PYTHON_VERSION is also exported for cache-scope naming in the bake file.
SET_ARGS=()
if [ -n "$PYTHON_VERSION" ]; then
  export PYTHON_VERSION
  SET_ARGS+=(--set "*.args.PYTHON_VERSION=${PYTHON_VERSION}")
fi

echo "Building contract-test images (python='${PYTHON_VERSION:-<dockerfile defaults>}', targets='${BAKE_TARGETS[*]}')"
docker buildx bake -f "$BAKE_FILE" --load "${SET_ARGS[@]}" "${BAKE_TARGETS[@]}"
