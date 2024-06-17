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

# Find and store aws_opentelemetry_distro whl file
cd dist
DISTRO=(aws_opentelemetry_distro-*-py3-none-any.whl)
if [ "$DISTRO" = "aws_opentelemetry_distro-*-py3-none-any.whl" ]; then
 echo "Could not find aws_opentelemetry_distro whl file in dist dir."
 exit 1
fi

# Create application images
# TODO: The vehicle sample app doesn't exist anymore so this needs to be cleaned up
cd ..
docker build . -t performance-test/simple-service-adot -f sample-applications/simple-flask-service/Dockerfile-ADOT --build-arg="DISTRO=${DISTRO}"
if [ $? = 1 ]; then
  echo "Docker build for simple-service-adot failed"
  exit 1
fi

docker build . -t performance-test/simple-service-otel -f sample-applications/simple-flask-service/Dockerfile-OTEL
if [ $? = 1 ]; then
  echo "Docker build for simple-service-otel failed"
  exit 1
fi