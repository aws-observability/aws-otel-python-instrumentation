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

# Check for expected env variables
for var in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN S3_BUCKET; do
    if [[ -z "${!var}" ]]; then
        echo "Variable $var not set, please ensure all of AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, and S3_BUCKET are set."
        exit 1
    fi
done

# Find and store aws_opentelemetry_distro whl file
cd dist
DISTRO=(aws_opentelemetry_distro-*-py3-none-any.whl)
if [ "$DISTRO" = "aws_opentelemetry_distro-*-py3-none-any.whl" ]; then
 echo "Could not find aws_opentelemetry_distro whl file in dist dir."
 exit 1
fi

# Create application images
cd ..
docker build . -t performance-test/vehicle-inventory-service -f performance-tests/Dockerfile-VehicleInventoryService-base --build-arg="DISTRO=${DISTRO}"
if [ $? = 1 ]; then
  echo "Docker build for VehicleInventoryService failed"
  exit 1
fi

docker build . -t performance-test/image-service -f performance-tests/Dockerfile-ImageService-base
if [ $? = 1 ]; then
  echo "Docker build for ImageService failed"
  exit 1
fi