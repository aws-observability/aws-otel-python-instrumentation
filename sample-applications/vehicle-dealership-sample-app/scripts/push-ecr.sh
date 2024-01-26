#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

REGION=${1:-"us-east-1"}

aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${REPOSITORY_PREFIX}

aws ecr create-repository --repository-name pythonsampleapp/image-service --region ${REGION} || true
docker tag pythonsampleapp/image-service:latest ${REPOSITORY_PREFIX}/pythonsampleapp/image-service:latest
docker push ${REPOSITORY_PREFIX}/pythonsampleapp/image-service:latest

aws ecr create-repository --repository-name pythonsampleapp/vehicle-inventory-service --region ${REGION} || true
docker tag pythonsampleapp/vehicle-inventory-service:latest ${REPOSITORY_PREFIX}/pythonsampleapp/vehicle-inventory-service:latest
docker push ${REPOSITORY_PREFIX}/pythonsampleapp/vehicle-inventory-service:latest