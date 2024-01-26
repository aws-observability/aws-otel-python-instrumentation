#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
account=$1
region=$2

# Save the endpoint URL to a variable
endpoint=$(kubectl get svc -n ingress-nginx | grep "ingress-nginx" | awk '{print $4}')

# Print the endpoint
echo "Endpoint: $endpoint"

export REPOSITORY_PREFIX=${account}.dkr.ecr.${region}.amazonaws.com
aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${REPOSITORY_PREFIX}
aws ecr create-repository --repository-name random-traffic-generator --region ${region} || true
docker build -t random-traffic-generator:latest .
docker tag random-traffic-generator:latest ${REPOSITORY_PREFIX}/random-traffic-generator:latest
docker push ${REPOSITORY_PREFIX}/random-traffic-generator:latest

sed  -e 's#\${REPOSITORY_PREFIX}'"#${REPOSITORY_PREFIX}#g" -e 's#\${URL}'"#$endpoint#g" deployment.yaml | kubectl apply -f -


