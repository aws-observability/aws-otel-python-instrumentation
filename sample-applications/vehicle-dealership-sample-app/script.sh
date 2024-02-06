#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

account=$1
cluster_name=$2
region=$3
password=$4
s3_bucket=$5

rm VehicleInventoryApp/.env
touch VehicleInventoryApp/.env
rm ImageServiceApp/.env
touch ImageServiceApp/.env

export REPOSITORY_PREFIX=${account}.dkr.ecr.$region.amazonaws.com
export POSTGRES_DATABASE=vehicle_inventory
export POSTGRES_USER=djangouser
export POSTGRES_PASSWORD=${password}
export S3_BUCKET=${s3_bucket}

docker-compose build

#eksctl create cluster --name ${cluster_name} --region ${region} --zones ${region}a,${region}b
#eksctl create addon --name aws-ebs-csi-driver --cluster ${cluster_name} --service-account-role-arn arn:aws:iam::${account}:role/Admin --region ${region} --force
#
./scripts/push-ecr.sh ${region}
#
#./scripts/set-permissions.sh ${cluster_name} ${region}

./scripts/deploy-eks.sh