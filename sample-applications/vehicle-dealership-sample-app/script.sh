# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
account=$1
cluster_name=$2
region=$3
password=$4
s3_bucket=$5

export REPOSITORY_PREFIX=${account}.dkr.ecr.$region.amazonaws.com
export MYSQL_ROOT_PASSWORD=${password}
export MYSQL_DATABASE=vehicle_inventory
export MYSQL_USER=djangouser
export MYSQL_PASSWORD=${password}
export S3_BUCKET=${s3_bucket}

docker-compose up --build

eksctl create cluster --name ${cluster_name} --region ${region} --zones ${region}a,${region}b
eksctl create addon --name aws-ebs-csi-driver --cluster ${cluster_name} --service-account-role-arn arn:aws:iam::${account}:role/Admin --region ${region} --force

./scripts/push-ecr.sh ${region}

./scripts/set-permissions.sh ${cluster_name} ${region}

./scripts/deploy-eks.sh