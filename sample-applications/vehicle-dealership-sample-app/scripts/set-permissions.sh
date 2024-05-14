#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

#set -ex

CLUSTER_NAME=${1:-"python-test"}
REGION=${2:-"us-east-1"}
OPERATION=${3:-"attach"}


POLICY_ARNS=(
  "arn:aws:iam::aws:policy/AmazonSQSFullAccess"
  "arn:aws:iam::aws:policy/AWSXrayWriteOnlyAccess"
  "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
  "arn:aws:iam::aws:policy/AmazonS3FullAccess"
  "arn:aws:iam::aws:policy/AmazonEC2FullAccess"
)

NODE_GROUP=$(aws eks list-nodegroups --cluster-name "$CLUSTER_NAME" --region "$REGION" --output text | awk '{print $2}')

role_arn=$(aws eks describe-nodegroup --cluster-name "$CLUSTER_NAME" --region "$REGION" --nodegroup-name "$NODE_GROUP" --query "nodegroup.nodeRole" --output text)
if [ -z "$role_arn" ]; then
  echo "Error: Failed to retrieve the node group IAM role arn."
  exit 1
fi

role_name=$(echo $role_arn | awk -F '/' '{print $NF}')

if [ "$OPERATION" == "attach" ]; then
  echo "Attaching policies to the node group..."

  for policy_arn in "${POLICY_ARNS[@]}"; do
    aws iam attach-role-policy --role-name "$role_name" --policy-arn "$policy_arn"
  done

  echo "Policies attached to the node group successfully."
else
  echo "Removing policies in the node group..."

  for policy_arn in "${POLICY_ARNS[@]}"; do
    aws iam detach-role-policy --role-name "$role_name" --policy-arn "$policy_arn"
  done

  echo "Policies detached to the node group successfully."
fi

