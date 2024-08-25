#!/bin/bash
set -e

pushd src || exit
./build-lambda-layer.sh
popd || exit

pushd sample-apps || exit
./package-lambda-function.sh
popd || exit

pushd terraform/lambda || exit
terraform init
terraform apply -auto-approve
popd || exit