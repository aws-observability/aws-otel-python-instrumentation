#!/bin/sh
set -e

rm -rf build
rm -rf ./aws-opentelemetry-distro
cp -r ../../aws-opentelemetry-distro ./
mkdir -p build
docker build --progress plain -t aws-opentelemetry-python-layer .
docker run --rm -v "$(pwd)/build:/out" aws-opentelemetry-python-layer
rm -rf ./aws-opentelemetry-distro