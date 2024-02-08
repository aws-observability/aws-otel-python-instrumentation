#!/bin/bash
# Check script is running in contract-tests
current_path=`pwd`
current_dir="${current_path##*/}"
if [ "$current_dir" != "aws-otel-python-instrumentation" ]; then
  echo "Please run from aws-otel-python-instrumentation dir"
  exit
fi

# Remove old whl files (excluding distro whl)
rm -rf dist/mock_collector*
rm -rf dist/contract_tests*

# Create mock-collector image
cd contract-tests/images/mock-collector
docker build . -t aws-appsignals-mock-collector-python
if [ $? = 1 ]; then
  echo "Docker build for mock collector failed"
  exit 1
fi

# Find and store aws_opentelemetry_distro whl file
cd ../../../dist
DISTRO=(aws_opentelemetry_distro-*-py3-none-any.whl)
if [ "$DISTRO" = "aws_opentelemetry_distro-*-py3-none-any.whl" ]; then
 echo "Could not find aws_opentelemetry_distro whl file in dist dir."
 exit 1
fi

# Create application images
cd ..
for dir in contract-tests/images/applications/*
do
  application="${dir##*/}"
  docker build . -t aws-appsignals-tests-${application}-app -f ${dir}/Dockerfile --build-arg="DISTRO=${DISTRO}"
  if [ $? = 1 ]; then
    echo "Docker build for ${application} application failed"
    exit 1
  fi
done

# Build and install mock-collector
cd contract-tests/images/mock-collector
python3 -m build --outdir ../../../dist
cd ../../../dist
pip install mock_collector-1.0.0-py3-none-any.whl --force-reinstall

# Build and install contract-tests
cd ../contract-tests/tests
python3 -m build --outdir ../../dist
cd ../../dist
# --force-reinstall causes `ERROR: No matching distribution found for mock-collector==1.0.0`, but uninstalling and reinstalling works pretty reliably.
pip uninstall contract-tests -y
pip install contract_tests-1.0.0-py3-none-any.whl