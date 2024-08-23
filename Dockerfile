# This Docker file builds auto-instrumentation image for aws-otel-python-instrumentation following upstream Docker build instruction:
# https://github.com/open-telemetry/opentelemetry-operator/blob/main/autoinstrumentation/python/Dockerfile
#   The packages are installed in the `/autoinstrumentation` directory. This is required as when instrumenting the pod by CWOperator,
#   one init container will be created to copy all the content in `/autoinstrumentation` directory to app's container. Then
#   update the `PYTHONPATH` environment variable accordingly. Then in the second stage, copy the directory to `/autoinstrumentation`.

# Using Python 3.10 because we are utilizing the opentelemetry-exporter-otlp-proto-grpc exporter,
# which relies on grpcio as a dependency. grpcio has strict dependencies on the OS and Python version.
# Also mentioned in Docker build template in the upstream repository:
# https://github.com/open-telemetry/opentelemetry-operator/blob/b5bb0ae34720d4be2d229dafecb87b61b37699b0/autoinstrumentation/python/requirements.txt#L2
# For further details, please refer to: https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/azure-functions/recover-python-functions.md#the-python-interpre[…]tions-python-worker
FROM public.ecr.aws/docker/library/python:3.11 AS build

WORKDIR /operator-build

ADD aws-opentelemetry-distro/ ./aws-opentelemetry-distro/

RUN mkdir workspace && pip install --target workspace ./aws-opentelemetry-distro

FROM public.ecr.aws/docker/library/busybox:latest

COPY --from=build /operator-build/workspace /autoinstrumentation

RUN chmod -R go+r /autoinstrumentation
