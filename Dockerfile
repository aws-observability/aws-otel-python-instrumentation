# This Docker file builds auto-instrumentation image for aws-otel-python-instrumentation following upstream Docker build instruction:
# https://github.com/open-telemetry/opentelemetry-operator/blob/main/autoinstrumentation/python/Dockerfile
#   The packages are installed in the `/autoinstrumentation` directory. This is required as when instrumenting the pod by CWOperator,
#   one init container will be created to copy all the content in `/autoinstrumentation` directory to app's container. Then
#   update the `PYTHONPATH` environment variable accordingly. Then in the second stage, copy the directory to `/autoinstrumentation`.
FROM python:3.11 AS build

WORKDIR /operator-build

ADD aws-opentelemetry-distro/ ./aws-opentelemetry-distro/

# Remove opentelemetry-exporter-otlp-proto-grpc and grpcio, as grpcio has strict dependencies on the Python version and
# will cause confusing failures if gRPC protocol is used. Now if gRPC protocol is requested by the user, instrumentation
# will complain that grpc is not installed, which is more understandable. References:
# * https://github.com/open-telemetry/opentelemetry-operator/blob/b5bb0ae34720d4be2d229dafecb87b61b37699b0/autoinstrumentation/python/requirements.txt#L2
# * https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/azure-functions/recover-python-functions.md#troubleshoot-cannot-import-cygrpc
RUN sed -i "/opentelemetry-exporter-otlp-proto-grpc/d" ./aws-opentelemetry-distro/pyproject.toml

RUN mkdir workspace && pip install --target workspace ./aws-opentelemetry-distro


FROM busybox

# Required to copy attribute files to distributed docker images
ADD THIRD-PARTY-LICENSES ./THIRD-PARTY-LICENSES

COPY --from=build /operator-build/workspace /autoinstrumentation