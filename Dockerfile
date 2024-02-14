# This Docker file builds auto-instrumentation image for aws-otel-python-instrumentation following upstream Docker build instruction:
# https://github.com/open-telemetry/opentelemetry-operator/blob/main/autoinstrumentation/python/Dockerfile
#   The packages are installed in the `/autoinstrumentation` directory. This is required as when instrumenting the pod by CWOperator,
#   one init container will be created to copy all the content in `/autoinstrumentation` directory to app's container. Then
#   update the `PYTHONPATH` environment variable accordingly. Then in the second stage, copy the directory to `/autoinstrumentation`.

FROM python:3.10 AS build

WORKDIR /operator-build

ADD aws-opentelemetry-distro/ ./aws-opentelemetry-distro/

RUN mkdir workspace && pip install --target workspace ./aws-opentelemetry-distro

FROM busybox

COPY --from=build /operator-build/workspace /autoinstrumentation

RUN chmod -R go+r /autoinstrumentation
