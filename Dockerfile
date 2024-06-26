# This Docker file builds auto-instrumentation image for aws-otel-python-instrumentation following upstream Docker build instruction:
# https://github.com/open-telemetry/opentelemetry-operator/blob/main/autoinstrumentation/python/Dockerfile
#   The packages are installed in the `/autoinstrumentation` directory. This is required as when instrumenting the pod by CWOperator,
#   one init container will be created to copy all the content in `/autoinstrumentation` directory to app's container. Then
#   update the `PYTHONPATH` environment variable accordingly. Then in the second stage, copy the directory to `/autoinstrumentation`.

# Stage 1: Install ADOT Python in the /operator-build folder
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

# Stage 2: Build the cp-utility binary
FROM rust:1.75 as builder

WORKDIR /usr/src/cp-utility
COPY ./tools/cp-utility .

## TARGETARCH is defined by buildx
# https://docs.docker.com/engine/reference/builder/#automatic-platform-args-in-the-global-scope
ARG TARGETARCH

# Run validations and audit only on amd64 because it is faster and those two steps
# are only used to validate the source code and don't require anything that is
# architecture specific.

# Validations
# Validate formatting
RUN if [ $TARGETARCH = "amd64" ]; then rustup component add rustfmt && cargo fmt --check ; fi

# Audit dependencies
RUN if [ $TARGETARCH = "amd64" ]; then cargo install cargo-audit && cargo audit ; fi


# Cross-compile based on the target platform.
RUN if [ $TARGETARCH = "amd64" ]; then export ARCH="x86_64" ; \
    elif [ $TARGETARCH = "arm64" ]; then export ARCH="aarch64" ; \
    else false; \
    fi \
    && rustup target add ${ARCH}-unknown-linux-musl \
    && cargo test  --target ${ARCH}-unknown-linux-musl \
    && cargo install --target ${ARCH}-unknown-linux-musl --path . --root .

# Stage 3: Build the distribution image by copying the THIRD-PARTY-LICENSES, the custom built cp command from stage 2, and the installed ADOT Python from stage 1 to their respective destinations
FROM scratch

# Required to copy attribute files to distributed docker images
ADD THIRD-PARTY-LICENSES ./THIRD-PARTY-LICENSES

COPY --from=builder /usr/src/cp-utility/bin/cp-utility /bin/cp
COPY --from=build /operator-build/workspace /autoinstrumentation
