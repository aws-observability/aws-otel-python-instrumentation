# This Docker file builds auto-instrumentation image for aws-otel-python-instrumentation following upstream Docker build instruction:
# https://github.com/open-telemetry/opentelemetry-operator/blob/main/autoinstrumentation/python/Dockerfile
#   The packages are installed in the `/autoinstrumentation` directory. This is required as when instrumenting the pod by CWOperator,
#   one init container will be created to copy all the content in `/autoinstrumentation` directory to app's container. Then
#   update the `PYTHONPATH` environment variable accordingly. Then in the second stage, copy the directory to `/autoinstrumentation`.

# Stage 1: Install ADOT Python in the /operator-build folder
FROM public.ecr.aws/docker/library/python:3.11 AS build

WORKDIR /operator-build

ADD aws-opentelemetry-distro/ ./aws-opentelemetry-distro/

# Remove opentelemetry-exporter-otlp-proto-grpc and grpcio, as grpcio has strict dependencies on the Python version and
# will cause confusing failures if gRPC protocol is used. Now if gRPC protocol is requested by the user, instrumentation
# will complain that grpc is not installed, which is more understandable. References:
# * https://github.com/open-telemetry/opentelemetry-operator/blob/b5bb0ae34720d4be2d229dafecb87b61b37699b0/autoinstrumentation/python/requirements.txt#L2
# * https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/azure-functions/recover-python-functions.md#troubleshoot-cannot-import-cygrpc
RUN sed -i "/opentelemetry-exporter-otlp-proto-grpc/d" ./aws-opentelemetry-distro/pyproject.toml

# urllib3 recently made a release that drops support for Python 3.8
# Our sdk depends on botocore which pulls in the version of urllib3 based Python runtime it detects.
# The rule is that if current pip command version is > 3.9 botocore will pick up the latest urllib3,
# otherwise it picks up the older urllib3 that is compatible with Python 3.8.
# https://github.com/boto/botocore/blob/develop/requirements-docs.txt
# Since this Dockerfile currently uses the fixed Python 3.11 base image to pull the required dependencies,
# EKS and ECS applications will encounter a runtime error for Python 3.8 compatibility.
# Our fix is to temporarily restrict the urllib3 version to one that works for all supported Python versions 
# that we currently commit to support (notably 3.8). 
# We also pin the setuptools version for similar issues with the library dropping 3.8 support.
# https://github.com/pypa/setuptools/blame/main/pkg_resources/__init__.py#L24
# TODO: Remove these temporary workarounds once we deprecate Python 3.8 support since it has reached end-of-life.
RUN mkdir workspace && pip install setuptools==75.2.0 urllib3==2.2.3 --target workspace ./aws-opentelemetry-distro

# Stage 2: Build the cp-utility binary
FROM public.ecr.aws/docker/library/rust:1.82 as builder

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
