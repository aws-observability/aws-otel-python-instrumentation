#!/bin/bash
# Post-build verification for the ADOT Python auto-instrumentation image.
#
# The image's job is to have the OpenTelemetry Operator init container copy its baked-in
# /autoinstrumentation payload into a shared volume, which the application container then
# loads via PYTHONPATH. This test exercises that exact mechanism using the image's own
# cp-utility and then VERIFIES THE PORTED IMAGE, not just the copy:
#
#   Copy fidelity   - the payload is copied into the operator volume and is byte-for-byte
#                     identical to the payload baked into the image (cp-utility works).
#   Ported correctly - loading the volume payload the way the operator does (PYTHONPATH)
#                     proves the ADOT distro imports FROM the volume, reports the expected
#                     release version, and resolves + loads as the OTel `aws_distro` entry
#                     point. This is what confirms the image was ported correctly, not just
#                     that bytes moved intact.
#
# Adapted from adot-java's test-adot-javaagent-image.sh. Java copies a single javaagent.jar
# and checksums it; the Python image is FROM scratch and ships an installed directory tree,
# so we verify the tree copies faithfully AND that it actually loads and self-identifies.
#
# Usage: test-adot-python-image.sh <TEST_TAG> [EXPECTED_VERSION]
#   TEST_TAG         image ref to test (a locally built, not-yet-pushed image)
#   EXPECTED_VERSION optional; when set (release runs pass env.VERSION) the ported distro's
#                    __version__ must match exactly. Omit for local runs against source.
#
# Enable debug mode, fail on any command that fails, and fail on unset variables.
set -x -e -u

TEST_TAG=$1
EXPECTED_VERSION="${2:-}"

VOLUME=operator-volume
WORKDIR=$(mktemp -d)

# The neutral image (used to read the volume and to load the payload) must run the SAME
# Python the payload was built with -- the payload contains version-specific compiled wheels
# (cp3XX), so loading under a different Python would false-fail. Derive it from the
# Dockerfile's build stage (the `... AS build` line) so it always tracks the real build base
# instead of a hardcoded version. Override with NEUTRAL_IMAGE=... if ever needed.
FALLBACK_NEUTRAL_IMAGE=public.ecr.aws/docker/library/python:3.11
DOCKERFILE="${DOCKERFILE:-Dockerfile}"
if [ -z "${NEUTRAL_IMAGE:-}" ]; then
  NEUTRAL_IMAGE=$(grep -iE 'AS[[:space:]]+build[[:space:]]*$' "${DOCKERFILE}" 2>/dev/null | head -1 | awk '{print $2}')
  # Emergency fallback: if the Dockerfile parse yields something bogus -- missing, a build-arg
  # like python:${VERSION}, or a non-python ref -- fall back to a known-good pin instead of failing.
  case "${NEUTRAL_IMAGE}" in
    *python:[0-9]*) : ;;  # concrete python:X.Y ref -> trust it
    *)
      echo "warning: could not derive a concrete python image from ${DOCKERFILE} (got '${NEUTRAL_IMAGE}'); falling back to ${FALLBACK_NEUTRAL_IMAGE}"
      NEUTRAL_IMAGE="${FALLBACK_NEUTRAL_IMAGE}"
      ;;
  esac
fi
IMAGE_SRC="${WORKDIR}/image-src"
VOLUME_COPY="${WORKDIR}/volume-copy"

cleanup() {
  docker rm -f adot-verify >/dev/null 2>&1 || true
  docker rm -f adot-src >/dev/null 2>&1 || true
  docker volume rm "${VOLUME}" >/dev/null 2>&1 || true
  rm -rf "${WORKDIR}"
}
trap cleanup EXIT

# Pre-pull the neutral image with retry/backoff. Unauthenticated public ECR pulls are
# rate-limited per source IP, which shared CI runners frequently trip ("toomanyrequests").
# Pulling once up front (with retries) means the later `docker run`s use the cached image.
pull_with_retry() {
  image=$1
  for attempt in 1 2 3 4 5; do
    if docker pull "${image}"; then
      return 0
    fi
    echo "pull of ${image} failed (attempt ${attempt}/5); retrying in $((attempt * 15))s..."
    sleep "$((attempt * 15))"
  done
  echo "error: could not pull ${image} after 5 attempts"
  return 1
}
pull_with_retry "${NEUTRAL_IMAGE}"

docker volume create "${VOLUME}"

# 1. Exercise the image's own cp-utility exactly as the operator init container does:
#    recursively copy the baked-in /autoinstrumentation payload into the shared volume.
docker run --rm --mount source="${VOLUME}",dst=/otel-auto-instrumentation "${TEST_TAG}" \
  /bin/cp -r /autoinstrumentation /otel-auto-instrumentation

# 2. Assert the payload actually landed in the operator volume, using a neutral container
#    (the ADOT image is FROM scratch and has no shell/coreutils).
docker run -d --name adot-verify --mount source="${VOLUME}",dst=/otel-auto-instrumentation \
  "${NEUTRAL_IMAGE}" sleep 300 >/dev/null
docker cp adot-verify:/otel-auto-instrumentation "${VOLUME_COPY}"
if [ -z "$(ls -A "${VOLUME_COPY}" 2>/dev/null)" ]; then
  echo "error: /autoinstrumentation was not copied into the operator-volume"
  exit 1
fi
echo "autoinstrumentation payload was copied to the operator-volume"

# 3. VERIFY THE PORTED IMAGE. Load the volume payload the way the OTel Operator does --
#    mount the volume and put it on PYTHONPATH -- then confirm the ADOT distro imports
#    from the volume, reports the expected version, and resolves as the OTel distro plugin.
# NOTE: -i is required so the heredoc on stdin is forwarded into the container's `python -`.
# Without it, python reads empty stdin, runs nothing, and exits 0 -- a silent false pass.
docker run --rm -i \
  --mount source="${VOLUME}",dst=/otel-auto-instrumentation \
  -e OTEL_PAYLOAD=/otel-auto-instrumentation \
  -e EXPECTED_VERSION="${EXPECTED_VERSION}" \
  -e PYTHONPATH=/otel-auto-instrumentation \
  "${NEUTRAL_IMAGE}" python - <<'PY'
import os

payload = os.environ["OTEL_PAYLOAD"]

# (a) the ADOT distro imports, and does so FROM the operator volume (not some other install).
#     amazon.opentelemetry.distro is a PEP 420 namespace package (__file__ is None), so locate
#     the load path via the concrete version module's file instead.
import amazon.opentelemetry.distro  # noqa: F401  (proves the package imports at all)
from amazon.opentelemetry.distro import version as version_mod
loaded_from = os.path.realpath(version_mod.__file__)
assert loaded_from.startswith(os.path.realpath(payload)), \
    f"ADOT distro loaded from {loaded_from}, not the operator volume {payload}"

# (b) the ported distro reports the expected release version (proves the correctly-versioned
#     source was built into the image)
version = version_mod.__version__
expected = os.environ.get("EXPECTED_VERSION") or ""
if expected:
    assert version == expected, f"ported distro version {version!r} != expected {expected!r}"

# (c) the OTel `aws_distro` entry point resolves and loads from the ported dist metadata
#     (proves the .dist-info ported and the distro is actually wired into auto-instrumentation)
from importlib.metadata import entry_points
eps = {ep.name: ep for ep in entry_points(group="opentelemetry_distro")}
assert "aws_distro" in eps, f"aws_distro entry point missing from ported payload: {sorted(eps)}"
distro_cls = eps["aws_distro"].load()
assert distro_cls.__name__ == "AwsOpenTelemetryDistro", f"unexpected distro class: {distro_cls!r}"

print(f"ported image verified: aws-opentelemetry-distro v{version} loaded from the operator "
      f"volume and resolved as OTel distro -> {distro_cls.__name__}")
PY

# 4. Copy fidelity: the copied tree must be byte-for-byte identical to the image payload.
#    (scratch image: create -- but never start -- a container to copy the original out of.)
docker create --name adot-src "${TEST_TAG}" /bin/cp >/dev/null
docker cp adot-src:/autoinstrumentation "${IMAGE_SRC}"
if diff -r "${IMAGE_SRC}" "${VOLUME_COPY}"; then
  echo "copied autoinstrumentation payload matched the image payload"
else
  echo "error: copied autoinstrumentation payload differs from the image payload"
  exit 1
fi

ORIG_CHECKSUM=$(cd "${IMAGE_SRC}" && find . -type f -exec sha256sum {} \; | LC_ALL=C sort | sha256sum | cut -d' ' -f1)
COPY_CHECKSUM=$(cd "${VOLUME_COPY}" && find . -type f -exec sha256sum {} \; | LC_ALL=C sort | sha256sum | cut -d' ' -f1)
if [ "${COPY_CHECKSUM}" = "${ORIG_CHECKSUM}" ]; then
  echo "copied autoinstrumentation checksum matched (${COPY_CHECKSUM})"
else
  echo "error: copied autoinstrumentation checksum mis-matched (image=${ORIG_CHECKSUM} volume=${COPY_CHECKSUM})"
  exit 1
fi
