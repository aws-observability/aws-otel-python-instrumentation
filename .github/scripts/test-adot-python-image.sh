#!/bin/bash
# Post-build verification for the ADOT Python auto-instrumentation image.
# This script:
#   1. runs the image's own cp-utility to copy /autoinstrumentation into an operator volume,
#   2. confirms the payload actually landed (the copy ran and is non-empty),
#   3. loads the volume payload the way the operator does (mount + PYTHONPATH) and asserts the
#      ADOT distro imports FROM the volume, reports the expected release version, and resolves
#      + loads as the OTel `aws_distro` entry point -- i.e. the RIGHT, working artifact ported,
#   4. checks the copied tree is byte-for-byte identical to the payload baked into the image
#      (diff -r + aggregate sha256).
#
# Steps 1-2 + 4 are copy fidelity (bytes moved intact); step 3 is the "ported correctly" check
# a pure checksum can't give -- it proves the payload actually loads and self-identifies.

# Usage: test-adot-python-image.sh <TEST_TAG> [EXPECTED_VERSION] [WHEEL]
#   TEST_TAG         image ref to test (a locally built, not-yet-pushed image)
#   EXPECTED_VERSION optional; when set (release runs pass env.VERSION) the ported distro's
#                    __version__ must match exactly. Omit for local runs against source.
#   WHEEL            optional path to the release wheel. When set, the image's ADOT library
#                    files are cross-checked against this independently-built artifact (a
#                    reference from BEFORE the image), not just the image against itself.

set -x -e -u

TEST_TAG=$1
EXPECTED_VERSION="${2:-}"
WHEEL="${3:-}"

# Per-run unique names so a run killed before the trap fires (cancelled workflow, OOM) can't
# leave a volume/containers behind for the next run to pick up -- which would make diff -r
# compare a mixed tree.
RUN_ID="$$-${RANDOM}"
VOLUME="operator-volume-${RUN_ID}"
VERIFY_CTR="adot-verify-${RUN_ID}"
SRC_CTR="adot-src-${RUN_ID}"
WORKDIR=$(mktemp -d)
IMAGE_SRC="${WORKDIR}/image-src"
VOLUME_COPY="${WORKDIR}/volume-copy"

cleanup() {
  docker rm -f "${VERIFY_CTR}" >/dev/null 2>&1 || true
  docker rm -f "${SRC_CTR}" >/dev/null 2>&1 || true
  docker volume rm "${VOLUME}" >/dev/null 2>&1 || true
  rm -rf "${WORKDIR}"
}
trap cleanup EXIT

docker volume create "${VOLUME}"

# Extract the image's baked-in payload up front (scratch image: create, don't run) -- used both
# for the copy-fidelity diff in step 4 and to detect which Python the payload was built for.
docker create --name "${SRC_CTR}" "${TEST_TAG}" /bin/cp >/dev/null
docker cp "${SRC_CTR}":/autoinstrumentation "${IMAGE_SRC}"

# Link the neutral verifier image to the artifact BY CONSTRUCTION: read the CPython tag from a
# compiled wheel in the payload (e.g. ...cpython-311-...so -> 3.11) rather than hardcoding, so
# the verifier's Python can't silently drift from the Dockerfile's. Fall back to 3.11 if the
# payload has no compiled extension to read a tag from.
pyver=$(find "${IMAGE_SRC}" -name '*.cpython-*-*.so' | head -1 | sed -E 's/.*cpython-([0-9])([0-9]+)-.*/\1.\2/')
NEUTRAL_IMAGE="public.ecr.aws/docker/library/python:${pyver:-3.11}"

# 1. Exercise the image's own cp-utility exactly as the operator init container does:
#    recursively copy the baked-in /autoinstrumentation payload into the shared volume.
docker run --rm --mount source="${VOLUME}",dst=/otel-auto-instrumentation "${TEST_TAG}" \
  /bin/cp -r /autoinstrumentation /otel-auto-instrumentation

# 2. Assert the payload actually landed in the operator volume, using a neutral container
#    (the ADOT image is FROM scratch and has no shell/coreutils).
docker run -d --name "${VERIFY_CTR}" --mount source="${VOLUME}",dst=/otel-auto-instrumentation \
  "${NEUTRAL_IMAGE}" sleep 300 >/dev/null
docker cp "${VERIFY_CTR}":/otel-auto-instrumentation "${VOLUME_COPY}"
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

#    Independent-reference check (optional): compare the ADOT library files baked into the image
#    against the separately-built release wheel -- an artifact from BEFORE the image. Unlike the
#    copy-fidelity check above (image vs a copy of itself), this validates the shipped library
#    against a reference we built independently, matching adot-java's build-artifact comparison.
#    Scoped to the distro source tree we author; __pycache__ (pip-compiled, not in the wheel) is
#    excluded, and the dist-info/RECORD (pip rewrites on install) is out of scope by only diffing
#    amazon/opentelemetry/distro. Transitive deps have no independent artifact, so are not covered.
#    Best-effort: warn and SKIP if the wheel is missing/unreadable so reference acquisition never
#    blocks a release; only a real content mismatch fails.
if [ -n "${WHEEL}" ]; then
  REF="${WORKDIR}/wheel-ref"
  mkdir -p "${REF}"
  if [ ! -f "${WHEEL}" ]; then
    echo "warning: wheel reference '${WHEEL}' not found; skipping independent-reference check"
  elif ! unzip -q "${WHEEL}" -d "${REF}"; then
    echo "warning: could not unzip wheel reference '${WHEEL}'; skipping independent-reference check"
  elif diff -r --exclude='__pycache__' "${REF}/amazon/opentelemetry/distro" "${IMAGE_SRC}/amazon/opentelemetry/distro"; then
    echo "image library matched the independently-built wheel"
  else
    echo "error: image library differs from the independently-built wheel"
    exit 1
  fi
fi



# 4. Copy fidelity: the copied tree must be byte-for-byte identical to the image payload
#    (already extracted to ${IMAGE_SRC} up front).
if diff -r "${IMAGE_SRC}" "${VOLUME_COPY}"; then
  echo "copied autoinstrumentation payload matched the image payload"
else
  echo "error: copied autoinstrumentation payload differs from the image payload"
  exit 1
fi

# -L follows symlinks so the fingerprint matches diff -r semantics: if the image payload holds
# any symlinks, cp-utility -r delivers them as regular files in the volume, and find -type f
# would otherwise count the two sides differently and mis-report a mismatch.
ORIG_CHECKSUM=$(cd "${IMAGE_SRC}" && find -L . -type f -exec sha256sum {} \; | LC_ALL=C sort | sha256sum | cut -d' ' -f1)
COPY_CHECKSUM=$(cd "${VOLUME_COPY}" && find -L . -type f -exec sha256sum {} \; | LC_ALL=C sort | sha256sum | cut -d' ' -f1)
if [ "${COPY_CHECKSUM}" = "${ORIG_CHECKSUM}" ]; then
  echo "copied autoinstrumentation checksum matched (${COPY_CHECKSUM})"
else
  echo "error: copied autoinstrumentation checksum mis-matched (image=${ORIG_CHECKSUM} volume=${COPY_CHECKSUM})"
  exit 1
fi