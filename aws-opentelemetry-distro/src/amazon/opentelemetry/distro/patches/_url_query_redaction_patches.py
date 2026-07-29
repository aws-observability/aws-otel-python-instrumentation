# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Patch for OpenTelemetry HTTP URL query-parameter redaction.

Upstream ``opentelemetry.util.http.PARAMS_TO_REDACT`` omits the SigV4/SigV4a query-string
authentication parameters, so an AWS presigned URL captured on a raw HTTP client span would leak
the signature and credential scope via ``url.full`` / ``http.url``. This patch extends that list
in place; because ``redact_query_parameters`` reads it as a module-level global at call time, the
new parameters apply to every HTTP instrumentation that calls ``redact_url``.

Upstream redaction blanks the value and keeps the parameter key, so the non-sensitive SigV4
parameters that presigned URL attribution reads remain intact.
"""

import logging

logger = logging.getLogger(__name__)

# SigV4/SigV4a query-string authentication parameters that carry sensitive material and are not
# already in the upstream default list. Parameter names are matched case-sensitively by
# ``redact_query_parameters`` (it compares against ``parse_qs`` keys), so these must match the
# exact casing AWS emits.
_AWS_SIGV4_SENSITIVE_QUERY_PARAMETERS = [
    "X-Amz-Signature",
    "X-Amz-Credential",
    "X-Amz-Security-Token",
]


def _apply_url_query_redaction_patches() -> None:
    """Extend the upstream sensitive-query-parameter list with AWS SigV4 credentials."""
    try:
        # pylint: disable=import-outside-toplevel
        from opentelemetry.util import http as otel_util_http
    except ImportError:
        logger.warning("Failed to apply URL query redaction patch: opentelemetry.util.http not available")
        return

    params_to_redact = getattr(otel_util_http, "PARAMS_TO_REDACT", None)
    if params_to_redact is None:
        logger.warning("Failed to apply URL query redaction patch: PARAMS_TO_REDACT not available")
        return

    for param in _AWS_SIGV4_SENSITIVE_QUERY_PARAMETERS:
        if param not in params_to_redact:
            params_to_redact.append(param)
