# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SigV4 credential provider for the upstream OTel SDK credential-provider hook.

Registered under the ``opentelemetry_otlp_credential_provider`` entry point as
``aws_sigv4``. Selected by setting any of (Python SDK experimental env vars,
see opentelemetry-python#4689):

    OTEL_PYTHON_EXPORTER_OTLP_HTTP_TRACES_CREDENTIAL_PROVIDER=aws_sigv4
    OTEL_PYTHON_EXPORTER_OTLP_HTTP_LOGS_CREDENTIAL_PROVIDER=aws_sigv4
    OTEL_PYTHON_EXPORTER_OTLP_HTTP_CREDENTIAL_PROVIDER=aws_sigv4

The factory returns an ``AwsAuthSession`` (a ``requests.Session`` subclass) that
re-signs every request via botocore's credential chain — so credential rotation
(IRSA, IMDS, SSO, container roles) is handled transparently, no app restart.

Signal detection: OTel's hook calls the factory with no args, so the same
``aws_sigv4`` name can serve traces / logs / metrics. The factory walks the
call stack to find the OTLP HTTP exporter module that triggered it (stable
module path substrings ``trace_exporter`` / ``_log_exporter`` /
``metric_exporter``) and uses that to pick the right endpoint env per signal.

Per-signal signing service resolution (highest priority first):
    1. ``AWS_SIGV4_SERVICE`` (explicit user override; applies to all signals)
    2. Inferred from the matched signal's OTLP endpoint URL:
       - ``https://xray.<region>.amazonaws.com/v1/traces`` -> ``xray``
       - ``https://logs.<region>.amazonaws.com/v1/logs``   -> ``logs``
       - host contains ``cloudwatch``                       -> ``cloudwatch``
    3. No service resolved -> the factory returns an unsigned session and
       logs a warning, instead of silently signing under a default service
       that the AWS endpoint may reject.

Region comes from the standard AWS resolution chain (``AWS_REGION`` /
``AWS_DEFAULT_REGION`` / ``~/.aws/config`` / IMDS).
"""

import inspect
import logging
import os
import re
from typing import Optional, Tuple
from urllib.parse import urlparse

import requests

from amazon.opentelemetry.distro._utils import IS_BOTOCORE_INSTALLED, get_aws_region, get_aws_session
from amazon.opentelemetry.distro.exporter.otlp.aws.common._aws_http_headers import _OTLP_AWS_HTTP_HEADERS
from amazon.opentelemetry.distro.exporter.otlp.aws.common.aws_auth_session import AwsAuthSession
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
)

_logger = logging.getLogger(__name__)

AWS_SIGV4_SERVICE = "AWS_SIGV4_SERVICE"

# Signal -> (endpoint env, module-path substring used to detect the calling
# OTLP HTTP exporter on the stack). Substrings match the upstream OTel layout:
#   opentelemetry.exporter.otlp.proto.http.trace_exporter
#   opentelemetry.exporter.otlp.proto.http._log_exporter
#   opentelemetry.exporter.otlp.proto.http.metric_exporter
_SIGNAL_TABLE: Tuple[Tuple[str, str, str], ...] = (
    ("traces", OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, "trace_exporter"),
    ("logs", OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, "log_exporter"),
    ("metrics", OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, "metric_exporter"),
)

# Endpoint-URL rules mapped to SigV4 signing service names. First match wins.
# The xray and logs URL patterns are kept identical to the ones already used
# by the configurator's auto-detection (AWS_TRACES_OTLP_ENDPOINT_PATTERN /
# AWS_LOGS_OTLP_ENDPOINT_PATTERN) so the two paths agree on what counts as an
# AWS endpoint.
_AWS_TRACES_OTLP_ENDPOINT_PATTERN = re.compile(r"https://xray\.([a-z0-9-]+)\.amazonaws\.com/v1/traces$")
_AWS_LOGS_OTLP_ENDPOINT_PATTERN = re.compile(r"https://logs\.([a-z0-9-]+)\.amazonaws\.com/v1/logs$")
_INFERENCE_RULES = (
    (lambda endpoint, host: bool(_AWS_TRACES_OTLP_ENDPOINT_PATTERN.match(endpoint)), "xray"),
    (lambda endpoint, host: bool(_AWS_LOGS_OTLP_ENDPOINT_PATTERN.match(endpoint)), "logs"),
    (lambda endpoint, host: "cloudwatch" in host, "cloudwatch"),
)


def aws_sigv4_session() -> requests.Session:
    """Factory for the ``aws_sigv4`` credential provider entry point.

    Signal-aware: the same factory backs traces / logs / metrics; it walks the
    call stack to discover which OTLP HTTP exporter module triggered the call
    and uses the matching endpoint env to resolve the SigV4 service.

    Falls back to a plain ``requests.Session`` (with the ADOT user-agent) when
    botocore is unavailable or no AWS region can be resolved, so the exporter
    still functions instead of crashing at startup.
    """
    signal = _detect_signal_from_stack()
    service = _resolve_signing_service(signal)
    if not service:
        _logger.warning(
            "Credential provider 'aws_sigv4' is selected but no SigV4 signing service "
            "could be resolved for the OTLP %s endpoint; set AWS_SIGV4_SERVICE explicitly. "
            "Using an unsigned session.",
            signal or "endpoint",
        )
        return _unsigned_fallback_session()

    if not IS_BOTOCORE_INSTALLED:
        _logger.warning("Credential provider 'aws_sigv4' requires botocore to be installed; using an unsigned session.")
        return _unsigned_fallback_session()

    region = get_aws_region()
    if not region:
        _logger.warning(
            "Credential provider 'aws_sigv4' is selected but no AWS region is set; "
            "configure AWS_REGION / AWS_DEFAULT_REGION or your AWS profile. "
            "Using an unsigned session."
        )
        return _unsigned_fallback_session()

    botocore_session = get_aws_session()
    session = AwsAuthSession(session=botocore_session, aws_region=region, service=service)
    session.headers.update(_OTLP_AWS_HTTP_HEADERS)
    return session


def _detect_signal_from_stack() -> Optional[str]:
    """Walk the call stack to identify which OTLP HTTP exporter triggered us.

    Matches loosely on module-path substrings (e.g. ``trace_exporter``) so the
    detection is resilient to OTel SDK class renames as long as the module
    layout stays put. Returns ``None`` if no OTLP HTTP exporter frame is found
    (e.g. when called outside an exporter __init__, such as from tests), in
    which case service resolution falls through to the default.
    """
    try:
        for frame_info in inspect.stack():
            module_name = frame_info.frame.f_globals.get("__name__", "")
            for signal, _endpoint_env, module_substring in _SIGNAL_TABLE:
                if module_substring in module_name:
                    return signal
    except Exception:  # pylint: disable=broad-except
        # Stack inspection should never break exporter setup. Fall through to
        # the default service.
        return None
    return None


def _resolve_signing_service(signal: Optional[str]) -> Optional[str]:
    """Resolve the SigV4 signing service in declared priority order.

    Returns ``None`` if neither the explicit env nor URL inference yields a
    service — callers must treat that as "do not sign" instead of falling
    back to a hard-coded default that may not match the configured endpoint.
    """
    explicit = os.environ.get(AWS_SIGV4_SERVICE, "").strip()
    if explicit:
        return explicit

    inferred = _infer_signing_service_from_endpoint(signal)
    if inferred:
        _logger.info(
            "AWS_SIGV4_SERVICE not set; inferred SigV4 service '%s' from the OTLP %s endpoint.",
            inferred,
            signal or "endpoint",
        )
        return inferred

    return None


def _infer_signing_service_from_endpoint(signal: Optional[str]) -> Optional[str]:
    """Best-effort inference of the SigV4 service from the matched signal's endpoint.

    When we know the signal, we read its specific endpoint env (with a fallback
    to the protocol-wide ``OTEL_EXPORTER_OTLP_ENDPOINT``). When we don't, we
    only consult the generic env to avoid leaking one signal's endpoint into
    another's signing decision.
    """
    endpoint = ""
    if signal is not None:
        for sig, env_name, _ in _SIGNAL_TABLE:
            if sig == signal:
                endpoint = os.environ.get(env_name, "").strip()
                break
    if not endpoint:
        endpoint = os.environ.get(OTEL_EXPORTER_OTLP_ENDPOINT, "").strip()
    if not endpoint:
        return None

    host = (urlparse(endpoint).hostname or "").lower()

    for matches, service in _INFERENCE_RULES:
        if matches(endpoint, host):
            return service
    return None


def _unsigned_fallback_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(_OTLP_AWS_HTTP_HEADERS)
    return session
