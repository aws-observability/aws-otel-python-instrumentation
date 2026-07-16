# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Span-processing helpers shared between ServiceEvents and the AWS distro.

Historically these lived in ``amazon.opentelemetry.distro._aws_span_processing_util``.
They were moved here when ServiceEvents was extracted into ``aws-opentelemetry-components``
so the dependency runs one way (``aws-opentelemetry-distro`` -> ``aws-opentelemetry-components``);
the distro imports these back from this module.
"""
import os
from urllib.parse import ParseResult, urlparse

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind

# Default attribute values if no valid span attribute value is identified
UNKNOWN_OPERATION: str = "UnknownOperation"
INTERNAL_OPERATION: str = "InternalOperation"

# Useful constants
_AWS_LAMBDA_FUNCTION_NAME: str = "AWS_LAMBDA_FUNCTION_NAME"


def get_ingress_operation(__, span: ReadableSpan) -> str:
    """
    Ingress operation (i.e. operation for Server and Consumer spans) will be generated from "http.method + http.target/
    with the first API path parameter" if the default span name is None, UnknownOperation or http.method value.
    """
    operation: str = span.name
    scope = getattr(span, "instrumentation_scope", None)
    if _AWS_LAMBDA_FUNCTION_NAME in os.environ and scope.name != "opentelemetry.instrumentation.flask":
        operation = os.environ.get(_AWS_LAMBDA_FUNCTION_NAME) + "/FunctionHandler"
    elif should_use_internal_operation(span):
        operation = INTERNAL_OPERATION
    elif not _is_valid_operation(span, operation):
        operation = _generate_ingress_operation(span)
    return operation


def extract_api_path_value(http_target: str) -> str:
    """Extract the first part from API http target if it exists

    Args
        http_target - http request target string value. Eg, /payment/1234
    Returns
        the first part from the http target. Eg, /payment
    :return:
    """
    if http_target is None or len(http_target) == 0:
        return "/"
    paths: [str] = http_target.split("/")
    if len(paths) > 1:
        return "/" + paths[1]
    return "/"


def is_key_present(span: ReadableSpan, key: str) -> bool:
    return span.attributes.get(key) is not None


def should_use_internal_operation(span: ReadableSpan) -> bool:
    """
    Any spans that are Local Roots and also not SERVER should have aws.local.operation renamed toInternalOperation.
    """
    return is_local_root(span) and not SpanKind.SERVER == span.kind


def is_local_root(span: ReadableSpan) -> bool:
    """
    A span is a local root if it has no parent or if the parent is remote. This function checks the parent context
    and returns true if it is a local root.
    """
    return span.parent is None or not span.parent.is_valid or span.parent.is_remote


def _get_http_method(span: ReadableSpan) -> str:
    """Get the HTTP method from the span, checking http.request.method first, then http.method (deprecated)."""
    if span.attributes is None:
        return None
    method = span.attributes.get(SpanAttributes.HTTP_REQUEST_METHOD)
    if method is not None:
        return method
    return span.attributes.get(SpanAttributes.HTTP_METHOD)


def _is_valid_operation(span: ReadableSpan, operation: str) -> bool:
    """
    When Span name is null, UnknownOperation or HttpMethod value, it will be treated as invalid local operation value
    that needs to be further processed
    """
    if operation is None or operation == UNKNOWN_OPERATION:
        return False

    http_method: str = _get_http_method(span)
    if http_method:
        return operation != http_method

    return True


def _generate_ingress_operation(span: ReadableSpan) -> str:
    """
    When span name is not meaningful, this method is invoked to try to extract the operation name from the
    request path — legacy `http.target`/`http.url` first, then stable `url.path`/`url.full` — combined with
    the HTTP method (legacy `http.method` or stable `http.request.method`).

    The stable (`url.path`/`url.full`, `http.request.method`) fallbacks matter for apps that opt into
    stable-only HTTP semconv (OTEL_SEMCONV_STABILITY_OPT_IN=http), where the legacy attributes are absent.
    Without them, an unmatched/scanner request (e.g. GET /wp-admin with no matched route) would yield
    UnknownOperation and be dropped instead of tracked as `GET /wp-admin`. Matches the Node distro's
    generateIngressOperation, which already reads both attribute families.
    """
    operation: str = UNKNOWN_OPERATION
    http_path: str = None
    if is_key_present(span, SpanAttributes.HTTP_TARGET):
        http_path = span.attributes.get(SpanAttributes.HTTP_TARGET)
    elif is_key_present(span, SpanAttributes.HTTP_URL):
        http_url = span.attributes.get(SpanAttributes.HTTP_URL)
        url: ParseResult = urlparse(http_url)
        http_path = url.path
    elif is_key_present(span, SpanAttributes.URL_PATH):
        http_path = span.attributes.get(SpanAttributes.URL_PATH)
    elif is_key_present(span, SpanAttributes.URL_FULL):
        url: ParseResult = urlparse(span.attributes.get(SpanAttributes.URL_FULL))
        http_path = url.path

    # get the first part from API path string as operation value
    # the more levels/parts we get from API path the higher chance for getting high cardinality data
    if http_path is not None:
        operation = extract_api_path_value(http_path)
        # Method: legacy http.method first, then stable http.request.method.
        http_method = span.attributes.get(SpanAttributes.HTTP_METHOD) or span.attributes.get(
            SpanAttributes.HTTP_REQUEST_METHOD
        )
        if http_method is not None:
            operation = http_method + " " + operation

    return operation
