# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List, Optional
from urllib.parse import unquote, urlparse

from amazon.opentelemetry.distro.presigned_aws_url import PresignedAwsUrl
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.semconv.trace import SpanAttributes

# SigV4 query-string authentication parameters.
# https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
_X_AMZ_ALGORITHM = "X-Amz-Algorithm"
_X_AMZ_CREDENTIAL = "X-Amz-Credential"
_X_AMZ_SIGNATURE = "X-Amz-Signature"
_X_AMZ_DATE = "X-Amz-Date"
_X_AMZ_EXPIRES = "X-Amz-Expires"
_X_AMZ_SIGNED_HEADERS = "X-Amz-SignedHeaders"
_SIGV4_ALGORITHMS = frozenset({"AWS4-HMAC-SHA256", "AWS4-ECDSA-P256-SHA256"})


class PresignedAwsUrlParser:
    """Recognizes a SigV4/SigV4a presigned AWS URL from a span's URL.

    Detection relies only on non-sensitive signals. A presigned (query-authenticated) request
    carries all six SigV4 query parameters: ``X-Amz-Algorithm``, ``X-Amz-Credential``,
    ``X-Amz-Signature``, ``X-Amz-Date``, ``X-Amz-Expires`` and ``X-Amz-SignedHeaders``. Of these,
    only the ``X-Amz-Algorithm`` value is inspected (against an allowlist); ``X-Amz-Credential`` and
    ``X-Amz-Signature`` are required to be present with a non-empty value but their values are never
    read, because the agent's default URL sanitization replaces them with ``REDACTED`` before metric
    attribution runs. ``X-Amz-Date``, ``X-Amz-Expires`` and ``X-Amz-SignedHeaders`` are never
    redacted, so requiring them provides cheap verification. The signing service is identified
    downstream from the endpoint hostname, not from the credential scope.
    """

    @staticmethod
    def parse_span(span: ReadableSpan) -> Optional[PresignedAwsUrl]:
        if span.attributes is None:
            return None
        # URL: stable ``url.full`` first, then legacy ``http.url``.
        url = span.attributes.get(SpanAttributes.URL_FULL)
        if url is None:
            url = span.attributes.get(SpanAttributes.HTTP_URL)
        # Method: stable ``http.request.method`` first, then legacy ``http.method``.
        http_method = span.attributes.get(SpanAttributes.HTTP_REQUEST_METHOD)
        if http_method is None:
            http_method = span.attributes.get(SpanAttributes.HTTP_METHOD)
        return PresignedAwsUrlParser.parse(url, http_method)

    @staticmethod
    def parse(url: Optional[str], http_method: Optional[str]) -> Optional[PresignedAwsUrl]:
        if not url:
            return None

        try:
            parsed = urlparse(url)
        except ValueError:
            return None

        if not parsed.hostname:
            return None

        query_parameters = _parse_query_parameters(parsed.query)
        if not _is_presigned_sigv4_request(query_parameters):
            return None

        return PresignedAwsUrl(parsed.hostname, parsed.path, http_method, query_parameters)


def _is_presigned_sigv4_request(query_parameters: Dict[str, List[str]]) -> bool:
    """A request is a presigned SigV4/SigV4a request when it carries the signing algorithm,
    credential and signature parameters together with the presigned query parameters that AWS always
    includes (``X-Amz-Date``, ``X-Amz-Expires``, ``X-Amz-SignedHeaders``). Only the algorithm value
    is inspected against an allowlist; the credential and signature must be present with a value but
    the value itself is not read, because sanitization replaces it with a non-empty ``REDACTED``.
    Empty values are rejected as malformed.
    """
    algorithm = _get_first_value(query_parameters, _X_AMZ_ALGORITHM)
    return (
        algorithm is not None
        and algorithm in _SIGV4_ALGORITHMS
        and _has_non_empty_value(query_parameters, _X_AMZ_CREDENTIAL)
        and _has_non_empty_value(query_parameters, _X_AMZ_SIGNATURE)
        and _has_non_empty_value(query_parameters, _X_AMZ_DATE)
        and _has_non_empty_value(query_parameters, _X_AMZ_EXPIRES)
        and _has_non_empty_value(query_parameters, _X_AMZ_SIGNED_HEADERS)
    )


def _has_non_empty_value(query_parameters: Dict[str, List[str]], name: str) -> bool:
    value = _get_first_value(query_parameters, name)
    return value is not None and value != ""


def _get_first_value(query_parameters: Dict[str, List[str]], name: str) -> Optional[str]:
    values = query_parameters.get(name)
    if not values:
        return None
    return values[0]


def _parse_query_parameters(raw_query: str) -> Dict[str, List[str]]:
    query_parameters: Dict[str, List[str]] = {}
    if not raw_query:
        return query_parameters

    for pair in raw_query.split("&"):
        delimiter_index = pair.find("=")
        if delimiter_index >= 0:
            name = pair[:delimiter_index]
            value = pair[delimiter_index + 1 :]
        else:
            name = pair
            value = ""
        query_parameters.setdefault(_decode(name), []).append(_decode(value))
    return query_parameters


def _decode(value: str) -> str:
    return unquote(value)
