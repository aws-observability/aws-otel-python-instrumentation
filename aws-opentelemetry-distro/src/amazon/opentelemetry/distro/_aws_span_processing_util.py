# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Utility module designed to support shared logic across AWS Span Processors."""
import json
import os
from typing import Dict, List

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_CONSUMER_PARENT_SPAN_KIND, AWS_LOCAL_OPERATION
from opentelemetry.sdk.trace import InstrumentationScope, ReadableSpan
from opentelemetry.semconv.trace import MessagingOperationValues, SpanAttributes
from opentelemetry.trace import SpanKind

# Default attribute values if no valid span attribute value is identified
UNKNOWN_SERVICE: str = "UnknownService"
UNKNOWN_OPERATION: str = "UnknownOperation"
UNKNOWN_REMOTE_SERVICE: str = "UnknownRemoteService"
UNKNOWN_REMOTE_OPERATION: str = "UnknownRemoteOperation"
INTERNAL_OPERATION: str = "InternalOperation"
LOCAL_ROOT: str = "LOCAL_ROOT"

# Useful constants
_SQS_RECEIVE_MESSAGE_SPAN_NAME: str = "Sqs.ReceiveMessage"
_AWS_SDK_INSTRUMENTATION_SCOPE_PREFIX: str = "io.opentelemetry.aws-sdk-"

# Max keyword length supported by parsing into remote_operation from DB_STATEMENT
MAX_KEYWORD_LENGTH = 27


# Get dialect keywords retrieved from dialect_keywords.json file.
# Only meant to be invoked by SQL_KEYWORD_PATTERN and unit tests
def _get_dialect_keywords() -> List[str]:
    current_dir: str = os.path.dirname(__file__)
    file_path: str = os.path.join(current_dir, "configuration/sql_dialect_keywords.json")
    with open(file_path, "r", encoding="utf-8") as json_file:
        keywords_data: Dict[str, str] = json.load(json_file)
    return keywords_data["keywords"]


# A regular expression pattern to match SQL keywords.
SQL_KEYWORD_PATTERN = r"^(?:" + "|".join(_get_dialect_keywords()) + r")\b"


def get_ingress_operation(__, span: ReadableSpan) -> str:
    """
    Ingress operation (i.e. operation for Server and Consumer spans) will be generated from "http.method + http.target/
    with the first API path parameter" if the default span name is None, UnknownOperation or http.method value.
    """
    operation: str = span.name
    if should_use_internal_operation(span):
        operation = INTERNAL_OPERATION
    elif not _is_valid_operation(span, operation):
        operation = _generate_ingress_operation(span)
    return operation


def get_egress_operation(span: ReadableSpan) -> str:
    if should_use_internal_operation(span):
        return INTERNAL_OPERATION
    return span.attributes.get(AWS_LOCAL_OPERATION)


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


def is_aws_sdk_span(span: ReadableSpan) -> bool:
    # https://opentelemetry.io/docs/specs/otel/trace/semantic_conventions/instrumentation/aws-sdk/#common-attributes
    return "aws-api" == span.attributes.get(SpanAttributes.RPC_SYSTEM)


def should_generate_service_metric_attributes(span: ReadableSpan) -> bool:
    return (is_local_root(span) and not _is_sqs_receive_message_consumer_span(span)) or SpanKind.SERVER == span.kind


def should_generate_dependency_metric_attributes(span: ReadableSpan) -> bool:
    return (
        SpanKind.CLIENT == span.kind
        or SpanKind.PRODUCER == span.kind
        or (_is_dependency_consumer_span(span) and not _is_sqs_receive_message_consumer_span(span))
    )


def is_consumer_process_span(span: ReadableSpan) -> bool:
    messaging_operation: str = span.attributes.get(SpanAttributes.MESSAGING_OPERATION)
    return SpanKind.CONSUMER == span.kind and MessagingOperationValues.PROCESS == messaging_operation


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


def _is_sqs_receive_message_consumer_span(span: ReadableSpan) -> bool:
    """To identify the SQS consumer spans produced by AWS SDK instrumentation"""
    messaging_operation: str = span.attributes.get(SpanAttributes.MESSAGING_OPERATION)
    instrumentation_scope: InstrumentationScope = span.instrumentation_scope

    return (
        (span.name is not None and _SQS_RECEIVE_MESSAGE_SPAN_NAME.casefold() == span.name.casefold())
        and SpanKind.CONSUMER == span.kind
        and instrumentation_scope is not None
        and instrumentation_scope.name.startswith(_AWS_SDK_INSTRUMENTATION_SCOPE_PREFIX)
        and (messaging_operation is None or messaging_operation == MessagingOperationValues.PROCESS)
    )


def _is_dependency_consumer_span(span: ReadableSpan) -> bool:
    if SpanKind.CONSUMER != span.kind:
        return False

    if is_consumer_process_span(span):
        if is_local_root(span):
            return True
        parent_span_kind: str = span.attributes.get(AWS_CONSUMER_PARENT_SPAN_KIND)
        return SpanKind.CONSUMER != parent_span_kind

    return True


def _is_valid_operation(span: ReadableSpan, operation: str) -> bool:
    """
    When Span name is null, UnknownOperation or HttpMethod value, it will be treated as invalid local operation value
    that needs to be further processed
    """
    if operation is None or operation == UNKNOWN_OPERATION:
        return False

    if is_key_present(span, SpanAttributes.HTTP_METHOD):
        http_method: str = span.attributes.get(SpanAttributes.HTTP_METHOD)
        return operation != http_method

    return True


def _generate_ingress_operation(span: ReadableSpan) -> str:
    """
    When span name is not meaningful(null, unknown or http_method value) as operation name for http use cases. Will try
    to extract the operation name from http target string
    """
    operation: str = UNKNOWN_OPERATION
    if is_key_present(span, SpanAttributes.HTTP_TARGET):
        http_target: str = span.attributes.get(SpanAttributes.HTTP_TARGET)
        # get the first part from API path string as operation value
        # the more levels/parts we get from API path the higher chance for getting high cardinality data
        if http_target is not None:
            operation = extract_api_path_value(http_target)
            if is_key_present(span, SpanAttributes.HTTP_METHOD):
                http_method: str = span.attributes.get(SpanAttributes.HTTP_METHOD)
                if http_method is not None:
                    operation = http_method + " " + operation

    return operation
