# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Utility module designed to support shared logic across AWS Span Processors."""
import json
import os
from typing import Dict, List
from urllib.parse import ParseResult, urlparse

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
_AWS_LAMBDA_FUNCTION_NAME: str = "AWS_LAMBDA_FUNCTION_NAME"
_BOTO3SQS_INSTRUMENTATION_SCOPE: str = "opentelemetry.instrumentation.boto3sqs"

# Max keyword length supported by parsing into remote_operation from DB_STATEMENT
MAX_KEYWORD_LENGTH = 27

# TODO: Use Semantic Conventions once upgrade to 0.47b0
GEN_AI_REQUEST_MODEL: str = "gen_ai.request.model"
GEN_AI_SYSTEM: str = "gen_ai.system"
GEN_AI_REQUEST_MAX_TOKENS: str = "gen_ai.request.max_tokens"
GEN_AI_REQUEST_TEMPERATURE: str = "gen_ai.request.temperature"
GEN_AI_REQUEST_TOP_P: str = "gen_ai.request.top_p"
GEN_AI_RESPONSE_FINISH_REASONS: str = "gen_ai.response.finish_reasons"
GEN_AI_USAGE_INPUT_TOKENS: str = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS: str = "gen_ai.usage.output_tokens"


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
    if _AWS_LAMBDA_FUNCTION_NAME in os.environ:
        operation = os.environ.get(_AWS_LAMBDA_FUNCTION_NAME) + "/FunctionHandler"
    elif should_use_internal_operation(span):
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


# Check if the current Span adheres to database semantic conventions
def is_db_span(span: ReadableSpan) -> bool:
    return (
        is_key_present(span, SpanAttributes.DB_SYSTEM)
        or is_key_present(span, SpanAttributes.DB_OPERATION)
        or is_key_present(span, SpanAttributes.DB_STATEMENT)
    )


def should_generate_service_metric_attributes(span: ReadableSpan) -> bool:
    return (is_local_root(span) and not _is_boto3sqs_span(span)) or SpanKind.SERVER == span.kind


def should_generate_dependency_metric_attributes(span: ReadableSpan) -> bool:
    return (
        SpanKind.CLIENT == span.kind
        or (SpanKind.PRODUCER == span.kind and not _is_boto3sqs_span(span))
        or (_is_dependency_consumer_span(span) and not _is_boto3sqs_span(span))
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


def _is_boto3sqs_span(span: ReadableSpan) -> bool:
    """
    To identify if the span produced is from the boto3sqs instrumentation.
    We use this to identify the boto3sqs spans and not generate metrics from the since we will generate
    the same metrics from botocore spans.
    """
    # TODO: Evaluate if we can bring the boto3sqs spans back to generate metrics and not have to suppress them.
    instrumentation_scope: InstrumentationScope = span.instrumentation_scope
    return (
        instrumentation_scope is not None
        and instrumentation_scope.name is not None
        and _BOTO3SQS_INSTRUMENTATION_SCOPE.casefold() == instrumentation_scope.name.casefold()
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
    When span name is not meaningful, this method is invoked to try to extract the operation name from either
    `http.target`, if present, or from `http.url`, and combine with `http.method`.
    """
    operation: str = UNKNOWN_OPERATION
    http_path: str = None
    if is_key_present(span, SpanAttributes.HTTP_TARGET):
        http_path = span.attributes.get(SpanAttributes.HTTP_TARGET)
    elif is_key_present(span, SpanAttributes.HTTP_URL):
        http_url = span.attributes.get(SpanAttributes.HTTP_URL)
        url: ParseResult = urlparse(http_url)
        http_path = url.path

    # get the first part from API path string as operation value
    # the more levels/parts we get from API path the higher chance for getting high cardinality data
    if http_path is not None:
        operation = extract_api_path_value(http_path)
        if is_key_present(span, SpanAttributes.HTTP_METHOD):
            http_method: str = span.attributes.get(SpanAttributes.HTTP_METHOD)
            if http_method is not None:
                operation = http_method + " " + operation

    return operation
