# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from typing import List
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_CONSUMER_PARENT_SPAN_KIND, AWS_LOCAL_OPERATION
from amazon.opentelemetry.distro._aws_span_processing_util import (
    _AWS_LAMBDA_FUNCTION_NAME,
    MAX_KEYWORD_LENGTH,
    _get_dialect_keywords,
    extract_api_path_value,
    get_egress_operation,
    get_ingress_operation,
    is_aws_sdk_span,
    is_consumer_process_span,
    is_key_present,
    is_local_root,
    should_generate_dependency_metric_attributes,
    should_generate_service_metric_attributes,
    should_use_internal_operation,
)
from opentelemetry.sdk.trace import Span, SpanContext
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.semconv.trace import MessagingOperationValues, SpanAttributes
from opentelemetry.trace import SpanKind
from opentelemetry.util.types import Attributes

_UNKNOWN_OPERATION: str = "UnknownOperation"
_INTERNAL_OPERATION: str = "InternalOperation"
_DEFAULT_PATH_VALUE: str = "/"


# pylint: disable=too-many-public-methods
class TestAwsSpanProcessingUtil(TestCase):
    def setUp(self):
        self.attributes_mock: Attributes = MagicMock()
        self.span_data_mock: Span = MagicMock()
        self.span_context_mock: SpanContext = MagicMock()
        self.span_data_mock.get_span_context.return_value = self.span_context_mock
        self.span_data_mock.attributes = self.attributes_mock
        self.attributes_mock.get = MagicMock(return_value=None)

    def test_get_ingress_operation_valid_name(self):
        valid_name: str = "ValidName"
        self.span_data_mock.name = valid_name
        self.span_data_mock.kind = SpanKind.SERVER
        actual_operation: str = get_ingress_operation(self, self.span_data_mock)
        self.assertEqual(actual_operation, valid_name)

    def test_get_ingress_operation_with_not_server(self):
        valid_name: str = "ValidName"
        self.span_data_mock.name = valid_name
        self.span_data_mock.kind = SpanKind.CLIENT
        actual_operation: str = get_ingress_operation(self, self.span_data_mock)
        self.assertEqual(actual_operation, _INTERNAL_OPERATION)

    @patch.dict(os.environ, {_AWS_LAMBDA_FUNCTION_NAME: "MyLambda"})
    def test_get_ingress_operation_in_lambda(self):
        valid_name: str = "ValidName"
        self.span_data_mock.name = valid_name
        self.span_data_mock.kind = SpanKind.SERVER
        actual_operation: str = get_ingress_operation(self, self.span_data_mock)
        self.assertEqual(actual_operation, "MyLambda/FunctionHandler")

    def test_get_ingress_operation_http_method_name_and_no_fallback(self):
        invalid_name: str = "GET"
        self.span_data_mock.name = invalid_name
        self.span_data_mock.kind = SpanKind.SERVER

        def attributes_get_side_effect(key):
            if key == SpanAttributes.HTTP_METHOD:
                return invalid_name
            return None

        self.attributes_mock.get.side_effect = attributes_get_side_effect
        actual_operation: str = get_ingress_operation(self, self.span_data_mock)
        self.assertEqual(actual_operation, _UNKNOWN_OPERATION)

    def test_get_ingress_operation_null_name_and_no_fallback(self):
        invalid_name: str = None
        self.span_data_mock.name = invalid_name
        self.span_data_mock.kind = SpanKind.SERVER
        actual_operation: str = get_ingress_operation(self, self.span_data_mock)
        self.assertEqual(actual_operation, _UNKNOWN_OPERATION)

    def test_get_ingress_operation_unknown_name_and_no_fallback(self):
        invalid_name: str = _UNKNOWN_OPERATION
        self.span_data_mock.name = invalid_name
        self.span_data_mock.kind = SpanKind.SERVER
        actual_operation: str = get_ingress_operation(self, self.span_data_mock)
        self.assertEqual(actual_operation, _UNKNOWN_OPERATION)

    def test_get_ingress_operation_invalid_name_and_valid_target(self):
        invalid_name = None
        valid_target: str = "/"
        self.span_data_mock.name = invalid_name
        self.span_data_mock.kind = SpanKind.SERVER

        def attributes_get_side_effect(key):
            if key == SpanAttributes.HTTP_TARGET:
                return valid_target
            return None

        self.attributes_mock.get.side_effect = attributes_get_side_effect
        actual_operation = get_ingress_operation(self, self.span_data_mock)
        self.assertEqual(actual_operation, valid_target)

    def test_get_ingress_operation_invalid_name_and_valid_target_and_method(self):
        invalid_name = None
        valid_target: str = "/"
        valid_method: str = "GET"
        self.span_data_mock.name = invalid_name
        self.span_data_mock.kind = SpanKind.SERVER

        def attributes_get_side_effect(key):
            if key == SpanAttributes.HTTP_TARGET:
                return valid_target
            if key == SpanAttributes.HTTP_METHOD:
                return valid_method
            return None

        self.attributes_mock.get.side_effect = attributes_get_side_effect
        actual_operation = get_ingress_operation(self, self.span_data_mock)
        expected_operation = f"{valid_method} {valid_target}"
        self.assertEqual(actual_operation, expected_operation)

    def test_get_egress_operation_use_internal_operation(self):
        invalid_name = None
        self.span_data_mock.name = invalid_name
        self.span_data_mock.kind = SpanKind.CONSUMER

        actual_operation = get_egress_operation(self.span_data_mock)
        self.assertEqual(actual_operation, _INTERNAL_OPERATION)

    def test_get_egress_operation_get_local_operation(self):
        operation: str = "TestOperation"

        def attributes_get_side_effect(key):
            if key == AWS_LOCAL_OPERATION:
                return operation
            return None

        self.attributes_mock.get.side_effect = attributes_get_side_effect
        self.span_data_mock.kind = SpanKind.SERVER

        actual_operation = get_egress_operation(self.span_data_mock)
        self.assertEqual(actual_operation, operation)

    def test_is_key_present_key_present(self):
        def attributes_get_side_effect(key):
            if key == SpanAttributes.HTTP_TARGET:
                return "target"
            return None

        self.attributes_mock.get.side_effect = attributes_get_side_effect
        self.assertTrue(is_key_present(self.span_data_mock, SpanAttributes.HTTP_TARGET))

    def test_is_key_present_key_absent(self):
        self.attributes_mock.get.return_value = None
        self.assertFalse(is_key_present(self.span_data_mock, "HTTP_TARGET"))

    def test_is_aws_span_true(self):
        def attributes_get_side_effect(key):
            if key == SpanAttributes.RPC_SYSTEM:
                return "aws-api"
            return None

        self.attributes_mock.get.side_effect = attributes_get_side_effect
        self.assertTrue(is_aws_sdk_span(self.span_data_mock))

    def test_is_aws_span_false(self):
        self.attributes_mock.get.return_value = None
        self.assertFalse(is_aws_sdk_span(self.span_data_mock))

    def test_should_use_internal_operation_false(self):
        self.span_data_mock.kind = SpanKind.SERVER
        self.assertFalse(should_use_internal_operation(self.span_data_mock))

        parent_span_context: SpanContext = MagicMock()
        parent_span_context.is_remote = False
        parent_span_context.is_valid = True

        self.span_data_mock.kind = SpanKind.CONSUMER
        self.span_data_mock.parent = parent_span_context

        self.assertFalse(should_use_internal_operation(self.span_data_mock))

    def test_extract_api_path_value_empty_target(self):
        invalid_target = ""
        path_value = extract_api_path_value(invalid_target)
        self.assertEqual(path_value, _DEFAULT_PATH_VALUE)

    def test_extract_api_path_value_null_target(self):
        invalid_target = None
        path_value = extract_api_path_value(invalid_target)
        self.assertEqual(path_value, _DEFAULT_PATH_VALUE)

    def test_extract_api_path_value_no_slash(self):
        invalid_target = "users"
        path_value = extract_api_path_value(invalid_target)
        self.assertEqual(path_value, _DEFAULT_PATH_VALUE)

    def test_extract_api_path_value_only_slash(self):
        invalid_target = "/"
        path_value = extract_api_path_value(invalid_target)
        self.assertEqual(path_value, _DEFAULT_PATH_VALUE)

    def test_extract_api_path_value_only_slash_at_end(self):
        invalid_target = "users/"
        path_value = extract_api_path_value(invalid_target)
        self.assertEqual(path_value, _DEFAULT_PATH_VALUE)

    def test_extract_api_path_valid_path(self):
        valid_target = "/users/1/pet?query#fragment"
        path_value = extract_api_path_value(valid_target)
        self.assertEqual(path_value, "/users")

    def test_should_generate_service_metric_attributes(self):
        parent_span_context: SpanContext = MagicMock()
        parent_span_context.is_remote = False
        parent_span_context.is_valid = True
        self.span_data_mock.parent = parent_span_context

        self.span_data_mock.kind = SpanKind.SERVER
        self.assertTrue(should_generate_service_metric_attributes(self.span_data_mock))

        self.span_data_mock.kind = SpanKind.CONSUMER
        self.assertFalse(should_generate_service_metric_attributes(self.span_data_mock))

        self.span_data_mock.kind = SpanKind.INTERNAL
        self.assertFalse(should_generate_service_metric_attributes(self.span_data_mock))

        self.span_data_mock.kind = SpanKind.PRODUCER
        self.assertFalse(should_generate_service_metric_attributes(self.span_data_mock))

        self.span_data_mock.kind = SpanKind.CLIENT
        self.assertFalse(should_generate_service_metric_attributes(self.span_data_mock))

        # It's a local root, so should return true
        parent_span_context.is_remote = True
        self.span_data_mock.kind = SpanKind.PRODUCER
        self.span_data_mock.parent_span_context = parent_span_context
        self.assertTrue(should_generate_service_metric_attributes(self.span_data_mock))

    def test_should_generate_dependency_metric_attributes(self):
        self.span_data_mock.kind = SpanKind.SERVER
        self.assertFalse(should_generate_dependency_metric_attributes(self.span_data_mock))

        self.span_data_mock.kind = SpanKind.INTERNAL
        self.assertFalse(should_generate_dependency_metric_attributes(self.span_data_mock))

        self.span_data_mock.kind = SpanKind.CONSUMER
        self.assertTrue(should_generate_dependency_metric_attributes(self.span_data_mock))

        self.span_data_mock.kind = SpanKind.PRODUCER
        self.assertTrue(should_generate_dependency_metric_attributes(self.span_data_mock))

        self.span_data_mock.kind = SpanKind.CLIENT
        self.assertTrue(should_generate_dependency_metric_attributes(self.span_data_mock))

        parent_span_context_mock = MagicMock()
        parent_span_context_mock.is_valid = True
        parent_span_context_mock.is_remote = False
        self.span_data_mock.kind = SpanKind.CONSUMER
        self.span_data_mock.parent = parent_span_context_mock

        def attributes_get_side_effect(key):
            if key == SpanAttributes.MESSAGING_OPERATION:
                return MessagingOperationValues.PROCESS
            if key == AWS_CONSUMER_PARENT_SPAN_KIND:
                return SpanKind.CONSUMER
            return None

        self.attributes_mock.get.side_effect = attributes_get_side_effect

        self.assertFalse(should_generate_dependency_metric_attributes(self.span_data_mock))

        parent_span_context_mock.is_valid = False
        self.assertTrue(should_generate_dependency_metric_attributes(self.span_data_mock))

    def test_is_local_root(self):
        # Parent Context is empty
        self.assertTrue(is_local_root(self.span_data_mock))

        parent_span_context = MagicMock()
        self.span_data_mock.parent = parent_span_context

        parent_span_context.is_remote = False
        parent_span_context.is_valid = True
        self.assertFalse(is_local_root(self.span_data_mock))

        parent_span_context.is_remote = True
        parent_span_context.is_valid = True
        self.assertTrue(is_local_root(self.span_data_mock))

        parent_span_context.is_remote = False
        parent_span_context.is_valid = False
        self.assertTrue(is_local_root(self.span_data_mock))

        parent_span_context.is_remote = True
        parent_span_context.is_valid = False
        self.assertTrue(is_local_root(self.span_data_mock))

    def test_is_consumer_process_span_false(self):
        self.assertFalse(is_consumer_process_span(self.span_data_mock))

    def test_is_consumer_process_span_true(self):
        def attributes_get_side_effect(key):
            if key == SpanAttributes.MESSAGING_OPERATION:
                return MessagingOperationValues.PROCESS
            return None

        self.attributes_mock.get.side_effect = attributes_get_side_effect
        self.span_data_mock.kind = SpanKind.CONSUMER

        self.assertTrue(is_consumer_process_span(self.span_data_mock))

    # check that boto3 SQS spans metrics are suppressed
    def test_no_metric_attributes_for_boto3sqs_producer_span(self):
        instrumentation_scope_mock: InstrumentationScope = MagicMock()
        instrumentation_scope_mock.name = "opentelemetry.instrumentation.boto3sqs"
        self.span_data_mock.instrumentation_scope = instrumentation_scope_mock
        self.span_data_mock.kind = SpanKind.PRODUCER
        self.span_data_mock.name = "testQueue send"
        self.assertFalse(should_generate_service_metric_attributes(self.span_data_mock))
        self.assertFalse(should_generate_dependency_metric_attributes(self.span_data_mock))

    def test_no_metric_attributes_for_boto3sqs_consumer_span(self):
        instrumentation_scope_mock: InstrumentationScope = MagicMock()
        instrumentation_scope_mock.name = "opentelemetry.instrumentation.boto3sqs"
        self.span_data_mock.instrumentation_scope = instrumentation_scope_mock
        self.span_data_mock.kind = SpanKind.CONSUMER
        self.span_data_mock.name = "testQueue receive"

        self.assertFalse(should_generate_service_metric_attributes(self.span_data_mock))
        self.assertFalse(should_generate_dependency_metric_attributes(self.span_data_mock))

    def test_no_metric_attributes_for_boto3sqs_process_span(self):
        instrumentation_scope_info_mock = MagicMock()
        instrumentation_scope_info_mock.name = "opentelemetry.instrumentation.boto3sqs"
        self.span_data_mock.instrumentation_scope = instrumentation_scope_info_mock
        self.span_data_mock.kind = SpanKind.CONSUMER
        self.span_data_mock.name = "testQueue process"

        def attributes_get_side_effect_process(key):
            if key == SpanAttributes.MESSAGING_OPERATION:
                return MessagingOperationValues.PROCESS
            return None

        self.attributes_mock.get.side_effect = attributes_get_side_effect_process
        self.span_data_mock.attributes = self.attributes_mock

        self.assertFalse(should_generate_service_metric_attributes(self.span_data_mock))
        self.assertFalse(should_generate_dependency_metric_attributes(self.span_data_mock))

    # check that consumer spans metrics are still generated for other instrumentation
    def test_metric_attributes_generated_for_instrumentation_other_than_boto3sqs(self):
        instrumentation_scope_info_mock = MagicMock()
        instrumentation_scope_info_mock.name = "my-instrumentation"
        self.span_data_mock.instrumentation_scope = instrumentation_scope_info_mock
        self.span_data_mock.kind = SpanKind.CONSUMER
        self.span_data_mock.name = "testQueue receive"

        self.assertTrue(should_generate_service_metric_attributes(self.span_data_mock))
        self.assertTrue(should_generate_dependency_metric_attributes(self.span_data_mock))

    def test_sql_dialect_keywords_order(self):
        keywords: List[str] = _get_dialect_keywords()
        prev_char_length: int = None
        for keyword in keywords:
            cur_char_length: int = len(keyword)
            # Confirm the keywords are sorted based on descending order of keywords character length
            if prev_char_length is not None:
                self.assertGreaterEqual(prev_char_length, cur_char_length)
            prev_char_length = cur_char_length

    # Confirm maximum length of keywords is not longer than MAX_KEYWORD_LENGTH
    def test_sql_dialect_keywords_max_length(self):
        keywords: List[str] = _get_dialect_keywords()
        for keyword in keywords:
            self.assertLessEqual(len(keyword), MAX_KEYWORD_LENGTH)
