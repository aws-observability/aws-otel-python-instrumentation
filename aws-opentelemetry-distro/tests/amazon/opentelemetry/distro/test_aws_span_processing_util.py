# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from opentelemetry.semconv.trace import SpanAttributes

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_LOCAL_OPERATION, AWS_CONSUMER_PARENT_SPAN_KIND
from amazon.opentelemetry.distro._aws_span_processing_util import (
    get_ingress_operation,
    get_egress_operation,
    extract_api_path_value,
    is_key_present,
    is_aws_sdk_span,
    should_use_internal_operation,
    should_generate_service_metric_attributes,
    should_generate_dependency_metric_attributes,
    is_local_root,
    is_consumer_process_span,
)
from opentelemetry.util.types import Attributes
from opentelemetry.trace import SpanKind
from opentelemetry.sdk.trace import Span, SpanContext
from opentelemetry.semconv.trace import MessagingOperationValues
from opentelemetry.sdk.util.instrumentation import InstrumentationScope


class TestAwsSpanProcessingUtil(TestCase):
    DEFAULT_PATH_VALUE: str = "/"
    UNKNOWN_OPERATION: str = "UnknownOperation"
    INTERNAL_OPERATION: str = "InternalOperation"

    def setUp(self):
        self.attributes_mock: Attributes = MagicMock()
        self.span_data_mock: Span = MagicMock()
        self.span_context_mock: SpanContext = MagicMock()
        self.span_data_mock.get_span_context.return_value = self.span_context_mock
        self.span_data_mock.attributes = self.attributes_mock

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
        self.assertEqual(actual_operation, self.INTERNAL_OPERATION)

    def test_get_ingress_operation_http_method_name_and_no_fallback(self):
        invalid_name: str = "GET"
        self.span_data_mock.name = invalid_name
        self.span_data_mock.kind = SpanKind.SERVER

        def mock_get(key):
            if key == SpanAttributes.HTTP_METHOD:
                return invalid_name
            else:
                return None

        self.attributes_mock.get.side_effect = mock_get
        actual_operation: str = get_ingress_operation(self, self.span_data_mock)
        self.assertEqual(actual_operation, self.UNKNOWN_OPERATION)

    def test_get_ingress_operation_invalid_name_and_valid_target(self):
        invalid_name = None
        valid_target = "/"
        self.span_data_mock.name = invalid_name
        self.span_data_mock.kind = SpanKind.SERVER

        def mock_get(key):
            if key == SpanAttributes.HTTP_TARGET:
                return valid_target
            else:
                return None

        self.attributes_mock.get.side_effect = mock_get
        actual_operation = get_ingress_operation(self, self.span_data_mock)
        self.assertEqual(actual_operation, valid_target)

    def test_get_ingress_operation_invalid_name_and_valid_target_and_method(self):
        invalid_name = None
        valid_target = "/"
        valid_method = "GET"
        self.span_data_mock.name = invalid_name
        self.span_data_mock.kind = SpanKind.SERVER

        def mock_get(key):
            if key == SpanAttributes.HTTP_TARGET:
                return valid_target
            elif key == SpanAttributes.HTTP_METHOD:
                return valid_method
            else:
                return None

        self.attributes_mock.get.side_effect = mock_get
        actual_operation = get_ingress_operation(self, self.span_data_mock)
        expected_operation = f"{valid_method} {valid_target}"
        self.assertEqual(actual_operation, expected_operation)

    def test_get_egress_operation_use_internal_operation(self):
        invalid_name = None
        self.span_data_mock.name = invalid_name
        self.span_data_mock.kind = SpanKind.CONSUMER

        actual_operation = get_egress_operation(self.span_data_mock)
        self.assertEqual(actual_operation, self.INTERNAL_OPERATION)

    def test_get_egress_operation_get_local_operation(self):
        operation = "TestOperation"

        def mock_get(key):
            if key == AWS_LOCAL_OPERATION:
                return operation
            else:
                return None

        self.attributes_mock.get.side_effect = mock_get
        self.span_data_mock.attributes = self.attributes_mock
        self.span_data_mock.kind = SpanKind.SERVER

        actual_operation = get_egress_operation(self.span_data_mock)
        self.assertEqual(actual_operation, operation)

    def test_extract_api_path_value_empty_target(self):
        invalid_target = ""
        path_value = extract_api_path_value(invalid_target)
        self.assertEqual(path_value, self.DEFAULT_PATH_VALUE)

    def test_extract_api_path_value_null_target(self):
        invalid_target = None
        path_value = extract_api_path_value(invalid_target)
        self.assertEqual(path_value, self.DEFAULT_PATH_VALUE)

    def test_extract_api_path_value_no_slash(self):
        invalid_target = "users"
        path_value = extract_api_path_value(invalid_target)
        self.assertEqual(path_value, self.DEFAULT_PATH_VALUE)

    def test_extract_api_path_value_only_slash(self):
        invalid_target = "/"
        path_value = extract_api_path_value(invalid_target)
        self.assertEqual(path_value, self.DEFAULT_PATH_VALUE)

    def test_extract_api_path_value_only_slash_at_end(self):
        invalid_target = "users/"
        path_value = extract_api_path_value(invalid_target)
        self.assertEqual(path_value, self.DEFAULT_PATH_VALUE)

    def test_extract_api_path_valid_path(self):
        valid_target = "/users/1/pet?query#fragment"
        path_value = extract_api_path_value(valid_target)
        self.assertEqual(path_value, "/users")

    def test_is_key_present_key_present(self):
        self.attributes_mock.get.return_value = "target"
        self.span_data_mock.attributes = self.attributes_mock
        self.assertTrue(is_key_present(self.span_data_mock, "HTTP_TARGET"))

    def test_is_key_present_key_absent(self):
        self.attributes_mock.get.return_value = None
        self.span_data_mock.attributes = self.attributes_mock
        self.assertFalse(is_key_present(self.span_data_mock, "HTTP_TARGET"))

    def test_is_aws_span_true(self):
        self.attributes_mock.get.return_value = "aws-api"
        self.span_data_mock.attributes = self.attributes_mock
        self.assertTrue(is_aws_sdk_span(self.span_data_mock))

    def test_is_aws_span_false(self):
        self.attributes_mock.get.return_value = None
        self.span_data_mock.attributes = self.attributes_mock
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

        def attributes_side_effect(key):
            if key == SpanAttributes.MESSAGING_OPERATION:
                return MessagingOperationValues.PROCESS
            elif key == AWS_CONSUMER_PARENT_SPAN_KIND:
                return SpanKind.CONSUMER
            return None

        self.attributes_mock.get.side_effect = attributes_side_effect
        self.span_data_mock.attributes = self.attributes_mock

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
        def attributes_side_effect(key):
            if key == SpanAttributes.MESSAGING_OPERATION:
                return MessagingOperationValues.PROCESS
            return None

        self.attributes_mock.get.side_effect = attributes_side_effect
        self.span_data_mock.attributes = self.attributes_mock
        self.span_data_mock.kind = SpanKind.CONSUMER

        self.assertTrue(is_consumer_process_span(self.span_data_mock))

    def test_no_metric_attributes_for_sqs_consumer_span_aws_sdk_v1(self):
        instrumentation_scope_mock: InstrumentationScope = MagicMock()
        instrumentation_scope_mock.name = "io.opentelemetry.aws-sdk-1.11"
        self.span_data_mock.instrumentation_scope = instrumentation_scope_mock
        self.span_data_mock.kind = SpanKind.CONSUMER
        self.span_data_mock.name = "SQS.ReceiveMessage"
        self.span_data_mock.attributes.get.return_value = MessagingOperationValues.PROCESS

        self.assertFalse(should_generate_service_metric_attributes(self.span_data_mock))
        self.assertFalse(should_generate_dependency_metric_attributes(self.span_data_mock))

    def test_no_metric_attributes_for_sqs_consumer_span_aws_sdk_v2(self):
        instrumentation_scope_mock: InstrumentationScope = MagicMock()
        instrumentation_scope_mock.name = "io.opentelemetry.aws-sdk-2.2"
        self.span_data_mock.instrumentation_scope = instrumentation_scope_mock
        self.span_data_mock.kind = SpanKind.CONSUMER
        self.span_data_mock.name = "SQS.ReceiveMessage"
        self.span_data_mock.attributes.get.return_value = MessagingOperationValues.PROCESS

        self.assertFalse(should_generate_service_metric_attributes(self.span_data_mock))
        self.assertFalse(should_generate_dependency_metric_attributes(self.span_data_mock))

    def test_metric_attributes_generated_for_other_instrumentation_sqs_consumer_span(self):
        instrumentation_scope_info_mock = MagicMock()
        instrumentation_scope_info_mock.get_name.return_value = "my-instrumentation"
        self.span_data_mock.instrumentation_scope_info = instrumentation_scope_info_mock
        self.span_data_mock.kind = SpanKind.CONSUMER
        self.span_data_mock.name = "Sqs.ReceiveMessage"

        self.assertTrue(should_generate_service_metric_attributes(self.span_data_mock))
        self.assertTrue(should_generate_dependency_metric_attributes(self.span_data_mock))

    def test_no_metric_attributes_for_aws_sdk_sqs_consumer_process_span(self):
        instrumentation_scope_info_mock = MagicMock()
        instrumentation_scope_info_mock.get_name.return_value = "io.opentelemetry.aws-sdk-2.2"
        self.span_data_mock.instrumentation_scope_info = instrumentation_scope_info_mock
        self.span_data_mock.kind = SpanKind.CONSUMER
        self.span_data_mock.name = "Sqs.ReceiveMessage"

        def attributes_side_effect(key):
            if key == SpanAttributes.MESSAGING_OPERATION:
                return MessagingOperationValues.PROCESS
            return None

        self.attributes_mock.get.side_effect = attributes_side_effect
        self.span_data_mock.attributes = self.attributes_mock

        self.assertFalse(should_generate_service_metric_attributes(self.span_data_mock))
        self.assertFalse(should_generate_dependency_metric_attributes(self.span_data_mock))
        self.attributes_mock.get.side_effect = (
                lambda key: MessagingOperationValues.RECEIVE if key == SpanAttributes.MESSAGING_OPERATION else None
        )

        self.assertTrue(should_generate_service_metric_attributes(self.span_data_mock))
        self.assertTrue(should_generate_dependency_metric_attributes(self.span_data_mock))