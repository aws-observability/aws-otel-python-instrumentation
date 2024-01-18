# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_CONSUMER_PARENT_SPAN_KIND
from amazon.opentelemetry.distro._aws_span_processing_util import (
    is_consumer_process_span,
    is_local_root,
    should_generate_dependency_metric_attributes,
    should_generate_service_metric_attributes,
)
from opentelemetry.sdk.trace import Span, SpanContext
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.semconv.trace import MessagingOperationValues, SpanAttributes
from opentelemetry.trace import SpanKind
from opentelemetry.util.types import Attributes


class TestAwsSpanProcessingUtilHelper(TestCase):
    DEFAULT_PATH_VALUE: str = "/"
    UNKNOWN_OPERATION: str = "UnknownOperation"
    INTERNAL_OPERATION: str = "InternalOperation"

    def setUp(self):
        self.attributes_mock: Attributes = MagicMock()
        self.span_data_mock: Span = MagicMock()
        self.span_context_mock: SpanContext = MagicMock()
        self.span_data_mock.get_span_context.return_value = self.span_context_mock
        self.span_data_mock.attributes = self.attributes_mock

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
            if key == AWS_CONSUMER_PARENT_SPAN_KIND:
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
