# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Optional
from unittest import TestCase

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_CONSUMER_PARENT_SPAN_KIND, AWS_SDK_DESCENDANT
from amazon.opentelemetry.distro._aws_span_processing_util import get_ingress_operation
from amazon.opentelemetry.distro.attribute_propagating_span_processor import AttributePropagatingSpanProcessor
from opentelemetry.sdk.trace import ReadableSpan, Span, Tracer, TracerProvider
from opentelemetry.semconv.trace import MessagingOperationValues, SpanAttributes
from opentelemetry.trace import SpanContext, SpanKind, TraceFlags, TraceState, set_span_in_context


def _get_ingress_operation(span: Span):
    return get_ingress_operation(None, span)


_SPAN_NAME_EXTRACTOR: Callable[[ReadableSpan], str] = _get_ingress_operation
_SPAN_NAME_KEY: str = "span_name"
_TEST_KEY_1: str = "key1"
_TEST_KEY_2: str = "key2"


class TestAttributePropagatingSpanProcessor(TestCase):
    def setUp(self):
        self.processor: AttributePropagatingSpanProcessor = AttributePropagatingSpanProcessor(
            _SPAN_NAME_EXTRACTOR,
            _SPAN_NAME_KEY,
            (
                _TEST_KEY_1,
                _TEST_KEY_2,
            ),
        )
        self.provider: TracerProvider = TracerProvider(
            id_generator=None,
            sampler=None,
            resource=None,
        )
        self.provider.add_span_processor(self.processor)
        self.tracer: Tracer = self.provider.get_tracer("awsxray")

    def test_attributes_propagation_by_spankind(self):
        for span_kind in SpanKind:
            span_with_app_only: Span = self.tracer.start_span(
                name="parent", kind=span_kind, attributes={_TEST_KEY_1: "TestValue1"}
            )
            span_with_op_only: Span = self.tracer.start_span(
                name="parent", kind=span_kind, attributes={_TEST_KEY_2: "TestValue2"}
            )
            span_with_app_and_op: Span = self.tracer.start_span(
                name="parent", kind=span_kind, attributes={_TEST_KEY_1: "TestValue1", _TEST_KEY_2: "TestValue2"}
            )

            if span_kind == SpanKind.SERVER:
                self._validate_span_attributes_inheritance(span_with_app_only, "parent", None, None)
                self._validate_span_attributes_inheritance(span_with_op_only, "parent", None, None)
                self._validate_span_attributes_inheritance(span_with_app_and_op, "parent", None, None)
            elif span_kind == SpanKind.INTERNAL:
                self._validate_span_attributes_inheritance(span_with_app_only, "InternalOperation", "TestValue1", None)
                self._validate_span_attributes_inheritance(span_with_op_only, "InternalOperation", None, "TestValue2")
                self._validate_span_attributes_inheritance(
                    span_with_app_and_op, "InternalOperation", "TestValue1", "TestValue2"
                )
            else:
                self._validate_span_attributes_inheritance(span_with_app_only, "InternalOperation", None, None)
                self._validate_span_attributes_inheritance(span_with_op_only, "InternalOperation", None, None)
                self._validate_span_attributes_inheritance(span_with_app_and_op, "InternalOperation", None, None)

    def test_attributes_propagation_with_internal_kinds(self):
        grand_parent_span: Span = self.tracer.start_span(
            name="grandparent", kind=SpanKind.INTERNAL, attributes={_TEST_KEY_1: "testValue1"}
        )
        parent_span: Span = self.tracer.start_span(
            name="parent",
            kind=SpanKind.INTERNAL,
            attributes={_TEST_KEY_2: "testValue2"},
            context=set_span_in_context(grand_parent_span),
        )
        child_span: Span = self.tracer.start_span(
            name="child", kind=SpanKind.CLIENT, context=set_span_in_context(parent_span)
        )
        grand_child_span: Span = self.tracer.start_span(
            name="child", kind=SpanKind.INTERNAL, context=set_span_in_context(child_span)
        )

        self.assertEqual(grand_parent_span.attributes.get(_TEST_KEY_1), "testValue1")
        self.assertIsNone(grand_parent_span.attributes.get(_TEST_KEY_2))
        self.assertEqual(parent_span.attributes.get(_TEST_KEY_1), "testValue1")
        self.assertEqual(parent_span.attributes.get(_TEST_KEY_2), "testValue2")
        self.assertEqual(child_span.attributes.get(_TEST_KEY_1), "testValue1")
        self.assertEqual(child_span.attributes.get(_TEST_KEY_2), "testValue2")
        self.assertIsNone(grand_child_span.attributes.get(_TEST_KEY_1))
        self.assertIsNone(grand_child_span.attributes.get(_TEST_KEY_2))

    def test_override_attributes(self):
        parent_span: Span = self.tracer.start_span(name="parent", kind=SpanKind.SERVER)
        parent_span.set_attribute(_TEST_KEY_1, "testValue1")
        parent_span.set_attribute(_TEST_KEY_2, "testValue2")

        transmit_spans_1: Span = self._create_nested_span(parent_span, 2)

        child_span: Span = self.tracer.start_span(name="child:1", context=set_span_in_context(transmit_spans_1))

        child_span.set_attribute(_TEST_KEY_2, "testValue3")

        transmit_spans_2: Span = self._create_nested_span(child_span, 2)

        self.assertEqual(transmit_spans_2.attributes.get(_TEST_KEY_2), "testValue3")

    def test_span_name_propagation_by_span_kind(self):
        for value in SpanKind:
            span: Span = self.tracer.start_span(name="parent", kind=value)
            if value == SpanKind.SERVER:
                self._validate_span_attributes_inheritance(span, "parent", None, None)
            else:
                self._validate_span_attributes_inheritance(span, "InternalOperation", None, None)

    def test_span_name_propagation_with_remote_parent_span(self):
        remote_parent_context: SpanContext = SpanContext(1, 2, True, TraceFlags.SAMPLED, TraceState.get_default())
        # Don't have a Span.Wrap(SpanContext) like method from Java, create a readable span instead
        remote_parent_span: Span = ReadableSpan(remote_parent_context)
        span: Span = self.tracer.start_span(
            name="parent", kind=SpanKind.SERVER, context=set_span_in_context(remote_parent_span)
        )
        self._validate_span_attributes_inheritance(span, "parent", None, None)

    def test_aws_sdk_descendant_span(self):
        aws_sdk_span: Span = self.tracer.start_span(
            name="parent", kind=SpanKind.CLIENT, attributes={SpanAttributes.RPC_SYSTEM: "aws-api"}
        )
        self.assertIsNone(aws_sdk_span.attributes.get(AWS_SDK_DESCENDANT))
        child_span: Span = self._create_nested_span(aws_sdk_span, 1)
        self.assertIsNotNone(child_span.attributes.get(AWS_SDK_DESCENDANT))
        self.assertEqual(child_span.attributes.get(AWS_SDK_DESCENDANT), "true")

    def test_consumer_parent_span_kind_attribute_propagation(self):
        grand_parent_span: Span = self.tracer.start_span(name="grandparent", kind=SpanKind.CONSUMER)
        parent_span: Span = self.tracer.start_span(
            name="parent", kind=SpanKind.INTERNAL, context=set_span_in_context(grand_parent_span)
        )
        child_span: Span = self.tracer.start_span(
            name="child",
            kind=SpanKind.CONSUMER,
            attributes={SpanAttributes.MESSAGING_OPERATION: MessagingOperationValues.PROCESS},
            context=set_span_in_context(parent_span),
        )
        self.assertIsNone(parent_span.attributes.get(AWS_CONSUMER_PARENT_SPAN_KIND))
        self.assertIsNone(child_span.attributes.get(AWS_CONSUMER_PARENT_SPAN_KIND))

    def test_no_consumer_parent_span_kind_attribute_with_consumer_process(self):
        parent_span: Span = self.tracer.start_span(name="parent", kind=SpanKind.SERVER)
        child_span: Span = self.tracer.start_span(
            name="child",
            kind=SpanKind.CONSUMER,
            attributes={SpanAttributes.MESSAGING_OPERATION: MessagingOperationValues.PROCESS},
            context=set_span_in_context(parent_span),
        )
        self.assertIsNone(child_span.attributes.get(AWS_CONSUMER_PARENT_SPAN_KIND))

    def test_consumer_parent_span_kind_attribute_with_consumer_parent(self):
        parent_span: Span = self.tracer.start_span(name="parent", kind=SpanKind.CONSUMER)
        child_span: Span = self.tracer.start_span(
            name="parent", kind=SpanKind.CONSUMER, context=set_span_in_context(parent_span)
        )
        self.assertEqual(child_span.attributes.get(AWS_CONSUMER_PARENT_SPAN_KIND), SpanKind.CONSUMER.name)

    def _create_nested_span(self, parent_span: Span, depth: int) -> Span:
        if depth == 0:
            return parent_span
        child_span: Span = self.tracer.start_span(name="child:" + str(depth), context=set_span_in_context(parent_span))
        try:
            return self._create_nested_span(child_span, depth - 1)
        finally:
            child_span.end()

    def _validate_span_attributes_inheritance(
        self,
        parent_span: Span,
        propagated_name: Optional[str] = None,
        propagation_value1: Optional[str] = None,
        propagation_value2: Optional[str] = None,
    ):
        leaf_span: ReadableSpan = self._create_nested_span(parent_span, 10)
        self.assertIsNotNone(leaf_span.parent)
        self.assertEqual(leaf_span.name, "child:1")
        if propagated_name is not None:
            self.assertEqual(propagated_name, leaf_span.attributes.get(_SPAN_NAME_KEY))
        else:
            self.assertIsNone(leaf_span.attributes.get(_SPAN_NAME_KEY))
        if propagation_value1 is not None:
            self.assertEqual(propagation_value1, leaf_span.attributes.get(_TEST_KEY_1))
        else:
            self.assertIsNone(leaf_span.attributes.get(_TEST_KEY_1))
        if propagation_value2 is not None:
            self.assertEqual(propagation_value2, leaf_span.attributes.get(_TEST_KEY_2))
        else:
            self.assertIsNone(leaf_span.attributes.get(_TEST_KEY_2))

    def test_attributes_propagation_cloud_resource_id(self):
        cloud_resource_id = "arn:x1"
        grand_parent_span: Span = self.tracer.start_span(
            name="grandparent", kind=SpanKind.INTERNAL, attributes={_TEST_KEY_1: "testValue1"}
        )
        parent_span: Span = self.tracer.start_span(
            name="parent",
            kind=SpanKind.SERVER,
            attributes={_TEST_KEY_2: "testValue2", SpanAttributes.CLOUD_RESOURCE_ID: cloud_resource_id},
            context=set_span_in_context(grand_parent_span),
        )
        child_span: Span = self.tracer.start_span(
            name="child", kind=SpanKind.INTERNAL, context=set_span_in_context(parent_span)
        )
        grand_child_span: Span = self.tracer.start_span(
            name="child", kind=SpanKind.CLIENT, context=set_span_in_context(child_span)
        )

        self.assertIsNone(grand_parent_span.attributes.get(SpanAttributes.CLOUD_RESOURCE_ID))
        self.assertIsNotNone(parent_span.attributes.get(SpanAttributes.CLOUD_RESOURCE_ID))
        self.assertEqual(child_span.attributes.get(SpanAttributes.CLOUD_RESOURCE_ID), cloud_resource_id)
        self.assertEqual(grand_child_span.attributes.get(SpanAttributes.CLOUD_RESOURCE_ID), cloud_resource_id)
