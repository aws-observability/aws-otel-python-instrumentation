# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from trace import Trace
from typing import Callable
from unittest import TestCase

from opentelemetry.context import Context
from opentelemetry.sdk.resources import Attributes
from opentelemetry.sdk.trace import ReadableSpan, Tracer, TracerProvider, Span
from opentelemetry.trace import set_tracer_provider, SpanKind

from amazon.opentelemetry.distro._aws_span_processing_util import get_ingress_operation
from amazon.opentelemetry.distro.attribute_propagating_span_processor import AttributePropagatingSpanProcessor


def _get_ingress_operation(span: Span):
    return get_ingress_operation(None, span)


_SPAN_NAME_EXTRACTOR: Callable[[ReadableSpan], str] = _get_ingress_operation
_SPAN_NAME_KEY: str = "span_name"
_TEST_KEY_1: str = "key1"
_TEST_KEY_2: str = "key2"


class TestAttributePropagatingSpanProcessor(TestCase):
    def setUp(self):
        self.processor: AttributePropagatingSpanProcessor = AttributePropagatingSpanProcessor(_SPAN_NAME_EXTRACTOR, _SPAN_NAME_KEY, (_TEST_KEY_1, _TEST_KEY_2,))
        self.provider: TracerProvider = TracerProvider(
            id_generator=None,
            sampler=None,
            resource=None,
        )
        set_tracer_provider(self.provider)
        self.provider.add_span_processor(self.processor)
        self.tracer: Tracer = self.provider.get_tracer("awsxray")

    def test_attributes_propagation_by_spankind(self):
        for span_kind in SpanKind:
            span_with_app_only: Span = self.tracer.start_span(name="parent", kind=span_kind, attributes={_TEST_KEY_1: "TestValue1"})
            span_with_op_only: Span = self.tracer.start_span(name="parent", kind=span_kind, attributes={_TEST_KEY_2: "TestValue2"})
            span_with_app_and_op: Span = self.tracer.start_span(name="parent", kind=span_kind, attributes={_TEST_KEY_1: "TestValue1", _TEST_KEY_2: "TestValue2"})

            if span_kind == SpanKind.SERVER:
                self._validate_span_attributes_inheritance(span_with_app_only, "parent", None, None)
                self._validate_span_attributes_inheritance(span_with_op_only, "parent", None, None)
                self._validate_span_attributes_inheritance(span_with_app_and_op, "parent", None, None)

    def _create_nested_span(self, parent_span: Span, depth: int) -> Span:
        if depth == 0:
            return parent_span
        child_span: Span = self.tracer.start_span(name="child:" + str(depth))
        child_span._parent = parent_span
        try:
            return self._create_nested_span(child_span, depth - 1)
        finally:
            child_span.end()

    def _validate_span_attributes_inheritance(self, parent_span: Span, propageted_name: str, propagation_value1: str, propagation_value2: str):
        leaf_span: ReadableSpan = self._create_nested_span(parent_span, 10)
        self.assertIsNotNone(leaf_span.parent)
        self.assertEqual(leaf_span.name, "child:1")
        if propageted_name is not None:
            self.assertEqual(leaf_span.attributes[_SPAN_NAME_KEY], propageted_name)
        else:
            self.assertIsNone(leaf_span.attributes[_SPAN_NAME_KEY])

