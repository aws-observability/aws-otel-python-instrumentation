# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from opentelemetry.sdk.trace import ReadableSpan

from amazon.opentelemetry.distro.attribute_propagating_span_processor import AttributePropagatingSpanProcessor
from amazon.opentelemetry.distro.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)


class TestAttributePropagatingSpanProcessorBuilder(TestCase):
    def test_basic(self):
        builder: AttributePropagatingSpanProcessorBuilder = AttributePropagatingSpanProcessorBuilder()
        span_mock: ReadableSpan = MagicMock()
        span_mock._attributes = {"key": "value"}

        def extractor(span: ReadableSpan) -> str:
            return next(iter(span._attributes.values()))

        self.assertIs(builder.set_propagation_data_key("test"), builder)

        builder.set_attributes_keys_to_propagate(["test key"])
        builder.set_propagation_data_extractor(extractor)

        processor: AttributePropagatingSpanProcessor = builder.build()
        self.assertEqual(processor._attribute_keys_to_propagate, ("test key",))
        self.assertEqual(processor._propagation_data_key, "test")
        self.assertEqual(processor._propagation_data_extractor(span_mock), "value")
