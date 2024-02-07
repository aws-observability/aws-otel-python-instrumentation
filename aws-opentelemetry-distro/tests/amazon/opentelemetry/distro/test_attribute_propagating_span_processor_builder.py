# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.distro.attribute_propagating_span_processor import AttributePropagatingSpanProcessor
from amazon.opentelemetry.distro.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)
from opentelemetry.sdk.trace import ReadableSpan


class TestAttributePropagatingSpanProcessorBuilder(TestCase):
    def test_basic(self):
        builder: AttributePropagatingSpanProcessorBuilder = AttributePropagatingSpanProcessorBuilder()
        self.assertIs(builder.set_propagation_data_key("test"), builder)

        def mock_extractor(_: ReadableSpan) -> str:
            return "test"

        self.assertIs(builder.set_propagation_data_extractor(mock_extractor), builder)
        self.assertIs(builder.set_attributes_keys_to_propagate(["test"]), builder)
        span_processor: AttributePropagatingSpanProcessor = builder.build()
        self.assertIs(span_processor._propagation_data_key, "test")
        self.assertEqual(span_processor._propagation_data_extractor(MagicMock()), "test")
        self.assertEqual(span_processor._attribute_keys_to_propagate, tuple(["test"]))
