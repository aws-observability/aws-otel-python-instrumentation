# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro._aws_span_processing_util import get_ingress_operation
from amazon.opentelemetry.distro.attribute_propagating_span_processor import AttributePropagatingSpanProcessor
from amazon.opentelemetry.distro.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)


class TestAttributePropagatingSpanProcessorBuilder(TestCase):
    def test_basic(self):
        builder: AttributePropagatingSpanProcessorBuilder = AttributePropagatingSpanProcessorBuilder()
        self.assertIs(builder.set_propagation_data_key("test"), builder)
        self.assertIs(builder.set_propagation_data_extractor(get_ingress_operation), builder)
        self.assertIs(builder.set_attributes_keys_to_propagate(["test"]), builder)
        span_processor: AttributePropagatingSpanProcessor = builder.build()
        self.assertIs(span_processor._propagation_data_key, "test")
        self.assertIs(span_processor._propagation_data_extractor, get_ingress_operation)
        self.assertEqual(span_processor._attribute_keys_to_propagate, tuple(["test"]))
