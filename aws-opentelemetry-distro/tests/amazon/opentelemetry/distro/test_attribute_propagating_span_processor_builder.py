# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)


class TestAttributePropagatingSpanProcessorBuilder(TestCase):
    def test_basic(self):
        builder: AttributePropagatingSpanProcessorBuilder = AttributePropagatingSpanProcessorBuilder()
        self.assertIs(builder.set_propagation_data_key("test"), builder)
