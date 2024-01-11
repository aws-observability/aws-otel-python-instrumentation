# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.attribute_propagating_span_processor import AttributePropagatingSpanProcessor


class TestAttributePropagatingSpanProcessor(TestCase):
    def test_basic(self):
        processor: AttributePropagatingSpanProcessor = AttributePropagatingSpanProcessor(None, None, None)
        self.assertTrue(processor.force_flush)
