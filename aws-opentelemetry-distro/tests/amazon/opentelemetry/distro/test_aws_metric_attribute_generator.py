# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from opentelemetry.sdk.trace import ReadableSpan, Resource
from opentelemetry.trace import SpanContext


class TestAwsMetricAttributeGenerator(TestCase):
    def test_basic(self):
        generator: _AwsMetricAttributeGenerator = _AwsMetricAttributeGenerator()
        context: SpanContext = SpanContext(1, 1, False)
        parent_context: SpanContext = SpanContext(1, 1, False)
        span: ReadableSpan = ReadableSpan("test", context, parent_context)
        resource: Resource = Resource({})
        self.assertEqual(generator.generate_metric_attributes_dict_from_span(span, resource), {})
