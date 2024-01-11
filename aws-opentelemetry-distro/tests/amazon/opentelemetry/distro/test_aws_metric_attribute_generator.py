# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator


class TestAwsMetricAttributeGenerator(TestCase):
    def test_basic(self):
        generator: _AwsMetricAttributeGenerator = _AwsMetricAttributeGenerator()
        self.assertEqual(generator.generate_metric_attributes_dict_from_span(None, None), {})
