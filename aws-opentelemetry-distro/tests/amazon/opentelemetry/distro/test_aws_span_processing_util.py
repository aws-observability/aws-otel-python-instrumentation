# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro._aws_span_processing_util import is_key_present
from opentelemetry.sdk.trace import ReadableSpan


class TestAwsSpanProcessingUtil(TestCase):
    def test_basic(self):
        span: ReadableSpan = ReadableSpan(name="test")
        self.assertFalse(is_key_present(span, "test"))
