# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro._delegating_readable_span import _DelegatingReadableSpan
from opentelemetry.sdk.trace import ReadableSpan

# from opentelemetry.trace import INVALID_SPAN_CONTEXT


class TestDelegatingReadableSpan(TestCase):
    def test_basic(self):
        readable_span: ReadableSpan = ReadableSpan(name="name", context=None)
        delegating_readable_span: _DelegatingReadableSpan = _DelegatingReadableSpan(readable_span)
        self.assertEqual(delegating_readable_span.name, "name")
