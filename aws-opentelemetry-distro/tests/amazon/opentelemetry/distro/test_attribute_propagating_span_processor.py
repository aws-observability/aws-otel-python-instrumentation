# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from trace import Trace
from typing import Callable
from unittest import TestCase

from opentelemetry.sdk.trace import ReadableSpan, Tracer

from amazon.opentelemetry.distro._aws_span_processing_util import get_ingress_operation
from amazon.opentelemetry.distro.attribute_propagating_span_processor import AttributePropagatingSpanProcessor

_SPAN_NAME_EXTRACTOR: Callable[[ReadableSpan], str] = get_ingress_operation
_SPAN_NAME_KEY: str = "span_name"
_TEST_KEY_1: str = "key1"
_TEST_KEY_2: str = "key2"

class TestAttributePropagatingSpanProcessor(TestCase):
    def setUp(self):
        self.processor: AttributePropagatingSpanProcessor = AttributePropagatingSpanProcessor(_SPAN_NAME_EXTRACTOR, _SPAN_NAME_KEY, (_TEST_KEY_1, _TEST_KEY_2, ))
        self.tracer: Tracer
        self.tracer

