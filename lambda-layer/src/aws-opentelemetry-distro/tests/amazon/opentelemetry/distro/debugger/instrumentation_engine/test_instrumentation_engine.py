# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Common test utilities for instrumentation engine tests."""

import unittest
from unittest.mock import Mock

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


class InstrumentationEngineTestBase(unittest.TestCase):
    """Base testing class for instrumentation engines."""

    def _setup_engine(self):
        """Setup method to be called by test classes. Override to set specific engine."""
        pass

    def setUp(self):
        self._setup_engine()
        self.callback = Mock(return_value=True)
        self.callback.__name__ = "test_callback"

    def tearDown(self):
        self.engine.cleanup()

    def _create_test_function(self):
        """Create a simple test function for instrumentation."""

        def test_func(x, y=10):
            z = x + y
            return z

        return test_func

    def _create_test_method(self):
        """Create a class with a method for instrumentation."""

        class TestClass:
            def method(self, x, y=10):
                z = x + y
                return z

        return TestClass, TestClass.method

    def _create_test_inner_function(self):
        """Create an inner/nested function for instrumentation."""

        def outer():
            def inner(x, y=10):
                z = x + y
                return z

            return inner

        return outer()

    def _create_test_static_method(self):
        """Create a class with a static method for instrumentation."""

        class TestClass:
            @staticmethod
            def static_method(x, y=10):
                z = x + y
                return z

        return TestClass, TestClass.static_method

    def _create_test_tracer(self):
        """Create a tracer with in-memory exporter for testing."""
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        return provider.get_tracer("test"), exporter
