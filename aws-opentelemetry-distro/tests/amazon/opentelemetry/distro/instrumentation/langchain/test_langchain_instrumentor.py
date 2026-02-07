# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import unittest
from unittest import TestCase

from amazon.opentelemetry.distro.instrumentation.langchain import LangChainInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


# https://pypi.org/project/langchain/
@unittest.skipIf(sys.version_info < (3, 10) or sys.version_info >= (4, 0), "langchain requires >=3.10, <4.0")
class TestLangChainInstrumentor(TestCase):
    def setUp(self):
        # pylint: disable=import-outside-toplevel
        from langchain_core.callbacks import BaseCallbackManager
        from langchain_core.outputs import LLMResult

        self.BaseCallbackManager = BaseCallbackManager
        self.LLMResult = LLMResult

        self.tracer_provider = TracerProvider()
        self.span_exporter = InMemorySpanExporter()
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.span_exporter))
        self.instrumentor = LangChainInstrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        self.span_exporter.clear()


if __name__ == "__main__":
    unittest.main()
