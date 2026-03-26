# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

from opentelemetry.instrumentation.openai_agents import OpenAIAgentsInstrumentor


class TestOpenAIAgentsInstrumentation(TestCase):

    def test_aws_openai_agents_entry_point_resolves(self):
        from importlib.metadata import entry_points

        eps = entry_points(group="opentelemetry_instrumentor", name="aws_openai_agents")
        self.assertEqual(len(list(eps)), 1)

        ep = list(eps)[0]
        instrumentor_class = ep.load()
        self.assertIs(instrumentor_class, OpenAIAgentsInstrumentor)

    def test_instrumentor_can_be_instantiated(self):
        instrumentor = OpenAIAgentsInstrumentor()
        self.assertIsNotNone(instrumentor)

    def test_instrument_called_twice_only_sets_processor_once(self):
        instrumentor = OpenAIAgentsInstrumentor()
        if instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.uninstrument()

        instrumentor.instrument()
        first_processor = instrumentor._processor
        self.assertIsNotNone(first_processor)

        instrumentor.instrument()
        self.assertIs(instrumentor._processor, first_processor)

        instrumentor.uninstrument()
        self.assertIsNone(instrumentor._processor)
