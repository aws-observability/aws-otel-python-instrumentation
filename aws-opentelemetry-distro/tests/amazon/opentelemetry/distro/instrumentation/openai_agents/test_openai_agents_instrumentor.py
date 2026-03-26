# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from importlib.metadata import entry_points
from unittest import TestCase

from opentelemetry.instrumentation.openai_agents import OpenAIAgentsInstrumentor


class TestOpenAIAgentsInstrumentation(TestCase):

    def test_aws_openai_agents_entry_point_resolves(self):
        eps = entry_points(group="opentelemetry_instrumentor", name="aws_openai_agents")
        self.assertEqual(len(list(eps)), 1)

        ep = list(eps)[0]
        instrumentor_class = ep.load()
        self.assertIs(instrumentor_class, OpenAIAgentsInstrumentor)

    def test_aws_entry_point_survives_when_openai_agents_disabled(self):
        disabled = {"openai_agents"}

        aws_eps = entry_points(group="opentelemetry_instrumentor", name="aws_openai_agents")
        otel_eps = entry_points(group="opentelemetry_instrumentor", name="openai_agents")

        self.assertTrue(any(ep.name in disabled for ep in otel_eps))
        self.assertFalse(any(ep.name in disabled for ep in aws_eps))

        ep = list(aws_eps)[0]
        instrumentor_class = ep.load()
        self.assertIs(instrumentor_class, OpenAIAgentsInstrumentor)

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
