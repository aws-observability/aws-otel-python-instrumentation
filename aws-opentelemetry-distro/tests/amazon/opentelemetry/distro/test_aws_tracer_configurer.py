# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import time
from unittest import TestCase

from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from opentelemetry.sdk.trace import TracerProvider, Tracer, Span

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import AwsOpenTelemetryConfigurator


class TestAwsTracerConfigurer(TestCase):
    def setUp(self):
        self.span_exporter: InMemorySpanExporter = InMemorySpanExporter()
        self.simple_span_processor: SimpleSpanProcessor = SimpleSpanProcessor(self.span_exporter)
        self.tracer_provider: TracerProvider = TracerProvider()
        self.tracer_provider.add_span_processor(self.simple_span_processor)
        self.aws_otel_configurator: AwsOpenTelemetryConfigurator = AwsOpenTelemetryConfigurator()
        self.aws_otel_configurator.configure()

    def test_provide_generate_xray_ids(self):
        for i in range(20):
            tracer: Tracer = self.tracer_provider.get_tracer("test")
            start_time_sec: int = int(time.time())
            span: Span = tracer.start_span("test")
            trace_id: int = span.get_span_context().trace_id
            self.assertGreater(trace_id, start_time_sec)

    def test_trace_id_ratio_sampler(self):
        num_spans: int = 10000
        num_sampled: int = 0
        tracer: Tracer = self.tracer_provider.get_tracer("test")
        for i in range(num_spans):
            span: Span = tracer.start_span("test")
            print(span.get_span_context().trace_flags)
            if span.get_span_context().trace_flags.sampled:
                num_sampled += 1
            span.end()
        self.assertGreater(0.05, num_sampled / num_spans)
