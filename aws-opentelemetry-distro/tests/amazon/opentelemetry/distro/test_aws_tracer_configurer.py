# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import time
from unittest import TestCase

from opentelemetry.sdk.environment_variables import OTEL_TRACES_SAMPLER, OTEL_TRACES_SAMPLER_ARG

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import AwsOpenTelemetryConfigurator
from amazon.opentelemetry.distro.aws_opentelemetry_distro import AwsOpenTelemetryDistro
from opentelemetry.environment_variables import OTEL_LOGS_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_TRACES_EXPORTER
from opentelemetry.sdk.trace import Span, Tracer, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import get_tracer_provider


class TestAwsTracerConfigurer(TestCase):
    def setUp(self):
        os.environ.setdefault(OTEL_TRACES_EXPORTER, "none")
        os.environ.setdefault(OTEL_METRICS_EXPORTER, "none")
        os.environ.setdefault(OTEL_LOGS_EXPORTER, "none")
        os.environ.setdefault(OTEL_TRACES_SAMPLER, "traceidratio")
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "0.01")
        self.span_exporter: InMemorySpanExporter = InMemorySpanExporter()
        self.simple_span_processor: SimpleSpanProcessor = SimpleSpanProcessor(self.span_exporter)
        self.aws_otel_configurator: AwsOpenTelemetryConfigurator = AwsOpenTelemetryConfigurator()
        self.aws_otel_configurator.configure()
        self.tracer_provider: TracerProvider = get_tracer_provider()
        self.aws_open_telemetry_distro: AwsOpenTelemetryDistro = AwsOpenTelemetryDistro()
        self.aws_open_telemetry_distro.configure()

    def test_provide_generate_xray_ids(self):
        for _ in range(20):
            tracer: Tracer = self.tracer_provider.get_tracer("test")
            start_time_sec: int = int(time.time())
            span: Span = tracer.start_span("test")
            trace_id: int = span.get_span_context().trace_id
            self.assertGreater(trace_id, start_time_sec)

    def test_trace_id_ratio_sampler(self):
        for _ in range(20):
            num_spans: int = 10000
            num_sampled: int = 0
            tracer: Tracer = self.tracer_provider.get_tracer("test")
            for i in range(num_spans):
                span: Span = tracer.start_span("test")
                if span.get_span_context().trace_flags.sampled:
                    num_sampled += 1
                span.end()
            self.assertGreater(0.05, num_sampled / num_spans)
