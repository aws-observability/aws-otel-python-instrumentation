# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import time
from unittest import TestCase

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import AwsOpenTelemetryConfigurator
from amazon.opentelemetry.distro.aws_opentelemetry_distro import AwsOpenTelemetryDistro
from opentelemetry.environment_variables import OTEL_LOGS_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_TRACES_EXPORTER
from opentelemetry.sdk.environment_variables import OTEL_TRACES_SAMPLER, OTEL_TRACES_SAMPLER_ARG
from opentelemetry.sdk.trace import Span, Tracer, TracerProvider
from opentelemetry.trace import get_tracer_provider


# This class setup Tracer Provider Globally, which can only set once
# if there is another setup for tracer provider, may cause issue
class TestAwsOpenTelemetryConfigurator(TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault(OTEL_TRACES_EXPORTER, "none")
        os.environ.setdefault(OTEL_METRICS_EXPORTER, "none")
        os.environ.setdefault(OTEL_LOGS_EXPORTER, "none")
        os.environ.setdefault(OTEL_TRACES_SAMPLER, "traceidratio")
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "0.01")
        aws_open_telemetry_distro: AwsOpenTelemetryDistro = AwsOpenTelemetryDistro()
        aws_open_telemetry_distro.configure()
        aws_otel_configurator: AwsOpenTelemetryConfigurator = AwsOpenTelemetryConfigurator()
        aws_otel_configurator.configure()
        cls.tracer_provider: TracerProvider = get_tracer_provider()

    # The probability of this passing once without correct IDs is low, 20 times is inconceivable.
    def test_provide_generate_xray_ids(self):
        for _ in range(20):
            tracer: Tracer = self.tracer_provider.get_tracer("test")
            start_time_sec: int = int(time.time())
            span: Span = tracer.start_span("test")
            trace_id: int = span.get_span_context().trace_id
            trace_id_4_byte_hex: str = hex(trace_id)[2:10]
            trace_id_4_byte_int: int = int(trace_id_4_byte_hex, 16)
            self.assertGreaterEqual(trace_id_4_byte_int, start_time_sec)

    # Sanity check that the trace ID ratio sampler works fine with the x-ray generator.
    def test_trace_id_ratio_sampler(self):
        for _ in range(20):
            num_spans: int = 100000
            num_sampled: int = 0
            tracer: Tracer = self.tracer_provider.get_tracer("test")
            for _ in range(num_spans):
                span: Span = tracer.start_span("test")
                if span.get_span_context().trace_flags.sampled:
                    num_sampled += 1
                span.end()
            # Configured for 1%, confirm there are at most 5% to account for randomness and reduce test flakiness.
            self.assertGreater(0.05, num_sampled / num_spans)
