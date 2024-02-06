# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import time
from typing import Optional
from unittest import TestCase
from unittest.mock import MagicMock

from opentelemetry.sdk.environment_variables import OTEL_TRACES_SAMPLER, OTEL_TRACES_SAMPLER_ARG
from opentelemetry.util._once import Once

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import AwsOpenTelemetryConfigurator
from opentelemetry.environment_variables import OTEL_LOGS_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_TRACES_EXPORTER
from opentelemetry.sdk.trace import Span, Tracer, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import get_tracer_provider, set_tracer_provider, ProxyTracerProvider

_TRACER_PROVIDER_SET_ONCE = Once()
_TRACER_PROVIDER: Optional[TracerProvider] = None
_PROXY_TRACER_PROVIDER = ProxyTracerProvider()

class Testbasic(TestCase):

    def testTraceProvider(self):
        os.environ.setdefault(OTEL_TRACES_EXPORTER, "none")
        os.environ.setdefault(OTEL_METRICS_EXPORTER, "none")
        os.environ.setdefault(OTEL_LOGS_EXPORTER, "none")
        os.environ.setdefault(OTEL_TRACES_SAMPLER, "traceidratio")
        os.environ.setdefault(OTEL_TRACES_SAMPLER_ARG, "0.01")
        self.span_exporter: InMemorySpanExporter = InMemorySpanExporter()
        self.simple_span_processor: SimpleSpanProcessor = SimpleSpanProcessor(self.span_exporter)
        self.tracer_provider: TracerProvider = MagicMock()
        self.tracer_provider.sampler = "123"
        self.tracer_provider.id = "114514"
        set_tracer_provider(self.tracer_provider)
        print("1234567")
        print(get_tracer_provider().sampler)
        self.assertEqual(1234, 5678)
