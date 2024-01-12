# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from amazon.opentelemetry.distro.attribute_propagating_span_processor_builder import (
    AttributePropagatingSpanProcessorBuilder,
)
from amazon.opentelemetry.distro.aws_always_record_sampler import AwsAlwaysRecordSampler
from opentelemetry.sdk._configuration import _BaseConfigurator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.sampling import Sampler
from opentelemetry.trace import set_tracer_provider


class AwsTracerProvider(TracerProvider):
    def __init__(self):
        always_record_sample: Sampler = AwsAlwaysRecordSampler()
        super(AwsTracerProvider, self).__init__(sampler=always_record_sample)
        self.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        self.add_span_processor(AttributePropagatingSpanProcessorBuilder().build())
        # TODO:
        # 1. Add SpanMetricsProcessor to generate AppSignal metrics from spans and exports them
        # 2. Add AwsMetricAttributesSpanExporter to add more attributes to all spans.


class AwsOpenTelemetryConfigurator(_BaseConfigurator):
    def __init__(self):
        self.trace_provider = None

    def _configure(self, **kwargs):
        self.trace_provider = AwsTracerProvider()
        set_tracer_provider(self.trace_provider)

    def get_trace_provider(self):
        return self.trace_provider
