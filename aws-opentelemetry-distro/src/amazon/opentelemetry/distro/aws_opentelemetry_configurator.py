# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from opentelemetry.sdk._configuration import _BaseConfigurator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import set_tracer_provider


class AwsTracerProvider(TracerProvider):
    def __init__(self):
        super(AwsTracerProvider, self).__init__()
        self.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        # TODO:
        # 1. Remove BatchSpanProcessor(ConsoleSpanExporter())) and update testing instructions
        # 2. Add SpanMetricsProcessor to generate AppSignal metrics from spans and exports them
        # 3. Add AwsMetricAttributesSpanExporter to add more attributes to all spans.
        # 4. Add AlwaysRecordSampler to record all spans.
        # 5. Add AttributePropagatingSpanProcessor to propagate span attributes from parent to child.


class AwsOpenTelemetryConfigurator(_BaseConfigurator):
    def __init__(self):
        self.trace_provider = None

    def _configure(self, **kwargs):
        self.trace_provider = AwsTracerProvider()
        set_tracer_provider(self.trace_provider)

    def get_trace_provider(self):
        return self.trace_provider
