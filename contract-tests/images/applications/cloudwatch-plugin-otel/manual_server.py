# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from plugins.opentelemetry.cloudwatch.span_metrics.instrumentor import SpanMetricsInstrumentor
from server import SpanMetricsApplication


class ManuallyInstrumentedApplication(SpanMetricsApplication):
    def configure_instrumentation(self, app):
        tracer_provider, meter_provider = self.create_providers()
        SpanMetricsInstrumentor().instrument(tracer_provider=tracer_provider, meter_provider=meter_provider)
        self.instrument_libraries(app, tracer_provider, meter_provider)


if __name__ == "__main__":
    ManuallyInstrumentedApplication().run()
