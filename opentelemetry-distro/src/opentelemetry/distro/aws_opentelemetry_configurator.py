# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from opentelemetry.sdk._configuration import _BaseConfigurator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import set_tracer_provider


class AwsTracerProvider(TracerProvider):
    def __init__(
            self
    ):
        pass
        # TODO:
        # 1. Add SpanMetricsProcessor to generate AppSignal metrics from spans and exports them
        # 2. Add AttributePropagatingSpanProcessor to propagate span attributes from parent to child
        # 3. Add AwsMetricAttributesSpanExporter to add more attributes to all spans.
        # 4. Add AlwaysRecordSampler to record all spans.


class AwsConfigurator(_BaseConfigurator):
    # pylint: disable=no-self-use
    def _configure(self, **kwargs):
        provider = AwsTracerProvider()
        set_tracer_provider(provider)
