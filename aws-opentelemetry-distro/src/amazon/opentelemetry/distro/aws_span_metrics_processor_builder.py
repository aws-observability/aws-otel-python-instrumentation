# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from amazon.opentelemetry.distro._aws_metric_attribute_generator import _AwsMetricAttributeGenerator
from amazon.opentelemetry.distro.aws_span_metrics_processor import AwsSpanMetricsProcessor
from amazon.opentelemetry.distro.metric_attribute_generator import MetricAttributeGenerator
from opentelemetry.sdk.metrics import Histogram, Meter, MeterProvider
from opentelemetry.sdk.resources import Resource

# Metric instrument configuration constants
_ERROR: str = "Error"
_FAULT: str = "Fault"
_LATENCY: str = "Latency"
_LATENCY_UNITS: str = "Milliseconds"

# Defaults
_DEFAULT_GENERATOR: MetricAttributeGenerator = _AwsMetricAttributeGenerator()
_DEFAULT_SCOPE_NAME: str = "AwsSpanMetricsProcessor"


class AwsSpanMetricsProcessorBuilder:
    """A builder for AwsSpanMetricsProcessor"""

    # Required builder elements
    _meter_provider: MeterProvider
    _resource: Resource

    # Optional builder elements
    _generator: MetricAttributeGenerator = _DEFAULT_GENERATOR
    _scope_name: str = _DEFAULT_SCOPE_NAME

    def __init__(self, meter_provider: MeterProvider, resource: Resource):
        self._meter_provider = meter_provider
        self._resource = resource

    def set_generator(self, generator: MetricAttributeGenerator) -> "AwsSpanMetricsProcessorBuilder":
        """
        Sets the generator used to generate attributes used in metrics produced by span metrics processor. If unset,
        defaults to _DEFAULT_GENERATOR. Must not be None.
        """
        if generator is None:
            raise ValueError("generator must not be None")
        self._generator = generator
        return self

    def set_scope_name(self, scope_name: str) -> "AwsSpanMetricsProcessorBuilder":
        """
        Sets the scope name used in the creation of metrics by the span metrics processor. If unset, defaults to
        _DEFAULT_SCOPE_NAME. Must not be None.
        """
        if scope_name is None:
            raise ValueError("scope_name must not be None")
        self._scope_name = scope_name
        return self

    def build(self) -> AwsSpanMetricsProcessor:
        meter: Meter = self._meter_provider.get_meter(self._scope_name)
        error_histogram: Histogram = meter.create_histogram(_ERROR)
        fault_histogram: Histogram = meter.create_histogram(_FAULT)
        latency_histogram: Histogram = meter.create_histogram(_LATENCY, unit=_LATENCY_UNITS)
        # TODO: Remove the Histogram name override after the CWAgent is fixed with metric name case-insensitive.
        error_histogram.name = _ERROR
        fault_histogram.name = _FAULT
        latency_histogram.name = _LATENCY

        return AwsSpanMetricsProcessor(
            error_histogram,
            fault_histogram,
            latency_histogram,
            self._generator,
            self._resource,
            self._meter_provider.force_flush,
        )
