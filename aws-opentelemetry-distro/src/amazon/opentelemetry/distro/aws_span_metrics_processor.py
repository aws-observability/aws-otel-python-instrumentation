# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Dict, Optional

from typing_extensions import override

from amazon.opentelemetry.distro.metric_attribute_generator import MetricAttributeGenerator
from opentelemetry.context import Context
from opentelemetry.metrics import Histogram
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import BoundedAttributes, ReadableSpan, Span, SpanProcessor, StatusCode
from opentelemetry.semconv.trace import SpanAttributes

_HTTP_STATUS_CODE = SpanAttributes.HTTP_STATUS_CODE
_NANOS_TO_MILLIS: float = 1_000_000.0

# Constants for deriving error and fault metrics
_ERROR_CODE_LOWER_BOUND: int = 400
_ERROR_CODE_UPPER_BOUND: int = 499
_FAULT_CODE_LOWER_BOUND: int = 500
_FAULT_CODE_UPPER_BOUND: int = 599


class AwsSpanMetricsProcessor(SpanProcessor):
    """AwsSpanMetricsProcessor is SpanProcessor that generates metrics from spans

    This processor will generate metrics based on span data. It depends on a MetricAttributeGenerator being provided on
    instantiation, which will provide a means to determine attributes which should be used to create metrics. A Resource
    must also be provided, which is used to generate metrics. Finally, three Histogram must be provided, which will be
    used to actually create desired metrics (see below)

    AwsSpanMetricsProcessor produces metrics for errors (e.g. HTTP 4XX status codes), faults (e.g. HTTP 5XX status
    codes), and latency (in Milliseconds). Errors and faults are counted, while latency is measured with a histogram.
    Metrics are emitted with attributes derived from span attributes.

    For highest fidelity metrics, this processor should be coupled with the AlwaysRecordSampler, which will result in
    100% of spans being sent to the processor.
    """

    # Metric instruments
    _error_histogram: Histogram
    _fault_histogram: Histogram
    _latency_histogram: Histogram

    _generator: MetricAttributeGenerator
    _resource: Resource

    _force_flush_function: Callable

    # no op function to act as a default function in case forceFlushFunction was
    # not supplied to the the constructor.
    # pylint: disable=no-self-use
    def _no_op_function(self, timeout_millis: float = None) -> bool:
        return True

    def __init__(
        self,
        error_histogram: Histogram,
        fault_histogram: Histogram,
        latency_histogram: Histogram,
        generator: MetricAttributeGenerator,
        resource: Resource,
        force_flush_function: Callable = _no_op_function,
    ):
        self._error_histogram = error_histogram
        self._fault_histogram = fault_histogram
        self._latency_histogram = latency_histogram
        self._generator = generator
        self._resource = resource
        self._force_flush_function = force_flush_function

    # pylint: disable=no-self-use
    @override
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        return

    @override
    def on_end(self, span: ReadableSpan) -> None:
        attribute_dict: Dict[str, BoundedAttributes] = self._generator.generate_metric_attributes_dict_from_span(
            span, self._resource
        )
        for attributes in attribute_dict.values():
            self._record_metrics(span, attributes)

    @override
    def shutdown(self) -> None:
        self.force_flush()

    # pylint: disable=no-self-use
    @override
    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return self._force_flush_function(timeout_millis)

    def _record_metrics(self, span: ReadableSpan, attributes: BoundedAttributes) -> None:
        # Only record metrics if non-empty attributes are returned.
        if len(attributes) > 0:
            self._record_error_or_fault(span, attributes)
            self._record_latency(span, attributes)

    def _record_error_or_fault(self, span: ReadableSpan, attributes: BoundedAttributes) -> None:
        # The logic to record error and fault should be kept in sync with the aws-xray exporter whenever possible except
        # for the throttle.
        # https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/awsxrayexporter/internal/translator/cause.go#L121-L160
        http_status_code: int = span.attributes.get(_HTTP_STATUS_CODE)
        status_code: StatusCode = span.status.status_code

        if _is_not_error_or_fault(http_status_code):
            if StatusCode.ERROR == status_code:
                self._error_histogram.record(0, attributes)
                self._fault_histogram.record(1, attributes)
            else:
                self._error_histogram.record(0, attributes)
                self._fault_histogram.record(0, attributes)
        elif _ERROR_CODE_LOWER_BOUND <= http_status_code <= _ERROR_CODE_UPPER_BOUND:
            self._error_histogram.record(1, attributes)
            self._fault_histogram.record(0, attributes)
        elif _FAULT_CODE_LOWER_BOUND <= http_status_code <= _FAULT_CODE_UPPER_BOUND:
            self._error_histogram.record(0, attributes)
            self._fault_histogram.record(1, attributes)

    def _record_latency(self, span: ReadableSpan, attributes: BoundedAttributes) -> None:
        nanos: int = span.end_time - span.start_time
        millis: float = nanos / _NANOS_TO_MILLIS
        self._latency_histogram.record(millis, attributes)


def _is_not_error_or_fault(http_status_code: int) -> bool:
    return (
        http_status_code is None
        or http_status_code < _ERROR_CODE_LOWER_BOUND
        or http_status_code > _FAULT_CODE_UPPER_BOUND
    )
