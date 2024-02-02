# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timedelta
from logging import Logger, getLogger
from time import sleep
from typing import Callable, TypeVar

from google.protobuf.internal.containers import RepeatedScalarFieldContainer
from grpc import Channel, insecure_channel
from mock_collector_service_pb2 import (
    ClearRequest,
    GetMetricsRequest,
    GetMetricsResponse,
    GetTracesRequest,
    GetTracesResponse,
)
from mock_collector_service_pb2_grpc import MockCollectorServiceStub

from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.metrics.v1.metrics_pb2 import Metric, ResourceMetrics, ScopeMetrics
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span

_logger: Logger = getLogger(__name__)
_TIMEOUT_DELAY: timedelta = timedelta(seconds=20)
_WAIT_INTERVAL: float = 0.1
T = TypeVar("T")


class ResourceScopeSpan:
    """Data class used to correlate resources, scope and telemetry signals.

    Correlate resource, scope and span
    """

    def __init__(self, resource_spans: ResourceSpans, scope_spans: ScopeSpans, span: Span):
        self.resource_spans: ResourceSpans = resource_spans
        self.scope_spans = scope_spans
        self.span: Span = span


class ResourceScopeMetric:
    """Data class used to correlate resources, scope and telemetry signals.

    Correlate resource, scope and metric
    """

    def __init__(self, resource_metrics: ResourceMetrics, scope_metrics: ScopeMetrics, metric: Metric):
        self.resource_metrics: ResourceMetrics = resource_metrics
        self.scope_metrics: ScopeMetrics = scope_metrics
        self.metric: Metric = metric


class MockCollectorClient:
    """The mock collector client is used to interact with the Mock collector image, used in the tests."""

    def __init__(self, mock_collector_address: str, mock_collector_port: str):
        channel: Channel = insecure_channel(f"{mock_collector_address}:{mock_collector_port}")
        self.client: MockCollectorServiceStub = MockCollectorServiceStub(channel)

    def clear_signals(self) -> None:
        """Clear all the signals in the backend collector"""
        self.client.clear(ClearRequest())

    def get_traces(self) -> list[ResourceScopeSpan]:
        """Get all traces that are currently stored in the collector

        Returns:
            List of `ResourceScopeSpan` which is essentially a flat list containing all the spans and their related
            scope and resources.
        """

        def get_export() -> list[ExportTraceServiceRequest]:
            response: GetTracesResponse = self.client.get_traces(GetTracesRequest())
            serialized_traces: RepeatedScalarFieldContainer[bytes] = response.traces
            return list(map(ExportTraceServiceRequest.FromString, serialized_traces))

        def wait_condition(exported: list[ExportTraceServiceRequest], current: list[ExportTraceServiceRequest]) -> bool:
            return 0 < len(exported) == len(current)

        exported_traces: list[ExportTraceServiceRequest] = _wait_for_content(get_export, wait_condition)
        spans: list[ResourceScopeSpan] = []
        for exported_trace in exported_traces:
            for resource_span in exported_trace.resource_spans:
                for scope_span in resource_span.scope_spans:
                    for span in scope_span.spans:
                        spans.append(ResourceScopeSpan(resource_span, scope_span, span))
        return spans

    def get_metrics(self, present_metrics: set[str]) -> list[ResourceScopeMetric]:
        """Get all metrics that are currently stored in the mock collector.

        Returns:
             List of `ResourceScopeMetric` which is a flat list containing all metrics and their related scope and
             resources.
        """

        def get_export() -> list[ExportMetricsServiceRequest]:
            response: GetMetricsResponse = self.client.get_metrics(GetMetricsRequest())
            serialized_metrics: RepeatedScalarFieldContainer[bytes] = response.metrics
            return list(map(ExportMetricsServiceRequest.FromString, serialized_metrics))

        def wait_condition(
            exported: list[ExportMetricsServiceRequest], current: list[ExportMetricsServiceRequest]
        ) -> bool:
            received_metrics: set[str] = set()
            for exported_metric in current:
                for resource_metric in exported_metric.resource_metrics:
                    for scope_metric in resource_metric.scope_metrics:
                        for metric in scope_metric.metrics:
                            received_metrics.add(metric.name)
            return 0 < len(exported) == len(current) and present_metrics.issubset(received_metrics)

        exported_metrics: list[ExportMetricsServiceRequest] = _wait_for_content(get_export, wait_condition)
        metrics: list[ResourceScopeMetric] = []
        for exported_metric in exported_metrics:
            for resource_metric in exported_metric.resource_metrics:
                for scope_metric in resource_metric.scope_metrics:
                    for metric in scope_metric.metrics:
                        metrics.append(ResourceScopeMetric(resource_metric, scope_metric, metric))
        return metrics


def _wait_for_content(get_export: Callable[[], list[T]], wait_condition: Callable[[list[T], list[T]], bool]) -> list[T]:
    # Verify that there is no more data to be received
    deadline: datetime = datetime.now() + _TIMEOUT_DELAY
    exported: list[T] = []

    while deadline > datetime.now():
        try:
            current_exported: list[T] = get_export()
            if wait_condition(exported, current_exported):
                return current_exported
            exported = current_exported

            sleep(_WAIT_INTERVAL)
        # pylint: disable=broad-exception-caught
        except Exception:
            _logger.exception("Error while reading content")

    raise RuntimeError("Timeout waiting for content")
