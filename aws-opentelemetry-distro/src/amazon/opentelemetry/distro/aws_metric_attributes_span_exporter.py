# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import List, Sequence

from amazon.opentelemetry.distro._aws_span_processing_util import (
    should_generate_dependency_metric_attributes,
    should_generate_service_metric_attributes,
)
from amazon.opentelemetry.distro._delegating_readable_span import _DelegatingReadableSpan
from amazon.opentelemetry.distro.metric_attribute_generator import MetricAttributeGenerator
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult


class AwsMetricAttributesSpanExporter(SpanExporter):
    """
    This exporter will update a span with metric attributes before exporting. It depends on a SpanExporter
    being provided on instantiation, which the AwsMetricAttributesSpanExporter will delegate
    export to. Also, a MetricAttributeGenerator must be provided, which will provide a means
    to determine attributes which should be applied to the span. Finally, a Resource must be
    provided, which is used to generate metric attributes.

    This exporter should be coupled with the AwsSpanMetricsProcessor using the same MetricAttributeGenerator.
    This will result in metrics and spans being produced with common attributes.
    """

    def __init__(self, delegate: SpanExporter, generator: MetricAttributeGenerator, resource: Resource):
        self._delegate = delegate
        self._generator = generator
        self._resource = resource

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        modified_spans: Sequence[ReadableSpan] = self._add_metric_attributes(spans)
        return self._delegate.export(modified_spans)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._delegate.force_flush(timeout_millis)

    def shutdown(self) -> None:
        return self._delegate.shutdown()

    def _add_metric_attributes(self, spans: Sequence[ReadableSpan]) -> Sequence[ReadableSpan]:
        modified_spans: List[ReadableSpan] = []

        for span in spans:
            attribute_map: [str, BoundedAttributes] = self._generator.generate_metric_attribute_map_from_span(
                span, self._resource
            )
            generates_service_metrics: bool = should_generate_service_metric_attributes(span)
            generates_dependency_metrics: bool = should_generate_dependency_metric_attributes(span)

            if generates_service_metrics and generates_dependency_metrics:
                attributes: BoundedAttributes = copy_attributes_with_local_root(
                    attribute_map.get(MetricAttributeGenerator.DEPENDENCY_METRIC)
                )
            elif generates_service_metrics:
                attributes: BoundedAttributes = attribute_map.get(MetricAttributeGenerator.SERVICE_METRIC)
            elif generates_dependency_metrics:
                attributes: BoundedAttributes = attribute_map.get(MetricAttributeGenerator.DEPENDENCY_METRIC)

            if attributes:
                span = wrap_span_with_attributes(span, attributes)
            modified_spans.append(span)

        return modified_spans


def copy_attributes_with_local_root(attributes: BoundedAttributes) -> BoundedAttributes:
    attributes["AWS_SPAN_KIND"] = "LOCAL_ROOT"
    return attributes


def wrap_span_with_attributes(span: ReadableSpan, attributes: BoundedAttributes) -> ReadableSpan:
    original_attributes: BoundedAttributes = span.attributes
    updated_attributes: BoundedAttributes = original_attributes.copy()
    updated_attributes.update(attributes)

    def span_attributes_override_function(self):
        return updated_attributes

    delegating_span = _DelegatingReadableSpan(readable_span=span)
    delegating_span.attributes = property(span_attributes_override_function)

    return delegating_span
