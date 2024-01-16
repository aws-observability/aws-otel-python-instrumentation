# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from types import MappingProxyType
from typing import List, Sequence

from typing_extensions import override

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_SPAN_KIND
from amazon.opentelemetry.distro._aws_span_processing_util import (
    LOCAL_ROOT,
    should_generate_dependency_metric_attributes,
    should_generate_service_metric_attributes,
)
from amazon.opentelemetry.distro.metric_attribute_generator import (
    DEPENDENCY_METRIC,
    SERVICE_METRIC,
    MetricAttributeGenerator,
)
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.util import types


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

    @override
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        modified_spans: Sequence[ReadableSpan] = self._add_metric_attributes(spans)
        return self._delegate.export(modified_spans)

    @override
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._delegate.force_flush(timeout_millis)

    @override
    def shutdown(self) -> None:
        return self._delegate.shutdown()

    def _add_metric_attributes(self, spans: Sequence[ReadableSpan]) -> Sequence[ReadableSpan]:
        modified_spans: List[ReadableSpan] = []

        for span in spans:
            # If the attribute_map has no items, no modifications are required. If there is one item, it means the
            # span either produces Service or Dependency metric attributes, and in either case we want to
            # modify the span with them. If there are two items, the span produces both Service and
            # Dependency metric attributes indicating the span is a local dependency root. The Service
            # Attributes must be a subset of the Dependency, with the exception of AWS_SPAN_KIND. The
            # knowledge that the span is a local root is more important that knowing that it is a
            # Dependency metric, so we take all the Dependency metrics but replace AWS_SPAN_KIND with
            # LOCAL_ROOT.
            attribute_map: [str, BoundedAttributes] = self._generator.generate_metric_attributes_dict_from_span(
                span, self._resource
            )
            generates_service_metrics: bool = should_generate_service_metric_attributes(span)
            generates_dependency_metrics: bool = should_generate_dependency_metric_attributes(span)

            attributes: BoundedAttributes = None
            if generates_service_metrics and generates_dependency_metrics:
                attributes: BoundedAttributes = copy_attributes_with_local_root(attribute_map.get(DEPENDENCY_METRIC))
            elif generates_service_metrics:
                attributes: BoundedAttributes = attribute_map.get(SERVICE_METRIC)
            elif generates_dependency_metrics:
                attributes: BoundedAttributes = attribute_map.get(DEPENDENCY_METRIC)

            if attributes:
                span = wrap_span_with_attributes(span, attributes)
            modified_spans.append(span)

        return modified_spans


def copy_attributes_with_local_root(attributes: BoundedAttributes) -> BoundedAttributes:
    new_attributes: types.Attributes = {}
    for key, value in attributes:
        new_attributes[key] = value

    new_attributes[AWS_SPAN_KIND] = LOCAL_ROOT

    return BoundedAttributes(
        maxlen=attributes.maxlen,
        attributes=new_attributes,
        immutable=attributes._immutable,
        max_value_len=attributes.max_value_len,
    )


# ReadableSpan does not permit modification. However, we need to add derived metric attributes to the span.
# To work around this, we will wrap the ReadableSpan with a _DelegatingReadableSpan
# that simply passes through all API calls, except for those pertaining to Attributes,
# i.e. ReadableSpan.attributes, similar as DelegatingSpanData class in Java.
# See https://github.com/open-telemetry/opentelemetry-specification/issues/1089 for more context on this approach.
def wrap_span_with_attributes(span: ReadableSpan, attributes: BoundedAttributes) -> ReadableSpan:
    original_attributes: BoundedAttributes = span.attributes
    update_attributes: types.Attributes = {}
    # Copy all attribute in span into update_attributes
    for key, value in original_attributes:
        update_attributes[key] = value
    # Append all attribute in attributes that is not in original_attributes into update_attributes
    for key, value in attributes:
        if key not in update_attributes:
            update_attributes[key] = value

    span._attributes = MappingProxyType(update_attributes)
    return span
