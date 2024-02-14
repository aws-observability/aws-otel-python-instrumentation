# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import List, Sequence, TypeVar

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

AttributesT = TypeVar("AttributesT", types.Attributes, BoundedAttributes)


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
                attributes = copy_attributes_with_local_root(attribute_map.get(DEPENDENCY_METRIC))
            elif generates_service_metrics:
                attributes = attribute_map.get(SERVICE_METRIC)
            elif generates_dependency_metrics:
                attributes = attribute_map.get(DEPENDENCY_METRIC)

            if attributes:
                span = wrap_span_with_attributes(span, attributes)
            modified_spans.append(span)

        return modified_spans


def copy_attributes_with_local_root(attributes: BoundedAttributes) -> BoundedAttributes:
    new_attributes: types.Attributes = {}
    for key, value in attributes.items():
        new_attributes[key] = value

    new_attributes[AWS_SPAN_KIND] = LOCAL_ROOT

    return BoundedAttributes(
        maxlen=attributes.maxlen,
        attributes=new_attributes,
        immutable=attributes._immutable,
        max_value_len=attributes.max_value_len,
    )


# TODO: AwsMetricAttributesSpanExporter depends on internal ReadableSpan method _attributes.
#  This is a bit risky but is required for our implementation.
#  The risk is that the implementation of _attributes changes in the future.
#  We need tests that thoroughly test this behaviour to make sure it does not change upstream.
def wrap_span_with_attributes(span: ReadableSpan, attributes: BoundedAttributes) -> ReadableSpan:
    # To make sure we create a new span without influence original span's Attributes
    # We have to create a deepcopy for it
    original_attributes: AttributesT = span.attributes
    update_attributes: types.Attributes = {}
    # Copy all attribute in span into update_attributes
    for key, value in original_attributes.items():
        update_attributes[key] = value
    # Replace existing span-attributes if there is same key in Attributes
    for key, value in attributes.items():
        update_attributes[key] = value

    if isinstance(original_attributes, BoundedAttributes):
        span._attributes = BoundedAttributes(
            maxlen=original_attributes.maxlen,
            attributes=update_attributes,
            immutable=original_attributes._immutable,
            max_value_len=original_attributes.max_value_len,
        )
    else:
        span._attributes = update_attributes
    return span
