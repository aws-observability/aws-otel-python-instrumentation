# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Callable, Optional, Tuple

from typing_extensions import override

from amazon.opentelemetry.distro._aws_attribute_keys import AWS_CONSUMER_PARENT_SPAN_KIND, AWS_SDK_DESCENDANT
from amazon.opentelemetry.distro._aws_span_processing_util import is_aws_sdk_span, is_local_root
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation import get_current_span


class AttributePropagatingSpanProcessor(SpanProcessor):
    """AwsAttributePropagatingSpanProcessor is SpanProcessor that propagates attributes from parent to child spans

    AwsAttributePropagatingSpanProcessor handles the propagation of attributes from parent spans to child spans,
    specified in self._attribute_keys_to_propagate. AwsAttributePropagatingSpanProcessor also propagates
    configurable data from parent spans to child spans, as a new attribute specified by self._propagation_data_key.
    Propagated data can be configured via the self._propagation_data_extractor.
    Span data propagation only starts from local root server/consumer spans,
    but from there will be propagated to any descendant spans. If the span is a CONSUMER
    PROCESS with the parent also a CONSUMER, it will set attribute AWS_CONSUMER_PARENT_SPAN_KIND as CONSUMER
    to indicate that dependency metrics should not be generated for this span.
    """

    _propagation_data_extractor: Callable[[ReadableSpan], str]
    _propagation_data_key: str
    _attribute_keys_to_propagate: Tuple[str, ...]

    def __init__(
        self,
        propagation_data_extractor: Callable[[ReadableSpan], str],
        propagation_data_key: str,
        attribute_keys_to_propagate: Tuple[str, ...],
    ):
        self._propagation_data_extractor = propagation_data_extractor
        self._propagation_data_key = propagation_data_key
        self._attribute_keys_to_propagate = attribute_keys_to_propagate

    @override
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        parent_span: ReadableSpan = get_current_span(parent_context)
        if isinstance(parent_span, ReadableSpan):
            # Add the AWS_SDK_DESCENDANT attribute to the immediate child spans of AWS SDK span.
            # This attribute helps the backend differentiate between SDK spans and their immediate children.
            # It's assumed that the HTTP spans are immediate children of the AWS SDK span
            # TODO: we should have a contract test to check the immediate children are HTTP span
            if is_aws_sdk_span(parent_span):
                span.set_attribute(AWS_SDK_DESCENDANT, "true")

            if SpanKind.INTERNAL == parent_span.kind:
                for key_to_propagate in self._attribute_keys_to_propagate:
                    value_to_propagate: str = parent_span.attributes.get(key_to_propagate)
                    if value_to_propagate is not None:
                        span.set_attribute(key_to_propagate, value_to_propagate)

            # We cannot guarantee that messaging.operation is set onStart, it could be set after the fact.
            # To work around this, add the AWS_CONSUMER_PARENT_SPAN_KIND attribute if parent and child are both CONSUMER
            # then check later if a metric should be generated.
            if _is_consumer_kind(span) and _is_consumer_kind(parent_span):
                span.set_attribute(AWS_CONSUMER_PARENT_SPAN_KIND, parent_span.kind.name)

            # Propagate span attribute cloud.resource_id for extracting lambda alias for dependency metrics.
            parent_resource_id = parent_span.attributes.get(SpanAttributes.CLOUD_RESOURCE_ID)
            current_resource_id = span.attributes.get(SpanAttributes.CLOUD_RESOURCE_ID)
            if current_resource_id is None and parent_resource_id is not None:
                span.set_attribute(SpanAttributes.CLOUD_RESOURCE_ID, parent_resource_id)

        propagation_data: str = None
        if is_local_root(span):
            if not _is_server_kind(span):
                propagation_data = self._propagation_data_extractor(span)
        elif parent_span and _is_server_kind(parent_span):
            propagation_data = self._propagation_data_extractor(parent_span)
        elif parent_span:
            propagation_data = parent_span.attributes.get(self._propagation_data_key)

        if propagation_data is not None:
            span.set_attribute(self._propagation_data_key, propagation_data)

    # pylint: disable=no-self-use
    @override
    def on_end(self, span: ReadableSpan) -> None:
        return

    @override
    def shutdown(self) -> None:
        self.force_flush()

    # pylint: disable=no-self-use
    @override
    def force_flush(self, timeout_millis: int = None) -> bool:
        return True


def _is_consumer_kind(span: ReadableSpan) -> bool:
    return SpanKind.CONSUMER == span.kind


def _is_server_kind(span: ReadableSpan) -> bool:
    return SpanKind.SERVER == span.kind
