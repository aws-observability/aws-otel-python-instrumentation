# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, Sequence

from opentelemetry import trace as trace_api
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan
from opentelemetry.sdk.util.instrumentation import InstrumentationInfo, InstrumentationScope
from opentelemetry.util import types


class _DelegatingReadableSpan(ReadableSpan):
    """

    Inherit this class to modify the ReadableSpan that will be exported,
    for example using the property() function to dynamically create an object with a custom function override.

    """

    def __init__(self, readable_span: ReadableSpan, attributes: types.Attributes = None) -> None:
        self._delegate = readable_span
        self._attributes = attributes

    @property
    def dropped_attributes(self) -> int:
        return self._delegate.dropped_attributes

    @property
    def dropped_events(self) -> int:
        return self._delegate.dropped_events

    @property
    def dropped_links(self) -> int:
        return self._delegate.dropped_links

    @property
    def name(self) -> str:
        return self._delegate.name

    def get_span_context(self):
        return self._delegate.get_span_context

    @property
    def context(self):
        return self._delegate.context

    @property
    def kind(self) -> trace_api.SpanKind:
        return self._delegate.kind

    @property
    def parent(self) -> Optional[trace_api.SpanContext]:
        return self._delegate.parent

    @property
    def start_time(self) -> Optional[int]:
        return self._delegate.start_time

    @property
    def end_time(self) -> Optional[int]:
        return self._delegate.end_time

    @property
    def status(self) -> trace_api.Status:
        return self._delegate.status

    @property
    def attributes(self) -> types.Attributes:
        return self._delegate.attributes

    @property
    def events(self) -> Sequence[Event]:
        return self._delegate.events

    @property
    def links(self) -> Sequence[trace_api.Link]:
        return self._delegate.links

    @property
    def resource(self) -> Resource:
        return self._delegate.resource

    def instrumentation_info(self) -> Optional[InstrumentationInfo]:
        return self._delegate.instrumentation_info

    @property
    def instrumentation_scope(self) -> Optional[InstrumentationScope]:
        return self._delegate.instrumentation_scope

    def to_json(self, indent: int = 4):
        return self._delegate.to_json(indent)
