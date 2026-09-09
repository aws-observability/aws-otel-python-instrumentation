# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import re
from collections.abc import MutableMapping
from typing import Collection, Optional

from typing_extensions import override

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.util import types

ENV_ADOT_REDACT_SPAN_ATTRIBUTES = "ADOT_REDACT_SPAN_ATTRIBUTES"
REDACTED_VALUE = "REDACTED"


class AttributeRedactingSpanProcessor(SpanProcessor):
    """
    Redacts configured attributes on completed spans, their events, and their links.

    Attribute names can be supplied to the constructor or through the
    ``ADOT_REDACT_SPAN_ATTRIBUTES`` environment variable as a comma-separated
    list. Each entry can be an exact attribute name or contain ``*`` wildcards.
    Matching attribute values are replaced with ``REDACTED`` in place while
    attribute names and non-matching values remain unchanged.

    Examples:
        Redact several exact attributes, every attribute beginning with
        ``http.request.``, and matching GenAI content attributes:

        ``ADOT_REDACT_SPAN_ATTRIBUTES=user.email,request.body,db.statement,http.request.*,gen_ai.*.content``

        Redact every span, span event, and span link attribute:

        ``ADOT_REDACT_SPAN_ATTRIBUTES=*``
    """

    def __init__(self, attributes_to_redact: Optional[Collection[str]] = None) -> None:
        self.attributes_to_redact = (
            list(attributes_to_redact)
            if attributes_to_redact
            else [
                attribute.strip()
                for attribute in os.environ.get(ENV_ADOT_REDACT_SPAN_ATTRIBUTES, "").split(",")
                if attribute.strip()
            ]
        )
        self._compiled_patterns = tuple(
            re.compile(re.escape(attribute).replace(r"\*", ".*")) for attribute in self.attributes_to_redact
        )

    # pylint: disable=no-self-use
    @override
    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        return

    @override
    def on_end(self, span: ReadableSpan) -> None:
        if not self.attributes_to_redact:
            return

        self._redact_attributes(span._attributes)  # noqa: SLF001

        for event in span.events:
            self._redact_attributes(event._attributes)  # noqa: SLF001

        for link in span.links:
            self._redact_attributes(link.attributes)

    def _redact_attributes(self, attributes: types.Attributes) -> None:
        if not attributes:
            return

        if isinstance(attributes, BoundedAttributes):
            # Completed spans, events, and links expose immutable BoundedAttributes, so
            # their public setter raises TypeError. Update existing values under its lock.
            with attributes._lock:  # noqa: SLF001
                for key in attributes._dict:  # noqa: SLF001
                    if self._should_redact(key):
                        attributes._dict[key] = REDACTED_VALUE  # noqa: SLF001
        elif isinstance(attributes, MutableMapping):
            for key in attributes:
                if self._should_redact(key):
                    attributes[key] = REDACTED_VALUE

    def _should_redact(self, attribute_name: str) -> bool:
        return any(pattern.fullmatch(attribute_name) for pattern in self._compiled_patterns)

    # pylint: disable=no-self-use
    @override
    def shutdown(self) -> None:
        return

    # pylint: disable=no-self-use
    @override
    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True
