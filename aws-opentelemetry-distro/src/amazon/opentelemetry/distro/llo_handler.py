# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import re
from typing import Dict, Any, List, Optional, Sequence

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.trace import ReadableSpan, Event


class LLOHandler:
    """
    Utility class for handling Large Language Model Output (LLO) attributes.
    This class identifies LLO attributes and determines whether they should be
    processed or filtered out from telemetry data.
    """

    def __init__(self):
        # List of exact attribute keys that should be considered LLO attributes
        self._exact_match_patterns = [
            "traceloop.entity.input",
            "traceloop.entity.output",
            "message.content",
            "input.value",
            "output.value",
            "gen_ai.prompt",
            "gen_ai.completion",
            "gen_ai.content.revised_prompt",
        ]

        # List of regex patterns that should be considered LLO attributes
        self._regex_match_patterns = [
            r"^gen_ai\.prompt\.\d+\.content$",
            r"^gen_ai\.completion\.\d+\.content$",
            r"^llm.input_messages\.\d+\.message.content$",
            r"^llm.output_messages\.\d+\.message.content$",
        ]

    def is_llo_attribute(self, key: str) -> bool:
        """
        Determine if an attribute is LLO based on its key.
        Strict matching is enforced to avoid unintended behavior.

        Args:
            key: The attribute key to check

        Returns:
            True if the key represents an LLO attribute, False otherwise
        """
        return (
            any(pattern == key for pattern in self._exact_match_patterns) or
            any(re.match(pattern, key) for pattern in self._regex_match_patterns)
        )

    def filter_attributes(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter out LLO attributes from a dictionary of attributes.

        Args:
            attributes: Dictionary of attribute key-value pairs

        Returns:
            A new dictionary with LLO attributes removed
        """
        filtered_attributes = {}
        for key, value in attributes.items():
            if not self.is_llo_attribute(key):
                filtered_attributes[key] = value
        return filtered_attributes

    def update_span_attributes(self, span: ReadableSpan) -> None:
        """
        Update span attributes by filtering out LLO attributes.

        Args:
            span: The span to update
        """
        # Filter out LLO attributes
        updated_attributes = self.filter_attributes(span.attributes)

        # Update span attributes
        if isinstance(span.attributes, BoundedAttributes):
            span._attributes = BoundedAttributes(
                maxlen=span.attributes.maxlen,
                attributes=updated_attributes,
                immutable=span.attributes._immutable,
                max_value_len=span.attributes.max_value_len
            )
        else:
            span._attributes = updated_attributes

    def process_span_events(self, span: ReadableSpan) -> None:
        """
        Process events within a span by filtering out LLO attributes from event attributes.

        Args:
            span: The span containing events to process
        """
        if not span.events:
            return

        updated_events = []

        for event in span.events:
            # Check if this event has any attributes to process
            if not event.attributes:
                updated_events.append(event)  # Keep the original event
                continue

            # Filter out LLO attributes from event
            updated_event_attributes = self.filter_attributes(event.attributes)

            # Check if attributes were changed
            need_to_update = len(updated_event_attributes) != len(event.attributes)

            if need_to_update:
                # Create new Event with the updated attributes
                limit = None
                if isinstance(event.attributes, BoundedAttributes):
                    limit = event.attributes.maxlen

                updated_event = Event(
                    name=event.name,
                    attributes=updated_event_attributes,
                    timestamp=event.timestamp,
                    limit=limit
                )

                updated_events.append(updated_event)
            else:
                # Keep the original event
                updated_events.append(event)

        # Update the span's events with processed events
        span._events = updated_events

    def process_spans(self, spans: Sequence[ReadableSpan]) -> List[ReadableSpan]:
        """
        Process a list of spans by filtering out LLO attributes from both
        span attributes and event attributes.

        Args:
            spans: List of spans to process

        Returns:
            List of processed spans with LLO attributes removed
        """
        modified_spans = []

        for span in spans:
            # Update span attributes
            self.update_span_attributes(span)

            # Process span events
            self.process_span_events(span)

            # Add the modified span to the result list
            modified_spans.append(span)

        return modified_spans
