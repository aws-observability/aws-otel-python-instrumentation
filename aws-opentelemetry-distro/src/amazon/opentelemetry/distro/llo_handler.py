import logging
import re

from typing import Any, Dict, List, Sequence

from opentelemetry.attributes import BoundedAttributes
from opentelemetry._events import Event
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._events import EventLoggerProvider
from opentelemetry.sdk.trace import ReadableSpan

GEN_AI_SYSTEM_MESSAGE = "gen_ai.system.message"
GEN_AI_USER_MESSAGE = "gen_ai.user.message"
GEN_AI_ASSISTANT_MESSAGE = "gen_ai.assistant.message"

_logger = logging.getLogger(__name__)

class LLOHandler:
    """
    Utility class for handling Large Language Objects (LLO) in OpenTelemetry spans.

    LLOHandler performs three primary functions:
    1. Identifies Large Language Objects (LLO) content in spans
    2. Extracts and transforms these attributes into OpenTelemetry Gen AI Events
    3. Filters LLO from spans
    """
    def __init__(self, logger_provider: LoggerProvider):
        """
        Initialize an LLOHandler with the specified logger provider.

        Args:
            logger_provider: The OpenTelemetry LoggerProvider used for emitting events.
                            Global LoggerProvider instance injected from our AwsOpenTelemetryConfigurator
        """
        self._logger_provider = logger_provider

        self._event_logger_provider = EventLoggerProvider(logger_provider=self._logger_provider)
        self._event_logger = self._event_logger_provider.get_event_logger("gen_ai.events")

        self._exact_match_patterns = []
        self._regex_match_patterns = [
            r"^gen_ai\.prompt\.\d+\.content$"
        ]


    def process_spans(self, spans: Sequence[ReadableSpan]) -> List[ReadableSpan]:
        """
        Performs LLO processing for each span:
        1. Emitting LLO attributes as Gen AI Events
        2. Filtering out LLO attributes from the span

        Args:
            spans: A sequence of OpenTelemetry ReadableSpan objects to process

        Returns:
            List of processed spans with LLO attributes removed
        """
        modified_spans = []

        for span in spans:
            self._emit_llo_attributes(span, span.attributes)
            updated_attributes = self._filter_attributes(span.attributes)

            if isinstance(span.attributes, BoundedAttributes):
                span._attributes = BoundedAttributes(
                    maxlen=span.attributes.maxlen,
                    attributes=updated_attributes,
                    immutable=span.attributes._immutable,
                    max_value_len=span.attributes.max_value_len
                )
            else:
                span._attributes = updated_attributes

            modified_spans.append(span)

        return modified_spans


    def _emit_llo_attributes(self, span: ReadableSpan, attributes: Dict[str, Any]) -> None:
        """
        Collects the Gen AI Events for each LLO attribute in the span and emits them
        using the event logger.

        Args:
            span: The source ReadableSpan that potentially contains LLO attributes
            attributes: Dictionary of span attributes to process

        Returns:
            None: Events are emitted via the event logger
        """
        all_events = []
        all_events.extend(self._extract_gen_ai_prompt_events(span, attributes))

        for event in all_events:
            self._event_logger.emit(event)
            _logger.debug(f"Emitted Gen AI Event: {event.name}")


    def _filter_attributes(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter out attributes that contain LLO from the span's attributes. This
        method creates a new attributes dictionary that excludes any keys identified
        as containing LLO data (based on the configured patterns).

        Args:
            attributes: Original dictionary of span attributes

        Returns:
            Dict[str, Any]: New dictionary with LLO attributes removed
        """
        filtered_attributes = {}

        for key, value in attributes.items():
            if not self._is_llo_attribute(key):
                filtered_attributes[key] = value

        return filtered_attributes


    def _is_llo_attribute(self, key: str) -> bool:
        """
        Determine if a span attribute contains an LLO based on its key name.

        Checks if theattribute key matches any of the configured patterns:
        1. Exact math patterns (complete string equality)
        2. Regex match patterns (regular expression matching)

        Args:
            key: The attribute key to check

        Returns:
            bool: True if the key matches an LLO pattern, False otherwise
        """
        return (
            any(pattern == key for pattern in self._exact_match_patterns) or
            any(re.match(pattern, key) for pattern in self._regex_match_patterns)
        )


    def _extract_gen_ai_prompt_events(self, span: ReadableSpan, attributes: Dict[str, Any]) -> List[Event]:
        """
        Extract gen_ai prompt events from attributes. Each item `gen_ai.prompt.{n}.content`
        maps has an associated `gen_ai.prompt.{n}.role` that determines the Event
        type to be created.

        `gen_ai.prompt.{n}.role`:
        1. `system` -> `gen_ai.system.message` Event
        2. `user` -> `gen_ai.user.message` Event
        3. `assistant` -> `gen_ai.assistant.message` Event
        4. `function` -> `gen_ai.{gen_ai.system}.message` custom Event
        5. `unknown` -> `gen_ai.{gen_ai.system}.message` custom Event

        Args:
            span: The source ReadableSpan that potentially contains LLO attributes
            attributes: Dictionary of span attributes to process

        Returns:
            List[Event]: List of OpenTelemetry Events created from prompt attributes
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("gen_ai.system", "unknown")

        prompt_timestamp = span.start_time
        prompt_content_pattern = re.compile(r"^gen_ai\.prompt\.(\d+)\.content$")

        for key, value in attributes.items():
            match = prompt_content_pattern.match(key)
            if not match:
                continue

            index = match.group(1)
            role_key = f"gen_ai.prompt.{index}.role"
            role = attributes.get(role_key, "unknown")

            event_attributes = {
                "gen_ai.system": gen_ai_system,
                "original_attribute": key
            }

            body = {
                "content": value,
                "role": role
            }

            event = None
            if role == "system":
                event = self._get_gen_ai_event(
                    name=GEN_AI_SYSTEM_MESSAGE,
                    span_ctx=span_ctx,
                    timestamp=prompt_timestamp,
                    attributes=event_attributes,
                    body=body
                )
            elif role == "user":
                event = self._get_gen_ai_event(
                    name=GEN_AI_USER_MESSAGE,
                    span_ctx=span_ctx,
                    timestamp=prompt_timestamp,
                    attributes=event_attributes,
                    body=body
                )
            elif role == "assistant":
                event = self._get_gen_ai_event(
                    name=GEN_AI_ASSISTANT_MESSAGE,
                    span_ctx=span_ctx,
                    timestamp=prompt_timestamp,
                    attributes=event_attributes,
                    body=body
                )
            elif role in ["function", "unknown"]:
                event = self._get_gen_ai_event(
                    name=f"gen_ai.{gen_ai_system}.message",
                    span_ctx=span_ctx,
                    timestamp=prompt_timestamp,
                    attributes=event_attributes,
                    body=body
                )

            if event:
                events.append(event)

        return events

    def _get_gen_ai_event(
        self,
        name,
        span_ctx,
        timestamp,
        attributes,
        body
    ):
        """
        Create and return a Gen AI Event with the provided parameters.

        Args:
            name: The name/type of the event (e.g., gen_ai.system.message)
            span_ctx: The span context to extract trace/span IDs from
            timestamp: The timestamp for the event
            attributes: Additional attributes to include with the event
            body: The event body containing content and role information

        Returns:
            Event: A fully configured OpenTelemetry Gen AI Event object
        """
        return Event(
            name=name,
            timestamp=timestamp,
            attributes=attributes,
            body=body,
            trace_id=span_ctx.trace_id,
            span_id=span_ctx.span_id,
            trace_flags=span_ctx.trace_flags
        )
