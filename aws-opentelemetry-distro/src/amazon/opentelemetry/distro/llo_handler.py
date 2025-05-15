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
    Utility class for handling Large Language Objects (LLO).
    This class identifies LLO attributes, emits them as log records, and filters
    them out from telemetry data.
    """
    def __init__(self, logger_provider: LoggerProvider):
        self._logger_provider = logger_provider

        self._event_logger_provider = EventLoggerProvider(logger_provider=self._logger_provider)
        self._event_logger = self._event_logger_provider.get_event_logger("gen_ai.events")

        self._exact_match_patterns = []
        self._regex_match_patterns = [
            r"^gen_ai\.prompt\.\d+\.content$"
        ]


    def process_spans(self, spans: Sequence[ReadableSpan]) -> List[ReadableSpan]:
        """
        Perform LLO processing for each span:
        1. Emitting LLO attributes as Gen AI Events
        2. Filtering out LLO attributes from the span
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
        Extract, transform, and emit LLO attributes as Gen AI Events
        """
        all_events = []
        all_events.extend(self._extract_gen_ai_prompt_events(span, attributes))

        for event in all_events:
            self._event_logger.emit(event)
            _logger.debug(f"Emitted Gen AI Event: {event.name}")


    def _filter_attributes(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter out attributes that contain LLO from the span's attributes.
        """
        filtered_attributes = {}

        for key, value in attributes.items():
            if not self._is_llo_attribute(key):
                filtered_attributes[key] = value

        return filtered_attributes


    def _is_llo_attribute(self, key: str) -> bool:
        """
        Determine if a span attribute contains an LLO based on its key.
        """
        return (
            any(pattern == key for pattern in self._exact_match_patterns) or
            any(re.match(pattern, key) for pattern in self._regex_match_patterns)
        )


    def _extract_gen_ai_prompt_events(self, span: ReadableSpan, attributes: Dict[str, Any]) -> List[Event]:
        """
        Extract gen_ai prompt events from attributes. Each item `gen_ai.prompt.{n}.content`
        maps has an associated `gen_ai.prompt.{n}.role` that we map to an Event type.

        `gen_ai.prompt.{n}.role`:
        1. `system` -> `gen_ai.system.message` Event
        2. `user` -> `gen_ai.user.message` Event
        3. `assistant` -> `gen_ai.assistant.message` Event
        4. `function` -> custom Event - TBD
        5. `unknown` -> custom Event - TBD
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
        return Event(
            name=name,
            timestamp=timestamp,
            attributes=attributes,
            body=body,
            trace_id=span_ctx.trace_id,
            span_id=span_ctx.span_id,
            trace_flags=span_ctx.trace_flags
        )
