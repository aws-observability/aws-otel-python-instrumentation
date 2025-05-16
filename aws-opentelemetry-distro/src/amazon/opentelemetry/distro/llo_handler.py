import logging
import re

from typing import Any, Dict, List, Optional, Sequence

from opentelemetry.attributes import BoundedAttributes
from opentelemetry._events import Event
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._events import EventLoggerProvider
from opentelemetry.sdk.trace import ReadableSpan, Event as SpanEvent

GEN_AI_SYSTEM_MESSAGE = "gen_ai.system.message"
GEN_AI_USER_MESSAGE = "gen_ai.user.message"
GEN_AI_ASSISTANT_MESSAGE = "gen_ai.assistant.message"
TRACELOOP_ENTITY_INPUT = "traceloop.entity.input"
TRACELOOP_ENTITY_OUTPUT = "traceloop.entity.output"
OPENINFERENCE_INPUT_VALUE = "input.value"
OPENINFERENCE_OUTPUT_VALUE = "output.value"

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

        self._exact_match_patterns = [
            "traceloop.entity.input",
            "traceloop.entity.output",
            "gen_ai.prompt",
            "gen_ai.completion",
            "gen_ai.content.revised_prompt",
            "input.value",
            "output.value",
        ]
        self._regex_match_patterns = [
            r"^gen_ai\.prompt\.\d+\.content$",
            r"^gen_ai\.completion\.\d+\.content$",
            r"^llm\.input_messages\.\d+\.message\.content$",
            r"^llm\.output_messages\.\d+\.message\.content$",
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
                    max_value_len=span.attributes.max_value_len,
                )
            else:
                span._attributes = updated_attributes

            self.process_span_events(span)

            modified_spans.append(span)

        return modified_spans

    def process_span_events(self, span: ReadableSpan) -> None:
        """
        Process events within a span by:
        1. Emitting LLO attributes as Gen AI Events
        2. Filtering out LLO attributes from event attributes
        """
        if not span.events:
            return

        updated_events = []

        for event in span.events:
            if not event.attributes:
                updated_events.append(event)
                continue

            self._emit_llo_attributes(span, event.attributes, event_timestamp=event.timestamp)

            updated_event_attributes = self._filter_attributes(event.attributes)

            if len(updated_event_attributes) != len(event.attributes):
                limit = None
                if isinstance(event.attributes, BoundedAttributes):
                    limit = event.attributes.maxlen

                updated_event = SpanEvent(
                    name=event.name, attributes=updated_event_attributes, timestamp=event.timestamp, limit=limit
                )

                updated_events.append(updated_event)
            else:
                updated_events.append(event)

        span._events = updated_events

    def _emit_llo_attributes(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ) -> None:
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
        all_events.extend(self._extract_gen_ai_prompt_events(span, attributes, event_timestamp))
        all_events.extend(self._extract_gen_ai_completion_events(span, attributes, event_timestamp))
        all_events.extend(self._extract_traceloop_events(span, attributes, event_timestamp))
        all_events.extend(self._extract_openlit_span_event_attributes(span, attributes, event_timestamp))
        all_events.extend(self._extract_openinference_attributes(span, attributes, event_timestamp))

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
        return any(pattern == key for pattern in self._exact_match_patterns) or any(
            re.match(pattern, key) for pattern in self._regex_match_patterns
        )

    def _extract_gen_ai_prompt_events(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ) -> List[Event]:
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

        prompt_timestamp = event_timestamp if event_timestamp is not None else span.start_time
        prompt_content_pattern = re.compile(r"^gen_ai\.prompt\.(\d+)\.content$")

        for key, value in attributes.items():
            match = prompt_content_pattern.match(key)
            if not match:
                continue

            index = match.group(1)
            role_key = f"gen_ai.prompt.{index}.role"
            role = attributes.get(role_key, "unknown")

            event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": key}

            body = {"content": value, "role": role}

            event = None
            if role == "system":
                event = self._get_gen_ai_event(
                    name=GEN_AI_SYSTEM_MESSAGE,
                    span_ctx=span_ctx,
                    timestamp=prompt_timestamp,
                    attributes=event_attributes,
                    body=body,
                )
            elif role == "user":
                event = self._get_gen_ai_event(
                    name=GEN_AI_USER_MESSAGE,
                    span_ctx=span_ctx,
                    timestamp=prompt_timestamp,
                    attributes=event_attributes,
                    body=body,
                )
            elif role == "assistant":
                event = self._get_gen_ai_event(
                    name=GEN_AI_ASSISTANT_MESSAGE,
                    span_ctx=span_ctx,
                    timestamp=prompt_timestamp,
                    attributes=event_attributes,
                    body=body,
                )
            elif role in ["function", "unknown"]:
                event = self._get_gen_ai_event(
                    name=f"gen_ai.{gen_ai_system}.message",
                    span_ctx=span_ctx,
                    timestamp=prompt_timestamp,
                    attributes=event_attributes,
                    body=body,
                )

            if event:
                events.append(event)

        return events

    def _extract_gen_ai_completion_events(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ) -> List[Event]:
        """
        Extract gen_ai completion events from attributes.

        Args:
            span: The source ReadableSpan that potentially contains LLO attributes
            attributes: Dictionary of span attributes to process
            event_timestamp: Optional timestamp to use instead of span.end_time

        Returns:
            List[Event]: List of OpenTelemetry Events created from completion attributes
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("gen_ai.system", "unknown")

        completion_timestamp = event_timestamp if event_timestamp is not None else span.end_time

        completion_content_pattern = re.compile(r"^gen_ai\.completion\.(\d+)\.content$")

        for key, value in attributes.items():
            match = completion_content_pattern.match(key)
            if not match:
                continue

            index = match.group(1)
            role_key = f"gen_ai.completion.{index}.role"
            role = attributes.get(role_key, "unknown")

            event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": key}

            body = {"content": value, "role": role}

            event = None
            if role == "assistant":
                event = self._get_gen_ai_event(
                    name=GEN_AI_ASSISTANT_MESSAGE,
                    span_ctx=span_ctx,
                    timestamp=completion_timestamp,
                    attributes=event_attributes,
                    body=body,
                )
            else:
                event = self._get_gen_ai_event(
                    name=f"gen_ai.{gen_ai_system}.message",
                    span_ctx=span_ctx,
                    timestamp=completion_timestamp,
                    attributes=event_attributes,
                    body=body,
                )

            if event:
                events.append(event)

        return events

    def _extract_traceloop_events(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ) -> List[Event]:
        """
        Extract traceloop events from attributes.

        Args:
            span: The source ReadableSpan that potentially contains LLO attributes
            attributes: Dictionary of span attributes to process
            event_timestamp: Optional timestamp to use instead of span timestamps

        Returns:
            List[Event]: List of OpenTelemetry Events created from traceloop attributes
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("traceloop.entity.name", "unknown")

        input_timestamp = event_timestamp if event_timestamp is not None else span.start_time
        output_timestamp = event_timestamp if event_timestamp is not None else span.end_time

        traceloop_attrs = [(TRACELOOP_ENTITY_INPUT, input_timestamp), (TRACELOOP_ENTITY_OUTPUT, output_timestamp)]

        for attr_key, timestamp in traceloop_attrs:
            if attr_key in attributes:
                event = self._get_gen_ai_event(
                    name=f"gen_ai.{gen_ai_system}.message",
                    span_ctx=span_ctx,
                    timestamp=timestamp,
                    attributes={"gen_ai.system": gen_ai_system, "original_attribute": attr_key},
                    body={"content": attributes[attr_key]},
                )
                events.append(event)

        return events

    def _extract_openlit_span_event_attributes(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ) -> List[Event]:
        """
        Extract LLO attributes specifically from OpenLit span events, which use direct key-value pairs
        like `gen_ai.prompt` or `gen_ai.completion` in event attributes.
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("gen_ai.system", "unknown")

        prompt_timestamp = event_timestamp if event_timestamp is not None else span.start_time
        completion_timestamp = event_timestamp if event_timestamp is not None else span.end_time

        openlit_event_attrs = [
            ("gen_ai.prompt", prompt_timestamp, "user"),  # Assume user role for direct prompts
            ("gen_ai.completion", completion_timestamp, "assistant"),  # Assume assistant role for completions
            ("gen_ai.content.revised_prompt", prompt_timestamp, "system"),  # Assume system role for revised prompts
        ]

        for attr_key, timestamp, role in openlit_event_attrs:
            if attr_key in attributes:
                event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": attr_key}
                body = {"content": attributes[attr_key], "role": role}

                if role == "user":
                    event_name = GEN_AI_USER_MESSAGE
                elif role == "assistant":
                    event_name = GEN_AI_ASSISTANT_MESSAGE
                elif role == "system":
                    event_name = GEN_AI_SYSTEM_MESSAGE
                else:
                    event_name = f"gen_ai.{gen_ai_system}.message"

                event = self._get_gen_ai_event(
                    name=event_name, span_ctx=span_ctx, timestamp=timestamp, attributes=event_attributes, body=body
                )

                events.append(event)

        return events

    def _extract_openinference_attributes(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ):
        """
        Extract Gen AI Events from LLO attributes in OpenInference spans
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("llm.model_name", "unknown")

        input_timestamp = event_timestamp if event_timestamp is not None else span.start_time
        output_timestamp = event_timestamp if event_timestamp is not None else span.end_time

        openinference_direct_attrs = [
            (OPENINFERENCE_INPUT_VALUE, input_timestamp, "user"),
            (OPENINFERENCE_OUTPUT_VALUE, output_timestamp, "assistant"),
        ]

        for attr_key, timestamp, role in openinference_direct_attrs:
            if attr_key in attributes:
                event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": attr_key}
                body = {"content": attributes[attr_key], "role": role}

                if role == "user":
                    event_name = GEN_AI_USER_MESSAGE
                elif role == "assistant":
                    event_name = GEN_AI_ASSISTANT_MESSAGE
                else:
                    event_name = f"gen_ai.{gen_ai_system}.message"

                event = self._get_gen_ai_event(
                    name=event_name, span_ctx=span_ctx, timestamp=timestamp, attributes=event_attributes, body=body
                )

                events.append(event)

        input_msg_pattern = re.compile(r"^llm\.input_messages\.(\d+)\.message\.content$")

        for key, value in attributes.items():
            match = input_msg_pattern.match(key)
            if not match:
                continue

            index = match.group(1)
            role_key = f"llm.input_messages.{index}.message.role"
            role = attributes.get(role_key, "user")  # Default to user if role not specified

            event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": key}
            body = {"content": value, "role": role}

            event_name = GEN_AI_USER_MESSAGE
            if role == "system":
                event_name = GEN_AI_SYSTEM_MESSAGE
            elif role == "assistant":
                event_name = GEN_AI_ASSISTANT_MESSAGE
            elif role not in ["user", "system", "assistant"]:
                event_name = f"gen_ai.{gen_ai_system}.message"

            event = self._get_gen_ai_event(
                name=event_name, span_ctx=span_ctx, timestamp=input_timestamp, attributes=event_attributes, body=body
            )

            events.append(event)

        output_msg_pattern = re.compile(r"^llm\.output_messages\.(\d+)\.message\.content$")

        for key, value in attributes.items():
            match = output_msg_pattern.match(key)
            if not match:
                continue

            index = match.group(1)
            role_key = f"llm.output_messages.{index}.message.role"
            role = attributes.get(role_key, "assistant")  # Default to assistant if role not specified

            event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": key}
            body = {"content": value, "role": role}

            event_name = GEN_AI_ASSISTANT_MESSAGE
            if role == "system":
                event_name = GEN_AI_SYSTEM_MESSAGE
            elif role == "user":
                event_name = GEN_AI_USER_MESSAGE
            elif role not in ["user", "system", "assistant"]:
                event_name = f"gen_ai.{gen_ai_system}.message"

            event = self._get_gen_ai_event(
                name=event_name, span_ctx=span_ctx, timestamp=output_timestamp, attributes=event_attributes, body=body
            )

            events.append(event)

        return events

    def _get_gen_ai_event(self, name, span_ctx, timestamp, attributes, body):
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
            trace_flags=span_ctx.trace_flags,
        )
