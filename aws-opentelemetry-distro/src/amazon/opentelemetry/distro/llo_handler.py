import logging
import re

from typing import Any, Dict, List, Sequence

from amazon.opentelemetry.distro.exporter.otlp.aws.logs.otlp_aws_logs_exporter import OTLPAwsLogExporter

from opentelemetry.attributes import BoundedAttributes
from opentelemetry._events import Event
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk._events import EventLoggerProvider
from opentelemetry.sdk.trace import ReadableSpan

_logger = logging.getLogger(__name__)

class LLOHandler:
    """
    Utility class for handling Large Language Objects (LLO).
    This class identifies LLO attributes, emits them as log records, and filters
    them out from telemetry data.
    """
    def __init__(self, logs_exporter: OTLPAwsLogExporter):
        self._logs_exporter = logs_exporter
        self._logger_provider = LoggerProvider()
        self._logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(self._logs_exporter)
        )

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

            event = None
            if role == "system":
                event = self._get_gen_ai_system_message_event(
                    span_ctx,
                    prompt_timestamp,
                    event_attributes,
                    value,
                    role
                )
            elif role == "user":
                event = self._get_gen_ai_user_message_event(
                    span_ctx,
                    prompt_timestamp,
                    event_attributes,
                    value,
                    role
                )
            elif role == "assistant":
                event = self._get_gen_ai_assistant_message_event(
                    span_ctx,
                    prompt_timestamp,
                    event_attributes,
                    value,
                    role,
                )
            elif role in ["function", "unknown"]:
                # TODO: Need to define a custom event and emit
                pass

            if event:
                events.append(event)

        return events

    def _get_gen_ai_system_message_event(
        self,
        span_ctx,
        timestamp,
        event_attributes,
        content,
        role
    ):
        """
        Create and return a `gen_ai.system.message` Event.
        """
        body = {"content": content}

        # According to OTel spec, this body field is only required if available and not equal to `system`.
        # ref: https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-events/#event-gen_aisystemmessage
        if role != "system":
            body["role"] = role

        return Event(
            name="gen_ai.system.message",
            timestamp=timestamp,
            attributes=event_attributes,
            body=body,
            trace_id=span_ctx.trace_id,
            span_id=span_ctx.span_id,
            trace_flags=span_ctx.trace_flags,
        )

    def _get_gen_ai_user_message_event(
        self,
        span_ctx,
        timestamp,
        event_attributes,
        content,
        role
    ):
        """
        Create and return a `gen_ai.user.message` Event.
        """
        body = {"content": content}

        # According to OTel spec, this body field is only required if available and not equal to `user`.
        # ref: https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-events/#event-gen_aiusermessage
        if role != "user":
            body["role"] = role

        return Event(
            name="gen_ai.user.message",
            timestamp=timestamp,
            attributes=event_attributes,
            body=body,
            trace_id=span_ctx.trace_id,
            span_id=span_ctx.span_id,
            trace_flags=span_ctx.trace_flags,
        )

    def _get_gen_ai_assistant_message_event(
        self,
        span_ctx,
        timestamp,
        event_attributes,
        content,
        role,
    ):
        """
        Create and return a `gen_ai.assistant.message` Event.

        According to the OTel spec, assistant message events may contain tool_calls,
        if available. In our implementation, tool call information is not available
        directly in the span attributes we're processing - it exists in separate
        related spans.

        Thus without implementing complex span correlation, we cannot reliable extract
        tool_calls for assistant messages. This limitation is acceptable per the OTel
        spec since tool_calls are only required when available. However, this will
        lead to reduction in data quality.

        ref: https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-events/#event-gen_aiassistantmessage
        """
        body = {"content": content}

        # According to the OTel spec, this body field is only required if available and not equal to `assistant`.
        # ref: https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-events/#event-gen_aiassistantmessage
        if role != "assistant":
            body["role"] = role

        return Event(
            name="gen_ai.assistant.message",
            timestamp=timestamp,
            attributes=event_attributes,
            body=body,
            trace_id=span_ctx.trace_id,
            span_id=span_ctx.span_id,
            trace_flags=span_ctx.trace_flags,
        )
