# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import re
from typing import Dict, Any, List, Sequence

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry._events import Event
from opentelemetry.sdk._events import EventLoggerProvider
from opentelemetry.semconv._incubating.attributes import gen_ai_attributes as GenAIAttributes

from amazon.opentelemetry.distro.otlp_aws_logs_exporter import OTLPAwsLogExporter

_logger = logging.getLogger(__name__)


class LLOHandler:
    """
    Utility class for handling Large Language Model Output (LLO) attributes.
    This class identifies LLO attributes, emits them as log records,
    and filters them out from telemetry data.
    """

    def __init__(self, logs_exporter: OTLPAwsLogExporter):
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

        self._regex_match_patterns = [
            r"^gen_ai\.prompt\.\d+\.content$",
            r"^gen_ai\.completion\.\d+\.content$",
            r"^llm.input_messages\.\d+\.message.content$",
            r"^llm.output_messages\.\d+\.message.content$",
        ]

        self._logs_exporter = logs_exporter
        self._logger_provider = LoggerProvider()
        self._logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(self._logs_exporter)
        )


        self._event_logger_provider = EventLoggerProvider(logger_provider=self._logger_provider)
        self._event_logger = self._event_logger_provider.get_event_logger("gen_ai.events")

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


    def _extract_gen_ai_prompt_events(self, span: ReadableSpan, attributes: Dict[str, Any]) -> List[Event]:
        """
        Extract gen_ai prompt events from attributes.

        Returns:
            A list of Event objects
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
            role = attributes.get(role_key, "user")

            event = Event(
                name=f"gen_ai.{role}.message",
                attributes={
                    GenAIAttributes.GEN_AI_SYSTEM: gen_ai_system,
                    "event.name": f"gen_ai.{role}.message",
                    "original_attribute.name": role_key
                },
                body={
                    "content": value
                },
                timestamp=prompt_timestamp,
                trace_id=span_ctx.trace_id,
                span_id=span_ctx.span_id,
                trace_flags=span_ctx.trace_flags
            )
            events.append(event)

        return events


    def _extract_gen_ai_completion_events(self, span: ReadableSpan, attributes: Dict[str, Any]) -> List[Event]:
        """
        Extract gen_ai completion events from attributes.

        Returns:
            A list of Event objects
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("gen_ai.system", "unknown")

        completion_timestamp = span.end_time

        completion_content_pattern = re.compile(r"^gen_ai\.completion\.(\d+)\.content$")

        for key, value in attributes.items():
            match = completion_content_pattern.match(key)
            if not match:
                continue

            index = match.group(1)
            role_key = f"gen_ai.completion.{index}.role"
            role = attributes.get(role_key, "assistant")

            event = Event(
                name="gen_ai.choice",
                attributes={
                    GenAIAttributes.GEN_AI_SYSTEM: gen_ai_system,
                    "event.name": "gen_ai.choice",
                    "original_attribute.name": role_key
                },
                body={
                    "index": int(index),
                    "finish_reason": attributes.get("gen_ai.finish_reason", "stop"),
                    "message": {
                        "role": role,
                        "content": value
                    }
                },
                timestamp=completion_timestamp,
                trace_id=span_ctx.trace_id,
                span_id=span_ctx.span_id,
                trace_flags=span_ctx.trace_flags
            )
            events.append(event)

        return events


    def _extract_traceloop_events(self, span: ReadableSpan, attributes: Dict[str, Any]) -> List[Event]:
        """
        Extract events from traceloop specific attributes.

        Returns:
            A list of Event objects
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("gen_ai.system", "unknown")

        start_timestamp = span.start_time
        end_timestamp = span.end_time

        if "traceloop.entity.input" in attributes:
            input_content = attributes["traceloop.entity.input"]

            event = Event(
                name="gen_ai.framework.event", # Use generic framework event name for now
                attributes={
                    GenAIAttributes.GEN_AI_SYSTEM: gen_ai_system,
                    "framework.name": "traceloop",
                    "framework.event.type": "input",
                    "original_attribute.name": "traceloop.entity.input"
                },
                body={
                    "framework.traceloop.entity.input": input_content
                },
                timestamp=start_timestamp,
                trace_id=span_ctx.trace_id,
                span_id=span_ctx.span_id,
                trace_flags=span_ctx.trace_flags
            )
            events.append(event)

        if "traceloop.entity.output" in attributes:
            output_content = attributes["traceloop.entity.output"]

            event = Event(
                name="gen_ai.framework.event",
                attributes={
                    GenAIAttributes.GEN_AI_SYSTEM: gen_ai_system,
                    "framework.name": "traceloop",
                    "framework.event.type": "output",
                    "original_attribute.name": "traceloop.entity.output"
                },
                body={
                    "framework.traceloop.entity.output": output_content
                },
                timestamp=end_timestamp,
                trace_id=span_ctx.trace_id,
                span_id=span_ctx.span_id,
                trace_flags=span_ctx.trace_flags
            )
            events.append(event)

        return events


    def emit_llo_attributes(self, span: ReadableSpan, attributes: Dict[str, Any]) -> None:
        """
        Extract, transform, and emit LLO attributes as OpenTelemetry events.

        Args:
            span: The span containing the LLO attributes
            attributes: Dictionary of attributes to check for LLO attributes
        """
        if not self._event_logger:
            return

        try:
            all_events = []
            all_events.extend(self._extract_gen_ai_prompt_events(span, attributes))
            all_events.extend(self._extract_gen_ai_completion_events(span, attributes))
            all_events.extend(self._extract_traceloop_events(span, attributes))

            for event in all_events:
                self._event_logger.emit(event)
                _logger.debug(f"Emitted GenAI event: {event.name}")
        except Exception as e:
            _logger.error(f"Error emitting GenAI events: {e}", exc_info=True)


    def update_span_attributes(self, span: ReadableSpan) -> None:
        """
        Update span attributes by:
        1. Emitting LLO attributes as log records (if logger is configured)
        2. Filtering out LLO attributes from the span

        Args:
            span: The span to update
        """
        self.emit_llo_attributes(span, span.attributes)
        updated_attributes = self.filter_attributes(span.attributes)

        if isinstance(span.attributes, BoundedAttributes):
            span._attributes = BoundedAttributes(
                maxlen=span.attributes.maxlen,
                attributes=updated_attributes,
                immutable=span.attributes._immutable,
                max_value_len=span.attributes.max_value_len
            )
        else:
            span._attributes = updated_attributes


    def process_spans(self, spans: Sequence[ReadableSpan]) -> List[ReadableSpan]:
        """
        Process a list of spans by:
        1. Emitting LLO attributes as log records (if logger is configured)
        2. Filtering out LLO attributes from both span attributes and event attributes

        Args:
            spans: List of spans to process

        Returns:
            List of processed spans with LLO attributes removed
        """
        modified_spans = []

        for span in spans:
            self.update_span_attributes(span)
            modified_spans.append(span)

        return modified_spans
