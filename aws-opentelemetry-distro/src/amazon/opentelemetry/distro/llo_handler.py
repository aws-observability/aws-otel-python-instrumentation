# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import os
import re
import json
from typing import Dict, Any, List, Optional, Sequence, Tuple

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.trace import ReadableSpan, Event as SpanEvent
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry._logs import get_logger
from opentelemetry._events import Event
from opentelemetry.sdk._events import EventLoggerProvider
from opentelemetry.trace import TraceFlags
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

    def _extract_gen_ai_prompt_events(self, span: ReadableSpan, attributes: Dict[str, Any]) -> List[Tuple[str, Dict]]:
        """
        Extract gen_ai prompt events from attributes.

        Returns:
            A list of tuples (event_name, event_body)
        """
        events = []

        prompt_indices = set()
        prompt_content_pattern = re.compile(r"^gen_ai\.prompt\.(\d+)\.content$")

        for key in attributes:
            match = prompt_content_pattern.match(key)
            if match:
                prompt_indices.add(int(match.group(1)))

        for idx in sorted(prompt_indices):
            content_key = f"gen_ai.prompt.{idx}.content"
            role_key = f"gen_ai.prompt.{idx}.role"

            role = attributes.get(role_key, "user")
            content = attributes.get(content_key)

            if content:
                event_name = f"gen_ai.{role}.message"
                body = {"content": content}
                events.append((event_name, body))

        return events

    def _extract_gen_ai_completion_events(self, span: ReadableSpan, attributes: Dict[str, Any]) -> List[Tuple[str, Dict]]:
        """
        Extract gen_ai completion events from attributes.

        Returns:
            A list of tuples (event_name, event_body)
        """
        events = []

        completion_indices = set()
        completion_content_pattern = re.compile(r"^gen_ai\.completion\.(\d+)\.content$")

        for key in attributes:
            match = completion_content_pattern.match(key)
            if match:
                completion_indices.add(int(match.group(1)))

        for idx in sorted(completion_indices):
            content_key = f"gen_ai.completion.{idx}.content"
            role_key = f"gen_ai.completion.{idx}.role"

            role = attributes.get(role_key, "assistant")
            content = attributes.get(content_key)

            if content:
                event_name = "gen_ai.choice"
                body = {
                    "index": idx,
                    "finish_reason": attributes.get("gen_ai.finish_reason", "stop"),
                    "message": {
                        "role": role,
                        "content": content
                    }
                }

                tool_calls_key = f"gen_ai.completion.{idx}.tool_calls"
                if tool_calls_key in attributes:
                    try:
                        tool_calls = attributes[tool_calls_key]
                        if isinstance(tool_calls, str):
                            tool_calls = json.loads(tool_calls)
                        body["message"]["tool_calls"] = tool_calls
                    except:
                        # If we can't parse, ignore tool calls
                        pass

                events.append((event_name, body))

        return events

    def _extract_traceloop_events(self, span: ReadableSpan, attributes: Dict[str, Any]) -> List[Tuple[str, Dict]]:
        """
        Extract events from traceloop specific attributes.

        Returns:
            A list of tuples (event_name, event_body)
        """
        events = []

        if "traceloop.entity.input" in attributes:
            try:
                input_data = json.loads(attributes["traceloop.entity.input"])
                if "inputs" in input_data and isinstance(input_data["inputs"], dict):
                    for key, value in input_data["inputs"].items():
                        if isinstance(value, str):
                            events.append((
                                "gen_ai.user.message",
                                {"content": value}
                            ))
            except:
                # If we can't parse as JSON, treat as raw content
                events.append((
                    "gen_ai.user.message",
                    {"content": attributes["traceloop.entity.input"]}
                ))

        if "traceloop.entity.output" in attributes:
            try:
                output_data = json.loads(attributes["traceloop.entity.output"])
                if "outputs" in output_data and isinstance(output_data["outputs"], dict):
                    for key, value in output_data["outputs"].items():
                        if isinstance(value, str):
                            events.append((
                                "gen_ai.choice",
                                {
                                    "index": 0,
                                    "finish_reason": "stop",
                                    "message": {
                                        "role": "assistant",
                                        "content": value
                                    }
                                }
                            ))
            except:
                # If we can't parse as JSON, treat as raw content
                events.append((
                    "gen_ai.choice",
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": attributes["traceloop.entity.output"]
                        }
                    }
                ))

        return events



    def emit_llo_attributes(self, span: ReadableSpan, attributes: Dict[str, Any], 
                            event_name: Optional[str] = None, event_timestamp: Optional[int] = None) -> None:
        """
        Extract, transform, and emit LLO attributes as OpenTelemetry events.

        Args:
            span: The span containing the LLO attributes
            attributes: Dictionary of attributes to check for LLO attributes
            event_name: Optional name of the event (if attributes are from an event)
            event_timestamp: Optional timestamp for events (span.start_time used for span attributes)
        """
        if not self._event_logger:
            return

        try:
            timestamp = event_timestamp or span.start_time

            gen_ai_system = span.attributes.get("gen_ai.system", "unknown")
            if gen_ai_system == "Langchain":
                gen_ai_system = "langchain"

            all_events = []
            all_events.extend(self._extract_gen_ai_prompt_events(span, attributes))
            all_events.extend(self._extract_gen_ai_completion_events(span, attributes))
            all_events.extend(self._extract_traceloop_events(span, attributes))

            span_ctx = span.context
            for event_name, body in all_events:
                event_attributes = {
                    GenAIAttributes.GEN_AI_SYSTEM: gen_ai_system,
                    "event.name": event_name
                }

                otel_event = Event(
                    name=event_name,
                    attributes=event_attributes,
                    body=body,
                    trace_id=span_ctx.trace_id,
                    span_id=span_ctx.span_id,
                    trace_flags=span_ctx.trace_flags
                )
                self._event_logger.emit(otel_event)
                _logger.debug(f"Emitted GenAI event: {event_name}")
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

    def process_span_events(self, span: ReadableSpan) -> None:
        """
        Process events within a span by:
        1. Emitting LLO attributes as log records (if logger is configured)
        2. Filtering out LLO attributes from event attributes

        Args:
            span: The span containing events to process
        """
        if not span.events:
            return

        updated_events = []

        for event in span.events:
            if not event.attributes:
                updated_events.append(event)
                continue

            self.emit_llo_attributes(
                span, 
                event.attributes, 
                event_name=event.name, 
                event_timestamp=event.timestamp
            )

            updated_event_attributes = self.filter_attributes(event.attributes)

            need_to_update = len(updated_event_attributes) != len(event.attributes)

            if need_to_update:
                limit = None
                if isinstance(event.attributes, BoundedAttributes):
                    limit = event.attributes.maxlen

                updated_event = SpanEvent(
                    name=event.name,
                    attributes=updated_event_attributes,
                    timestamp=event.timestamp,
                    limit=limit
                )

                updated_events.append(updated_event)
            else:
                updated_events.append(event)

        span._events = updated_events

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
            self.process_span_events(span)
            modified_spans.append(span)

        return modified_spans
