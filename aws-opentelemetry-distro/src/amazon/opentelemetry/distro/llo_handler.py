# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import os
import re
from typing import Dict, Any, List, Optional, Sequence

from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.trace import ReadableSpan, Event
from opentelemetry.sdk._logs import LoggerProvider, LogRecord
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry._logs import get_logger
from opentelemetry._logs.severity import SeverityNumber
from opentelemetry.trace import TraceFlags

from amazon.opentelemetry.distro.otlp_aws_logs_exporter import OTLPAwsLogExporter

_logger = logging.getLogger(__name__)


class LLOHandler:
    """
    Utility class for handling Large Language Model Output (LLO) attributes.
    This class identifies LLO attributes, emits them as log records,
    and filters them out from telemetry data.
    """

    def __init__(self):
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

        self._setup_logger()

    def _setup_logger(self):
        """
        Set up the logger with OTLP AWS Logs Exporter
        """
        logs_endpoint = os.getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT")
        if logs_endpoint:
            self._logs_exporter = OTLPAwsLogExporter(endpoint=logs_endpoint)
            self._logger_provider = LoggerProvider()
            self._logger_provider.add_log_record_processor(
                BatchLogRecordProcessor(self._logs_exporter)
            )
            self._logger = get_logger("llo_logger", logger_provider=self._logger_provider)
            _logger.debug(f"Initialized LLO logger with AWS OTLP Logs exporter at {logs_endpoint}")
        else:
            self._logger = None
            _logger.warning("No OTEL_EXPORTER_OTLP_LOGS_ENDPOINT specified, LLO attributes will be filtered but not emitted as logs")

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

    def emit_llo_attributes(self, span: ReadableSpan, attributes: Dict[str, Any], 
                            event_name: Optional[str] = None, event_timestamp: Optional[int] = None) -> None:
        """
        Extract and emit LLO attributes as log records.

        Args:
            span: The span containing the LLO attributes
            attributes: Dictionary of attributes to check for LLO attributes
            event_name: Optional name of the event (if attributes are from an event)
            event_timestamp: Optional timestamp for events (span.start_time used for span attributes)
        """
        if not self._logger:
            return

        try:
            timestamp = event_timestamp or span.start_time
            
            for key, value in attributes.items():
                if not self.is_llo_attribute(key):
                    continue

                body = {
                    "attribute_key": key,
                    "content": value
                }

                if event_name:
                    body["event_name"] = event_name

                body["span_name"] = span.name

                log_attributes = {
                    "event.name": f"llo.attribute.{key.split('.')[-1]}",
                }

                for context_key in ["gen_ai.system", "gen_ai.operation.name", "gen_ai.request.model"]:
                    if context_key in span.attributes:
                        log_attributes[context_key] = span.attributes[context_key]

                self._logger.emit(
                    LogRecord(
                        timestamp=timestamp,
                        observed_timestamp=timestamp,
                        trace_id=span.context.trace_id,
                        span_id=span.context.span_id,
                        trace_flags=TraceFlags(0x01),
                        severity_number=SeverityNumber.INFO,
                        severity_text=None,
                        body=body,
                        attributes=log_attributes
                    )
                )

                _logger.debug(f"Emitted LLO log record for attribute: {key}")

        except Exception as e:
            _logger.error(f"Error emitting LLO log records: {e}", exc_info=True)

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

                updated_event = Event(
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
