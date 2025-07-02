# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, TypedDict

from opentelemetry._events import Event
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk._events import EventLoggerProvider
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.trace import Event as SpanEvent
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.util import types

ROLE_SYSTEM = "system"
ROLE_USER = "user"
ROLE_ASSISTANT = "assistant"

_logger = logging.getLogger(__name__)


class PatternType(str, Enum):
    """Types of LLO attribute patterns."""

    INDEXED = "indexed"
    DIRECT = "direct"


class PatternConfig(TypedDict, total=False):
    """Configuration for an LLO pattern."""

    type: PatternType
    regex: Optional[str]
    role_key: Optional[str]
    role: Optional[str]
    default_role: Optional[str]
    source: str


LLO_PATTERNS: Dict[str, PatternConfig] = {
    "gen_ai.prompt.{index}.content": {
        "type": PatternType.INDEXED,
        "regex": r"^gen_ai\.prompt\.(\d+)\.content$",
        "role_key": "gen_ai.prompt.{index}.role",
        "default_role": "unknown",
        "source": "prompt",
    },
    "gen_ai.completion.{index}.content": {
        "type": PatternType.INDEXED,
        "regex": r"^gen_ai\.completion\.(\d+)\.content$",
        "role_key": "gen_ai.completion.{index}.role",
        "default_role": "unknown",
        "source": "completion",
    },
    "llm.input_messages.{index}.message.content": {
        "type": PatternType.INDEXED,
        "regex": r"^llm\.input_messages\.(\d+)\.message\.content$",
        "role_key": "llm.input_messages.{index}.message.role",
        "default_role": ROLE_USER,
        "source": "input",
    },
    "llm.output_messages.{index}.message.content": {
        "type": PatternType.INDEXED,
        "regex": r"^llm\.output_messages\.(\d+)\.message\.content$",
        "role_key": "llm.output_messages.{index}.message.role",
        "default_role": ROLE_ASSISTANT,
        "source": "output",
    },
    "traceloop.entity.input": {
        "type": PatternType.DIRECT,
        "role": ROLE_USER,
        "source": "input",
    },
    "traceloop.entity.output": {
        "type": PatternType.DIRECT,
        "role": ROLE_ASSISTANT,
        "source": "output",
    },
    "crewai.crew.tasks_output": {
        "type": PatternType.DIRECT,
        "role": ROLE_ASSISTANT,
        "source": "output",
    },
    "crewai.crew.result": {
        "type": PatternType.DIRECT,
        "role": ROLE_ASSISTANT,
        "source": "result",
    },
    "gen_ai.prompt": {
        "type": PatternType.DIRECT,
        "role": ROLE_USER,
        "source": "prompt",
    },
    "gen_ai.completion": {
        "type": PatternType.DIRECT,
        "role": ROLE_ASSISTANT,
        "source": "completion",
    },
    "gen_ai.content.revised_prompt": {
        "type": PatternType.DIRECT,
        "role": ROLE_SYSTEM,
        "source": "prompt",
    },
    "gen_ai.agent.actual_output": {
        "type": PatternType.DIRECT,
        "role": ROLE_ASSISTANT,
        "source": "output",
    },
    "gen_ai.agent.human_input": {
        "type": PatternType.DIRECT,
        "role": ROLE_USER,
        "source": "input",
    },
    "input.value": {
        "type": PatternType.DIRECT,
        "role": ROLE_USER,
        "source": "input",
    },
    "output.value": {
        "type": PatternType.DIRECT,
        "role": ROLE_ASSISTANT,
        "source": "output",
    },
    "system_prompt": {
        "type": PatternType.DIRECT,
        "role": ROLE_SYSTEM,
        "source": "prompt",
    },
    "tool.result": {
        "type": PatternType.DIRECT,
        "role": ROLE_ASSISTANT,
        "source": "output",
    },
    "llm.prompts": {
        "type": PatternType.DIRECT,
        "role": ROLE_USER,
        "source": "prompt",
    },
}


class LLOHandler:
    """
    Utility class for handling Large Language Objects (LLO) in OpenTelemetry spans.

    LLOHandler performs three primary functions:
    1. Identifies Large Language Objects (LLO) content in spans
    2. Extracts and transforms these attributes into OpenTelemetry Gen AI Events
    3. Filters LLO from spans to maintain privacy and reduce span size

    The handler uses a configuration-driven approach with a pattern registry that defines
    all supported LLO attribute patterns and their extraction rules. This makes it easy
    to add support for new frameworks without modifying the core logic.
    """

    def __init__(self, logger_provider: LoggerProvider):
        """
        Initialize an LLOHandler with the specified logger provider.

        This constructor sets up the event logger provider and compiles patterns
        from the pattern registry for efficient matching.

        Args:
            logger_provider: The OpenTelemetry LoggerProvider used for emitting events.
                           Global LoggerProvider instance injected from our AwsOpenTelemetryConfigurator
        """
        self._logger_provider = logger_provider
        self._event_logger_provider = EventLoggerProvider(logger_provider=self._logger_provider)

        self._build_pattern_matchers()

    def _build_pattern_matchers(self) -> None:
        """
        Build efficient pattern matching structures from the pattern registry.

        Creates:
        - Set of exact match patterns for O(1) lookups
        - List of compiled regex patterns for indexed patterns
        - Mapping of patterns to their configurations
        """
        self._exact_match_patterns = set()
        self._regex_patterns = []
        self._pattern_configs = {}

        for pattern_key, config in LLO_PATTERNS.items():
            if config["type"] == PatternType.DIRECT:
                self._exact_match_patterns.add(pattern_key)
                self._pattern_configs[pattern_key] = config
            elif config["type"] == PatternType.INDEXED:
                if regex_str := config.get("regex"):
                    compiled_regex = re.compile(regex_str)
                    self._regex_patterns.append((compiled_regex, pattern_key, config))

    def _collect_all_llo_messages(self, span: ReadableSpan, attributes: types.Attributes) -> List[Dict[str, Any]]:
        """
        Collect all LLO messages from attributes using the pattern registry.

        This is the main collection method that processes all patterns defined
        in the registry and extracts messages accordingly.

        Args:
            span: The source ReadableSpan containing the attributes
            attributes: Dictionary of attributes to process

        Returns:
            List[Dict[str, Any]]: List of message dictionaries with 'content', 'role', and 'source' keys
        """
        messages = []

        if attributes is None:
            return messages

        for attr_key, value in attributes.items():
            if attr_key in self._exact_match_patterns:
                config = self._pattern_configs[attr_key]
                messages.append(
                    {"content": value, "role": config.get("role", "unknown"), "source": config.get("source", "unknown")}
                )

        indexed_messages = self._collect_indexed_messages(attributes)
        messages.extend(indexed_messages)

        return messages

    def _collect_indexed_messages(self, attributes: types.Attributes) -> List[Dict[str, Any]]:
        """
        Collect messages from indexed patterns (e.g., gen_ai.prompt.0.content).

        Handles patterns with numeric indices and their associated role attributes.

        Args:
            attributes: Dictionary of attributes to process

        Returns:
            List[Dict[str, Any]]: List of message dictionaries
        """
        indexed_messages = {}

        if attributes is None:
            return []

        for attr_key, value in attributes.items():
            for regex, pattern_key, config in self._regex_patterns:
                match = regex.match(attr_key)
                if match:
                    index = int(match.group(1))

                    role = config.get("default_role", "unknown")
                    if role_key_template := config.get("role_key"):
                        role_key = role_key_template.replace("{index}", str(index))
                        role = attributes.get(role_key, role)

                    key = (pattern_key, index)
                    indexed_messages[key] = {"content": value, "role": role, "source": config.get("source", "unknown")}
                    break

        sorted_keys = sorted(indexed_messages.keys(), key=lambda k: (k[0], k[1]))
        return [indexed_messages[k] for k in sorted_keys]

    def _collect_llo_attributes_from_span(self, span: ReadableSpan) -> Dict[str, Any]:
        """
        Collect all LLO attributes from a span's attributes and events.

        Args:
            span: The span to collect LLO attributes from

        Returns:
            Dictionary of all LLO attributes found in the span
        """
        all_llo_attributes = {}

        # Collect from span attributes
        if span.attributes is not None:
            for key, value in span.attributes.items():
                if self._is_llo_attribute(key):
                    all_llo_attributes[key] = value

        # Collect from span events
        if span.events:
            for event in span.events:
                if event.attributes:
                    for key, value in event.attributes.items():
                        if self._is_llo_attribute(key):
                            all_llo_attributes[key] = value

        return all_llo_attributes

    # pylint: disable-next=no-self-use
    def _update_span_attributes(self, span: ReadableSpan, filtered_attributes: types.Attributes) -> None:
        """
        Update span attributes, preserving BoundedAttributes if present.

        Args:
            span: The span to update
            filtered_attributes: The filtered attributes to set
        """
        if filtered_attributes is not None and isinstance(span.attributes, BoundedAttributes):
            span._attributes = BoundedAttributes(
                maxlen=span.attributes.maxlen,
                attributes=filtered_attributes,
                immutable=span.attributes._immutable,
                max_value_len=span.attributes.max_value_len,
            )
        else:
            span._attributes = filtered_attributes

    def process_spans(self, spans: Sequence[ReadableSpan]) -> List[ReadableSpan]:
        """
        Processes a sequence of spans to extract and filter LLO attributes.

        For each span, this method:
        1. Collects all LLO attributes from span attributes and all span events
        2. Emits a single consolidated Gen AI Event with all collected LLO content
        3. Filters out LLO attributes from the span and its events to maintain privacy
        4. Preserves non-LLO attributes in the span

        Handles LLO attributes from multiple frameworks:
        - Traceloop (indexed prompt/completion patterns and entity input/output)
        - OpenLit (direct prompt/completion patterns, including from span events)
        - OpenInference (input/output values and structured messages)
        - Strands SDK (system prompts and tool results)
        - CrewAI (tasks output and results)

        Args:
            spans: A sequence of OpenTelemetry ReadableSpan objects to process

        Returns:
            List[ReadableSpan]: Modified spans with LLO attributes removed
        """
        modified_spans = []

        for span in spans:
            # Collect all LLO attributes from both span attributes and events
            all_llo_attributes = self._collect_llo_attributes_from_span(span)

            # Emit a single consolidated event if we found any LLO attributes
            if all_llo_attributes:
                self._emit_llo_attributes(span, all_llo_attributes)

            # Filter span attributes
            filtered_attributes = None
            if span.attributes is not None:
                filtered_attributes = self._filter_attributes(span.attributes)

            # Update span attributes
            self._update_span_attributes(span, filtered_attributes)

            # Filter span events
            self._filter_span_events(span)

            modified_spans.append(span)

        return modified_spans

    def _filter_span_events(self, span: ReadableSpan) -> None:
        """
        Filter LLO attributes from span events.

        This method removes LLO attributes from event attributes while preserving
        the event structure and non-LLO attributes.

        Args:
            span: The ReadableSpan to filter events for

        Returns:
            None: The span is modified in-place
        """
        if not span.events:
            return

        updated_events = []

        for event in span.events:
            if not event.attributes:
                updated_events.append(event)
                continue

            updated_event_attributes = self._filter_attributes(event.attributes)

            if updated_event_attributes is not None and len(updated_event_attributes) != len(event.attributes):
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

    # pylint: disable-next=no-self-use
    def _group_messages_by_type(self, messages: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, str]]]:
        """
        Group messages into input and output categories based on role and source.

        Args:
            messages: List of message dictionaries with 'role', 'content', and 'source' keys

        Returns:
            Dictionary with 'input' and 'output' lists of messages
        """
        input_messages = []
        output_messages = []

        for message in messages:
            role = message.get("role", "unknown")
            content = message.get("content", "")
            formatted_message = {"role": role, "content": content}

            if role in [ROLE_SYSTEM, ROLE_USER]:
                input_messages.append(formatted_message)
            elif role == ROLE_ASSISTANT:
                output_messages.append(formatted_message)
            else:
                # Route based on source for non-standard roles
                if any(key in message.get("source", "") for key in ["completion", "output", "result"]):
                    output_messages.append(formatted_message)
                else:
                    input_messages.append(formatted_message)

        return {"input": input_messages, "output": output_messages}

    def _emit_llo_attributes(
        self, span: ReadableSpan, attributes: types.Attributes, event_timestamp: Optional[int] = None
    ) -> None:
        """
        Extract LLO attributes and emit them as a single consolidated Gen AI Event.

        This method:
        1. Collects all LLO attributes using the pattern registry
        2. Groups them into input and output messages
        3. Emits one event per span containing all LLO content

        The event body format:
        {
            "input": {
                "messages": [
                    {"role": "system", "content": "..."},
                    {"role": "user", "content": "..."}
                ]
            },
            "output": {
                "messages": [
                    {"role": "assistant", "content": "..."}
                ]
            }
        }

        Args:
            span: The source ReadableSpan containing the attributes
            attributes: Dictionary of attributes to process
            event_timestamp: Optional timestamp to override span timestamps

        Returns:
            None: Event is emitted via the event logger
        """
        if attributes is None:
            return
        has_llo_attrs = any(self._is_llo_attribute(key) for key in attributes)
        if not has_llo_attrs:
            return

        all_messages = self._collect_all_llo_messages(span, attributes)
        if not all_messages:
            return

        # Group messages into input/output categories
        grouped_messages = self._group_messages_by_type(all_messages)

        # Build event body
        event_body = {}
        if grouped_messages["input"]:
            event_body["input"] = {"messages": grouped_messages["input"]}
        if grouped_messages["output"]:
            event_body["output"] = {"messages": grouped_messages["output"]}

        if not event_body:
            return

        timestamp = event_timestamp if event_timestamp is not None else span.end_time
        event_logger = self._event_logger_provider.get_event_logger(span.instrumentation_scope.name)

        event_attributes = {}
        if span.attributes and "session.id" in span.attributes:
            event_attributes["session.id"] = span.attributes["session.id"]

        event = Event(
            name=span.instrumentation_scope.name,
            timestamp=timestamp,
            body=event_body,
            attributes=event_attributes if event_attributes else None,
            trace_id=span.context.trace_id,
            span_id=span.context.span_id,
            trace_flags=span.context.trace_flags,
        )

        event_logger.emit(event)
        _logger.debug("Emitted Gen AI Event with input/output message format")

    def _filter_attributes(self, attributes: types.Attributes) -> types.Attributes:
        """
        Create a new attributes dictionary with LLO attributes removed.

        This method creates a new dictionary containing only non-LLO attributes,
        preserving the original values while filtering out sensitive LLO content.
        This helps maintain privacy and reduces the size of spans.

        Args:
            attributes: Original dictionary of span or event attributes

        Returns:
            types.Attributes: New dictionary with LLO attributes removed, or None if input is None
        """
        has_llo_attrs = False
        for key in attributes:
            if self._is_llo_attribute(key):
                has_llo_attrs = True
                break

        if not has_llo_attrs:
            return attributes

        if attributes is None:
            return None

        filtered_attributes = {}
        for key, value in attributes.items():
            if not self._is_llo_attribute(key):
                filtered_attributes[key] = value

        return filtered_attributes

    def _is_llo_attribute(self, key: str) -> bool:
        """
        Determine if an attribute key contains LLO content based on pattern matching.

        Uses the pattern registry to check if a key matches any LLO pattern.

        Args:
            key: The attribute key to check

        Returns:
            bool: True if the key matches any LLO pattern, False otherwise
        """
        if key in self._exact_match_patterns:
            return True

        for regex, _, _ in self._regex_patterns:
            if regex.match(key):
                return True

        return False
