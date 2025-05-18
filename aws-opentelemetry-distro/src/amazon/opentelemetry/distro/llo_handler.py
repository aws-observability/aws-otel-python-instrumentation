import logging
import re
from typing import Any, Dict, List, Optional, Sequence

<<<<<<< HEAD
from typing import Any, Dict, List, Optional, Sequence

from opentelemetry.attributes import BoundedAttributes
=======
>>>>>>> 770f906 (add custom batch export)
from opentelemetry._events import Event
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk._events import EventLoggerProvider
<<<<<<< HEAD
from opentelemetry.sdk.trace import ReadableSpan, Event as SpanEvent

# Message event types
GEN_AI_SYSTEM_MESSAGE = "gen_ai.system.message"
GEN_AI_USER_MESSAGE = "gen_ai.user.message"
GEN_AI_ASSISTANT_MESSAGE = "gen_ai.assistant.message"

# Framework-specific attribute keys
TRACELOOP_ENTITY_INPUT = "traceloop.entity.input"
TRACELOOP_ENTITY_OUTPUT = "traceloop.entity.output"
OPENINFERENCE_INPUT_VALUE = "input.value"
OPENINFERENCE_OUTPUT_VALUE = "output.value"
OPENLIT_PROMPT = "gen_ai.prompt"
OPENLIT_COMPLETION = "gen_ai.completion"
OPENLIT_REVISED_PROMPT = "gen_ai.content.revised_prompt"

# Roles
ROLE_SYSTEM = "system"
ROLE_USER = "user"
ROLE_ASSISTANT = "assistant"
=======
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.trace import Event as SpanEvent
from opentelemetry.sdk.trace import ReadableSpan
>>>>>>> 770f906 (add custom batch export)

# Message event types
GEN_AI_SYSTEM_MESSAGE = "gen_ai.system.message"
GEN_AI_USER_MESSAGE = "gen_ai.user.message"
GEN_AI_ASSISTANT_MESSAGE = "gen_ai.assistant.message"

# Framework-specific attribute keys
TRACELOOP_ENTITY_INPUT = "traceloop.entity.input"
TRACELOOP_ENTITY_OUTPUT = "traceloop.entity.output"
OPENINFERENCE_INPUT_VALUE = "input.value"
OPENINFERENCE_OUTPUT_VALUE = "output.value"
OPENLIT_PROMPT = "gen_ai.prompt"
OPENLIT_COMPLETION = "gen_ai.completion"
OPENLIT_REVISED_PROMPT = "gen_ai.content.revised_prompt"

# Roles
ROLE_SYSTEM = "system"
ROLE_USER = "user"
ROLE_ASSISTANT = "assistant"

_logger = logging.getLogger(__name__)


class LLOHandler:
    """
    Utility class for handling Large Language Objects (LLO) in OpenTelemetry spans.

    LLOHandler performs three primary functions:
    1. Identifies Large Language Objects (LLO) content in spans
    2. Extracts and transforms these attributes into OpenTelemetry Gen AI Events
    3. Filters LLO from spans to maintain privacy and reduce span size

    Supported frameworks and their attribute patterns:
    - Standard Gen AI:
      - gen_ai.prompt.{n}.content: Structured prompt content
      - gen_ai.prompt.{n}.role: Role for prompt content (system, user, assistant, etc.)
      - gen_ai.completion.{n}.content: Structured completion content
      - gen_ai.completion.{n}.role: Role for completion content (usually assistant)

    - Traceloop:
      - traceloop.entity.input: Input text for LLM operations
      - traceloop.entity.output: Output text from LLM operations
      - traceloop.entity.name: Name of the entity processing the LLO

    - OpenLit:
      - gen_ai.prompt: Direct prompt text (treated as user message)
      - gen_ai.completion: Direct completion text (treated as assistant message)
      - gen_ai.content.revised_prompt: Revised prompt text (treated as system message)

    - OpenInference:
      - input.value: Direct input prompt
      - output.value: Direct output response
      - llm.input_messages.{n}.message.content: Individual structured input messages
      - llm.input_messages.{n}.message.role: Role for input messages
      - llm.output_messages.{n}.message.content: Individual structured output messages
      - llm.output_messages.{n}.message.role: Role for output messages
      - llm.model_name: Model name used for the LLM operation
    """

    def __init__(self, logger_provider: LoggerProvider):
        """
        Initialize an LLOHandler with the specified logger provider.

        This constructor sets up the event logger provider, configures the event logger,
        and initializes the patterns used to identify LLO attributes.

        Args:
            logger_provider: The OpenTelemetry LoggerProvider used for emitting events.
                           Global LoggerProvider instance injected from our AwsOpenTelemetryConfigurator
        """
        self._logger_provider = logger_provider

        self._event_logger_provider = EventLoggerProvider(logger_provider=self._logger_provider)
        self._event_logger = self._event_logger_provider.get_event_logger("gen_ai.events")

        # Patterns for attribute filtering
        self._exact_match_patterns = [
            TRACELOOP_ENTITY_INPUT,
            TRACELOOP_ENTITY_OUTPUT,
            OPENLIT_PROMPT,
            OPENLIT_COMPLETION,
            OPENLIT_REVISED_PROMPT,
            OPENINFERENCE_INPUT_VALUE,
            OPENINFERENCE_OUTPUT_VALUE,
        ]

        # Pre-compile regex patterns for better performance
        self._regex_patterns = [
            re.compile(r"^gen_ai\.prompt\.\d+\.content$"),
            re.compile(r"^gen_ai\.completion\.\d+\.content$"),
            re.compile(r"^llm\.input_messages\.\d+\.message\.content$"),
            re.compile(r"^llm\.output_messages\.\d+\.message\.content$"),
        ]

        # Additional pre-compiled patterns used in extraction methods
        self._prompt_content_pattern = re.compile(r"^gen_ai\.prompt\.(\d+)\.content$")
        self._completion_content_pattern = re.compile(r"^gen_ai\.completion\.(\d+)\.content$")
        self._openinference_input_msg_pattern = re.compile(r"^llm\.input_messages\.(\d+)\.message\.content$")
        self._openinference_output_msg_pattern = re.compile(r"^llm\.output_messages\.(\d+)\.message\.content$")

    def process_spans(self, spans: Sequence[ReadableSpan]) -> List[ReadableSpan]:
        """
        Processes a sequence of spans to extract and filter LLO attributes.

        For each span, this method:
        1. Extracts LLO attributes and emits them as Gen AI Events
        2. Filters out LLO attributes from the span to maintain privacy
        3. Processes any LLO attributes in span events
        4. Preserves non-LLO attributes in the span

        Handles LLO attributes from multiple frameworks:
        - Standard Gen AI (structured prompt/completion pattern)
        - Traceloop (entity input/output pattern)
        - OpenLit (direct prompt/completion pattern)
        - OpenInference (input/output value and structured messages pattern)

        Args:
            spans: A sequence of OpenTelemetry ReadableSpan objects to process

        Returns:
            List[ReadableSpan]: Modified spans with LLO attributes removed
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
        Process events within a span to extract and filter LLO attributes.

        For each event in the span, this method:
        1. Emits LLO attributes found in event attributes as Gen AI Events
        2. Filters out LLO attributes from event attributes
        3. Creates updated events with filtered attributes
        4. Replaces the original span events with updated events

        This ensures that LLO attributes are properly handled even when they appear
        in span events rather than directly in the span's attributes.

        Args:
            span: The ReadableSpan to process events for

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
        Extract Gen AI Events from LLO attributes and emit them via the event logger.

        This method:
        1. Collects LLO attributes from multiple frameworks using specialized extractors
        2. Converts each LLO attribute into appropriate Gen AI Events
        3. Emits all collected events through the event logger

        Supported frameworks:
        - Standard Gen AI: Structured prompt/completion with roles
        - Traceloop: Entity input/output
        - OpenLit: Direct prompt/completion/revised prompt
        - OpenInference: Direct values and structured messages

        Args:
            span: The source ReadableSpan containing the attributes
            attributes: Dictionary of attributes to process
            event_timestamp: Optional timestamp to override span timestamps

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
        Create a new attributes dictionary with LLO attributes removed.

        This method creates a new dictionary containing only non-LLO attributes,
        preserving the original values while filtering out sensitive LLO content.
        This helps maintain privacy and reduces the size of spans.

        Args:
            attributes: Original dictionary of span or event attributes

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
        Determine if an attribute key contains LLO content based on pattern matching.

        Checks attribute keys against two types of patterns:
        1. Exact match patterns (complete string equality):
           - Traceloop: "traceloop.entity.input", "traceloop.entity.output"
           - OpenLit: "gen_ai.prompt", "gen_ai.completion", "gen_ai.content.revised_prompt"
           - OpenInference: "input.value", "output.value"

        2. Regex match patterns (regular expression matching):
           - Standard Gen AI: "gen_ai.prompt.{n}.content", "gen_ai.completion.{n}.content"
           - OpenInference: "llm.input_messages.{n}.message.content",
                           "llm.output_messages.{n}.message.content"

        Args:
            key: The attribute key to check

        Returns:
            bool: True if the key matches any LLO pattern, False otherwise
        """
        # Check exact matches first (faster)
        if key in self._exact_match_patterns:
            return True

        # Then check regex patterns
        return any(pattern.match(key) for pattern in self._regex_patterns)

    def _extract_gen_ai_prompt_events(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ) -> List[Event]:
        """
        Extract Gen AI Events from structured prompt attributes.

        Processes attributes matching the pattern `gen_ai.prompt.{n}.content` and their
        associated `gen_ai.prompt.{n}.role` attributes to create appropriate events.

        Event types are determined by the role:
        1. `system` → `gen_ai.system.message` Event
        2. `user` → `gen_ai.user.message` Event
        3. `assistant` → `gen_ai.assistant.message` Event
        4. `function` → `gen_ai.{gen_ai.system}.message` custom Event
        5. `unknown` → `gen_ai.{gen_ai.system}.message` custom Event

        Args:
            span: The source ReadableSpan containing the attributes
            attributes: Dictionary of attributes to process
            event_timestamp: Optional timestamp to override span.start_time

        Returns:
            List[Event]: Events created from prompt attributes
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("gen_ai.system", "unknown")

        # Use helper method to get appropriate timestamp (prompts are inputs)
        prompt_timestamp = self._get_timestamp(span, event_timestamp, is_input=True)

        for key, value in attributes.items():
            match = self._prompt_content_pattern.match(key)
            if not match:
                continue

            index = match.group(1)
            role_key = f"gen_ai.prompt.{index}.role"
            role = attributes.get(role_key, "unknown")

            event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": key}
            body = {"content": value, "role": role}

            # Use helper method to determine event name based on role
            event_name = self._get_event_name_for_role(role, gen_ai_system)

            event = self._get_gen_ai_event(
                name=event_name,
                span_ctx=span_ctx,
                timestamp=prompt_timestamp,
                attributes=event_attributes,
                body=body,
            )

            events.append(event)

        return events

    def _extract_gen_ai_completion_events(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ) -> List[Event]:
        """
        Extract Gen AI Events from structured completion attributes.

        Processes attributes matching the pattern `gen_ai.completion.{n}.content` and their
        associated `gen_ai.completion.{n}.role` attributes to create appropriate events.

        Event types are determined by the role:
        1. `assistant` → `gen_ai.assistant.message` Event (most common)
        2. Other roles → `gen_ai.{gen_ai.system}.message` custom Event

        Args:
            span: The source ReadableSpan containing the attributes
            attributes: Dictionary of attributes to process
            event_timestamp: Optional timestamp to override span.end_time

        Returns:
            List[Event]: Events created from completion attributes
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("gen_ai.system", "unknown")

        # Use helper method to get appropriate timestamp (completions are outputs)
        completion_timestamp = self._get_timestamp(span, event_timestamp, is_input=False)

        for key, value in attributes.items():
            match = self._completion_content_pattern.match(key)
            if not match:
                continue

            index = match.group(1)
            role_key = f"gen_ai.completion.{index}.role"
            role = attributes.get(role_key, "unknown")

            event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": key}
            body = {"content": value, "role": role}

            # Use helper method to determine event name based on role
            event_name = self._get_event_name_for_role(role, gen_ai_system)

            event = self._get_gen_ai_event(
                name=event_name,
                span_ctx=span_ctx,
                timestamp=completion_timestamp,
                attributes=event_attributes,
                body=body,
            )

            events.append(event)

        return events

    def _extract_traceloop_events(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ) -> List[Event]:
        """
        Extract Gen AI Events from Traceloop attributes.

        Processes Traceloop-specific attributes:
        - `traceloop.entity.input`: Input data (uses span.start_time)
        - `traceloop.entity.output`: Output data (uses span.end_time)
        - `traceloop.entity.name`: Used as the gen_ai.system value

        Creates generic `gen_ai.{entity_name}.message` events for both input and output.

        Args:
            span: The source ReadableSpan containing the attributes
            attributes: Dictionary of attributes to process
            event_timestamp: Optional timestamp to override span timestamps

        Returns:
            List[Event]: Events created from Traceloop attributes
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("traceloop.entity.name", "unknown")

        # Use helper methods to get appropriate timestamps
        input_timestamp = self._get_timestamp(span, event_timestamp, is_input=True)
        output_timestamp = self._get_timestamp(span, event_timestamp, is_input=False)

        traceloop_attrs = [
            (TRACELOOP_ENTITY_INPUT, input_timestamp, ROLE_USER),  # Treat input as user role
            (TRACELOOP_ENTITY_OUTPUT, output_timestamp, ROLE_ASSISTANT),  # Treat output as assistant role
        ]

        for attr_key, timestamp, role in traceloop_attrs:
            if attr_key in attributes:
                event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": attr_key}
                body = {"content": attributes[attr_key], "role": role}

                # Custom event name for Traceloop (always use system-specific format)
                event_name = f"gen_ai.{gen_ai_system}.message"

                event = self._get_gen_ai_event(
                    name=event_name,
                    span_ctx=span_ctx,
                    timestamp=timestamp,
                    attributes=event_attributes,
                    body=body,
                )
                events.append(event)

        return events

    def _extract_openlit_span_event_attributes(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ) -> List[Event]:
        """
        Extract Gen AI Events from OpenLit direct attributes.

        OpenLit uses direct key-value pairs for LLO attributes:
        - `gen_ai.prompt`: Direct prompt text (treated as user message)
        - `gen_ai.completion`: Direct completion text (treated as assistant message)
        - `gen_ai.content.revised_prompt`: Revised prompt text (treated as system message)

        The event timestamps are set based on attribute type:
        - Prompt and revised prompt: span.start_time
        - Completion: span.end_time

        Args:
            span: The source ReadableSpan containing the attributes
            attributes: Dictionary of attributes to process
            event_timestamp: Optional timestamp to override span timestamps

        Returns:
            List[Event]: Events created from OpenLit attributes
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("gen_ai.system", "unknown")

        # Use helper methods to get appropriate timestamps
        prompt_timestamp = self._get_timestamp(span, event_timestamp, is_input=True)
        completion_timestamp = self._get_timestamp(span, event_timestamp, is_input=False)

        openlit_event_attrs = [
            (OPENLIT_PROMPT, prompt_timestamp, ROLE_USER),  # Assume user role for direct prompts
            (OPENLIT_COMPLETION, completion_timestamp, ROLE_ASSISTANT),  # Assume assistant role for completions
            (OPENLIT_REVISED_PROMPT, prompt_timestamp, ROLE_SYSTEM),  # Assume system role for revised prompts
        ]

        for attr_key, timestamp, role in openlit_event_attrs:
            if attr_key in attributes:
                event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": attr_key}
                body = {"content": attributes[attr_key], "role": role}

                # Use helper method to determine event name based on role
                event_name = self._get_event_name_for_role(role, gen_ai_system)

                event = self._get_gen_ai_event(
                    name=event_name,
                    span_ctx=span_ctx,
                    timestamp=timestamp,
                    attributes=event_attributes,
                    body=body,
                )

                events.append(event)

        return events

    def _extract_openinference_attributes(
        self, span: ReadableSpan, attributes: Dict[str, Any], event_timestamp: Optional[int] = None
    ) -> List[Event]:
        """
        Extract Gen AI Events from OpenInference attributes.

        OpenInference uses two patterns for LLO attributes:
        1. Direct values:
           - `input.value`: Direct input prompt (treated as user message)
           - `output.value`: Direct output response (treated as assistant message)

        2. Structured messages:
           - `llm.input_messages.{n}.message.content`: Individual input messages
           - `llm.input_messages.{n}.message.role`: Role for input message
           - `llm.output_messages.{n}.message.content`: Individual output messages
           - `llm.output_messages.{n}.message.role`: Role for output message

        The LLM model name is extracted from the `llm.model_name` attribute
        instead of `gen_ai.system` which other frameworks use.

        Event timestamps are set based on message type:
        - Input messages: span.start_time
        - Output messages: span.end_time

        Args:
            span: The source ReadableSpan containing the attributes
            attributes: Dictionary of attributes to process
            event_timestamp: Optional timestamp to override span timestamps

        Returns:
            List[Event]: Events created from OpenInference attributes
        """
        events = []
        span_ctx = span.context
        gen_ai_system = span.attributes.get("llm.model_name", "unknown")

        # Use helper methods to get appropriate timestamps
        input_timestamp = self._get_timestamp(span, event_timestamp, is_input=True)
        output_timestamp = self._get_timestamp(span, event_timestamp, is_input=False)

        # Process direct value attributes
        openinference_direct_attrs = [
            (OPENINFERENCE_INPUT_VALUE, input_timestamp, ROLE_USER),
            (OPENINFERENCE_OUTPUT_VALUE, output_timestamp, ROLE_ASSISTANT),
        ]

        for attr_key, timestamp, role in openinference_direct_attrs:
            if attr_key in attributes:
                event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": attr_key}
                body = {"content": attributes[attr_key], "role": role}

                # Use helper method to determine event name based on role
                event_name = self._get_event_name_for_role(role, gen_ai_system)

                event = self._get_gen_ai_event(
                    name=event_name, span_ctx=span_ctx, timestamp=timestamp, attributes=event_attributes, body=body
                )

                events.append(event)

        # Process input messages
        for key, value in attributes.items():
            match = self._openinference_input_msg_pattern.match(key)
            if not match:
                continue

            index = match.group(1)
            role_key = f"llm.input_messages.{index}.message.role"
            role = attributes.get(role_key, ROLE_USER)  # Default to user if role not specified

            event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": key}
            body = {"content": value, "role": role}

            # Use helper method to determine event name based on role
            event_name = self._get_event_name_for_role(role, gen_ai_system)

            event = self._get_gen_ai_event(
                name=event_name, span_ctx=span_ctx, timestamp=input_timestamp, attributes=event_attributes, body=body
            )

            events.append(event)

        # Process output messages
        for key, value in attributes.items():
            match = self._openinference_output_msg_pattern.match(key)
            if not match:
                continue

            index = match.group(1)
            role_key = f"llm.output_messages.{index}.message.role"
            role = attributes.get(role_key, ROLE_ASSISTANT)  # Default to assistant if role not specified

            event_attributes = {"gen_ai.system": gen_ai_system, "original_attribute": key}
            body = {"content": value, "role": role}

            # Use helper method to determine event name based on role
            event_name = self._get_event_name_for_role(role, gen_ai_system)

            event = self._get_gen_ai_event(
                name=event_name, span_ctx=span_ctx, timestamp=output_timestamp, attributes=event_attributes, body=body
            )

            events.append(event)

        return events

    def _get_event_name_for_role(self, role: str, gen_ai_system: str) -> str:
        """
        Map a message role to the appropriate event name.

        Args:
            role: The role of the message (system, user, assistant, etc.)
            gen_ai_system: The gen_ai system identifier

        Returns:
            str: The appropriate event name for the given role
        """
        if role == ROLE_SYSTEM:
            return GEN_AI_SYSTEM_MESSAGE
        elif role == ROLE_USER:
            return GEN_AI_USER_MESSAGE
        elif role == ROLE_ASSISTANT:
            return GEN_AI_ASSISTANT_MESSAGE
        else:
            return f"gen_ai.{gen_ai_system}.message"

    def _get_timestamp(self, span: ReadableSpan, event_timestamp: Optional[int], is_input: bool) -> int:
        """
        Determine the appropriate timestamp to use for an event.

        Args:
            span: The source span
            event_timestamp: Optional override timestamp
            is_input: Whether this is an input (True) or output (False) message

        Returns:
            int: The timestamp to use for the event
        """
        if event_timestamp is not None:
            return event_timestamp

        return span.start_time if is_input else span.end_time

    def _get_gen_ai_event(self, name, span_ctx, timestamp, attributes, body):
        """
        Create and return a Gen AI Event with the specified parameters.

        This helper method constructs a fully configured OpenTelemetry Event object
        that includes all necessary fields for proper event propagation and context.

        Args:
            name: Event type name (e.g., gen_ai.system.message, gen_ai.user.message)
            span_ctx: Span context to extract trace/span IDs from
            timestamp: Timestamp for the event (nanoseconds)
            attributes: Additional attributes to include with the event
            body: Event body containing content and role information

        Returns:
            Event: A fully configured OpenTelemetry Gen AI Event object with
                  proper trace context propagation
        """
        return Event(
            name=name,
            timestamp=timestamp,
            attributes=attributes,
            body=body,
            trace_id=span_ctx.trace_id,
            span_id=span_ctx.span_id,
            trace_flags=span_ctx.trace_flags,
<<<<<<< HEAD
        )
=======
        )
>>>>>>> 770f906 (add custom batch export)
