# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=no-self-use

import time
import unittest
import uuid
from unittest.mock import Mock, patch

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import Generation, LLMResult

from amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2 import (
    LangChainInstrumentor,
    _BaseCallbackManagerInitWrapper,
    _instruments,
)
from amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler import (
    OpenTelemetryCallbackHandler,
    SpanHolder,
    _sanitize_metadata_value,
    _set_request_params,
    _set_span_attribute,
)
from amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.span_attributes import (
    GenAIOperationValues,
    SpanAttributes,
)
from opentelemetry.trace import SpanKind, StatusCode


class TestOpenTelemetryHelperFunctions(unittest.TestCase):
    """Test the helper functions in the callback handler module."""

    def test_set_span_attribute(self):
        mock_span = Mock()

        _set_span_attribute(mock_span, "test.attribute", "test_value")
        mock_span.set_attribute.assert_called_once_with("test.attribute", "test_value")

        mock_span.reset_mock()

        _set_span_attribute(mock_span, "test.attribute", None)
        mock_span.set_attribute.assert_not_called()

        _set_span_attribute(mock_span, "test.attribute", "")
        mock_span.set_attribute.assert_not_called()

    def test_sanitize_metadata_value(self):
        self.assertEqual(_sanitize_metadata_value(None), None)
        self.assertEqual(_sanitize_metadata_value(True), True)
        self.assertEqual(_sanitize_metadata_value("string"), "string")
        self.assertEqual(_sanitize_metadata_value(123), 123)
        self.assertEqual(_sanitize_metadata_value(1.23), 1.23)

        self.assertEqual(_sanitize_metadata_value([1, "two", 3.0]), ["1", "two", "3.0"])
        self.assertEqual(_sanitize_metadata_value((1, "two", 3.0)), ["1", "two", "3.0"])

        class TestClass:
            def __str__(self):
                return "test_class"

        self.assertEqual(_sanitize_metadata_value(TestClass()), "test_class")

    @patch(
        "amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler._set_span_attribute"
    )
    def test_set_request_params(self, mock_set_span_attribute):
        mock_span = Mock()
        mock_span_holder = Mock(spec=SpanHolder)

        kwargs = {"model_id": "gpt-4", "temperature": 0.7, "max_tokens": 100, "top_p": 0.9}
        _set_request_params(mock_span, kwargs, mock_span_holder)

        self.assertEqual(mock_span_holder.request_model, "gpt-4")
        mock_set_span_attribute.assert_any_call(mock_span, SpanAttributes.GEN_AI_REQUEST_MODEL, "gpt-4")
        mock_set_span_attribute.assert_any_call(mock_span, SpanAttributes.GEN_AI_RESPONSE_MODEL, "gpt-4")
        mock_set_span_attribute.assert_any_call(mock_span, SpanAttributes.GEN_AI_REQUEST_TEMPERATURE, 0.7)
        mock_set_span_attribute.assert_any_call(mock_span, SpanAttributes.GEN_AI_REQUEST_MAX_TOKENS, 100)
        mock_set_span_attribute.assert_any_call(mock_span, SpanAttributes.GEN_AI_REQUEST_TOP_P, 0.9)

        mock_set_span_attribute.reset_mock()
        mock_span_holder.reset_mock()

        kwargs = {"invocation_params": {"model_id": "gpt-3.5-turbo", "temperature": 0.5, "max_tokens": 50}}
        _set_request_params(mock_span, kwargs, mock_span_holder)

        self.assertEqual(mock_span_holder.request_model, "gpt-3.5-turbo")
        mock_set_span_attribute.assert_any_call(mock_span, SpanAttributes.GEN_AI_REQUEST_MODEL, "gpt-3.5-turbo")


class TestOpenTelemetryCallbackHandler(unittest.TestCase):
    """Test the OpenTelemetryCallbackHandler class."""

    def setUp(self):
        self.mock_tracer = Mock()
        self.mock_span = Mock()
        self.mock_tracer.start_span.return_value = self.mock_span
        self.handler = OpenTelemetryCallbackHandler(self.mock_tracer)
        self.run_id = uuid.uuid4()
        self.parent_run_id = uuid.uuid4()

    def test_init(self):
        """Test the initialization of the handler."""
        handler = OpenTelemetryCallbackHandler(self.mock_tracer)
        self.assertEqual(handler.tracer, self.mock_tracer)
        self.assertEqual(handler.span_mapping, {})

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_create_span(self, mock_context_api):
        """Test the _create_span method."""
        mock_context_api.get_value.return_value = {}
        mock_context_api.set_value.return_value = {}
        mock_context_api.attach.return_value = None

        span = self.handler._create_span(
            run_id=self.run_id,
            parent_run_id=None,
            span_name="test_span",
            kind=SpanKind.INTERNAL,
            metadata={"key": "value"},
        )

        self.mock_tracer.start_span.assert_called_once_with("test_span", kind=SpanKind.INTERNAL)
        self.assertEqual(span, self.mock_span)
        self.assertIn(self.run_id, self.handler.span_mapping)

        self.mock_tracer.reset_mock()

        parent_span = Mock()
        self.handler.span_mapping[self.parent_run_id] = SpanHolder(parent_span, [], time.time(), "model-id")

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_llm_start_and_end(self, mock_context_api):
        mock_context_api.get_value.return_value = False
        serialized = {"name": "test_llm"}
        prompts = ["Hello, world!"]
        kwargs = {"invocation_params": {"model_id": "gpt-4", "temperature": 0.7, "max_tokens": 100}}

        class MockSpanHolder:
            def __init__(self, span, name, start_timestamp):
                self.span = span
                self.name = name
                self.start_timestamp = start_timestamp
                self.request_model = None

        def mock_create_span(run_id, parent_run_id, name, kind, metadata):
            span_holder = MockSpanHolder(span=self.mock_span, name=name, start_timestamp=time.time_ns())
            self.handler.span_mapping[run_id] = span_holder
            return self.mock_span

        original_create_span = self.handler._create_span
        self.handler._create_span = Mock(side_effect=mock_create_span)

        self.handler.on_llm_start(
            serialized=serialized,
            prompts=prompts,
            run_id=self.run_id,
            parent_run_id=self.parent_run_id,
            metadata={},
            **kwargs,
        )

        self.handler._create_span.assert_called_once_with(
            self.run_id,
            self.parent_run_id,
            f"{GenAIOperationValues.CHAT} gpt-4",
            kind=SpanKind.CLIENT,
            metadata={},
        )

        self.handler.span_mapping[self.run_id] = SpanHolder(self.mock_span, [], time.time(), "gpt-4")

        llm_output = {
            "token_usage": {"prompt_tokens": 10, "completion_tokens": 20},
            "model_name": "gpt-4",
            "id": "response-123",
        }
        generations = [[Generation(text="This is a test response")]]
        response = LLMResult(generations=generations, llm_output=llm_output)

        with patch(
            # pylint: disable=no-self-use
            "amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler._set_span_attribute" # noqa: E501
        ) as mock_set_attribute:
            with patch.object(self.handler, "_end_span"):
                self.handler.on_llm_end(response=response, run_id=self.run_id, parent_run_id=self.parent_run_id)

                print("\nAll calls to mock_set_attribute:")
                for i, call in enumerate(mock_set_attribute.call_args_list):
                    args, kwargs = call
                    print(f"Call {i+1}:", args, kwargs)

                mock_set_attribute.assert_any_call(self.mock_span, SpanAttributes.GEN_AI_RESPONSE_MODEL, "gpt-4")
                mock_set_attribute.assert_any_call(self.mock_span, SpanAttributes.GEN_AI_RESPONSE_ID, "response-123")
                mock_set_attribute.assert_any_call(self.mock_span, SpanAttributes.GEN_AI_USAGE_INPUT_TOKENS, 10)
                mock_set_attribute.assert_any_call(self.mock_span, SpanAttributes.GEN_AI_USAGE_OUTPUT_TOKENS, 20)

        self.handler._create_span = original_create_span

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_llm_error(self, mock_context_api):
        """Test the on_llm_error method."""
        mock_context_api.get_value.return_value = False
        self.handler.span_mapping[self.run_id] = SpanHolder(self.mock_span, [], time.time(), "gpt-4")
        error = ValueError("Test error")

        self.handler._handle_error(error=error, run_id=self.run_id, parent_run_id=self.parent_run_id)

        self.mock_span.set_status.assert_called_once()
        args, _ = self.mock_span.set_status.call_args
        self.assertEqual(args[0].status_code, StatusCode.ERROR)

        self.mock_span.record_exception.assert_called_once_with(error)
        self.mock_span.end.assert_called_once()

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_chain_start_end(self, mock_context_api):
        """Test the on_chain_start and on_chain_end methods."""
        mock_context_api.get_value.return_value = False
        serialized = {"name": "test_chain"}
        inputs = {"query": "What is the capital of France?"}

        with patch.object(self.handler, "_create_span", return_value=self.mock_span) as mock_create_span:
            self.handler.on_chain_start(
                serialized=serialized,
                inputs=inputs,
                run_id=self.run_id,
                parent_run_id=self.parent_run_id,
                metadata={},
            )

            mock_create_span.assert_called_once()
            self.mock_span.set_attribute.assert_called_once_with("gen_ai.prompt", str(inputs))

        outputs = {"result": "Paris"}
        self.handler.span_mapping[self.run_id] = SpanHolder(self.mock_span, [], time.time(), "gpt-4")

        with patch.object(self.handler, "_end_span") as mock_end_span:
            self.handler.on_chain_end(outputs=outputs, run_id=self.run_id, parent_run_id=self.parent_run_id)

            self.mock_span.set_attribute.assert_called_with("gen_ai.completion", str(outputs))
            mock_end_span.assert_called_once_with(self.mock_span, self.run_id)

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_tool_start_end(self, mock_context_api):
        """Test the on_tool_start and on_tool_end methods."""
        mock_context_api.get_value.return_value = False
        serialized = {"name": "test_tool", "id": "tool-123", "description": "A test tool"}
        input_str = "What is 2 + 2?"

        with patch.object(self.handler, "_create_span", return_value=self.mock_span) as mock_create_span:
            with patch.object(self.handler, "_get_name_from_callback", return_value="test_tool") as mock_get_name:
                self.handler.on_tool_start(
                    serialized=serialized, input_str=input_str, run_id=self.run_id, parent_run_id=self.parent_run_id
                )

                mock_create_span.assert_called_once()
                mock_get_name.assert_called_once()

                self.mock_span.set_attribute.assert_any_call("gen_ai.tool.input", input_str)
                self.mock_span.set_attribute.assert_any_call("gen_ai.tool.call.id", "tool-123")
                self.mock_span.set_attribute.assert_any_call("gen_ai.tool.description", "A test tool")
                self.mock_span.set_attribute.assert_any_call("gen_ai.tool.name", "test_tool")
                self.mock_span.set_attribute.assert_any_call("gen_ai.operation.name", "execute_tool")

        output = "The answer is 4"

        self.handler.span_mapping[self.run_id] = SpanHolder(self.mock_span, [], time.time(), "gpt-4")

        with patch.object(self.handler, "_end_span") as mock_end_span:
            self.handler.on_tool_end(output=output, run_id=self.run_id)

            mock_end_span.assert_called_once()

            self.mock_span.set_attribute.assert_any_call("gen_ai.tool.output", output)

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_agent_action_and_finish(self, mock_context_api):
        """Test the on_agent_action and on_agent_finish methods."""
        mock_context_api.get_value.return_value = False

        # Create a mock AgentAction
        mock_action = Mock()
        mock_action.tool = "calculator"
        mock_action.tool_input = "2 + 2"

        # Create a mock AgentFinish
        mock_finish = Mock()
        mock_finish.return_values = {"output": "The answer is 4"}

        # Set up the handler with a mocked span
        self.handler.span_mapping[self.run_id] = SpanHolder(self.mock_span, [], time.time(), "gpt-4")

        # Test on_agent_action
        self.handler.on_agent_action(action=mock_action, run_id=self.run_id, parent_run_id=self.parent_run_id)

        # Verify the expected attributes were set
        self.mock_span.set_attribute.assert_any_call("gen_ai.agent.tool.input", "2 + 2")
        self.mock_span.set_attribute.assert_any_call("gen_ai.agent.tool.name", "calculator")
        self.mock_span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_OPERATION_NAME, "invoke_agent")

        # Test on_agent_finish
        self.handler.on_agent_finish(finish=mock_finish, run_id=self.run_id, parent_run_id=self.parent_run_id)

        # Verify the output attribute was set
        self.mock_span.set_attribute.assert_any_call("gen_ai.agent.tool.output", "The answer is 4")

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_agent_error(self, mock_context_api):
        """Test the on_agent_error method."""
        mock_context_api.get_value.return_value = False

        # Create a test error
        test_error = ValueError("Something went wrong")

        # Patch the _handle_error method
        with patch.object(self.handler, "_handle_error") as mock_handle_error:
            # Call on_agent_error
            self.handler.on_agent_error(error=test_error, run_id=self.run_id, parent_run_id=self.parent_run_id)

            # Verify _handle_error was called with the right parameters
            mock_handle_error.assert_called_once_with(test_error, self.run_id, self.parent_run_id)


class TestLangChainInstrumentor(unittest.TestCase):
    """Test the LangChainInstrumentor class."""

    def setUp(self):
        self.instrumentor = LangChainInstrumentor()

    def test_instrumentation_dependencies(self):
        """Test that instrumentation_dependencies returns the correct dependencies."""
        result = self.instrumentor.instrumentation_dependencies()
        self.assertEqual(result, _instruments)
        self.assertEqual(result, ("langchain >= 0.1.0",))

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.get_tracer")
    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.wrap_function_wrapper")
    def test_instrument(self, mock_wrap, mock_get_tracer):
        """Test the _instrument method."""
        mock_tracer = Mock()
        mock_get_tracer.return_value = mock_tracer
        tracer_provider = Mock()

        self.instrumentor._instrument(tracer_provider=tracer_provider)

        mock_get_tracer.assert_called_once()
        mock_wrap.assert_called_once()

        module = mock_wrap.call_args[1]["module"]
        name = mock_wrap.call_args[1]["name"]
        wrapper = mock_wrap.call_args[1]["wrapper"]

        self.assertEqual(module, "langchain_core.callbacks")
        self.assertEqual(name, "BaseCallbackManager.__init__")
        self.assertIsInstance(wrapper, _BaseCallbackManagerInitWrapper)
        self.assertIsInstance(wrapper.callback_handler, OpenTelemetryCallbackHandler)

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.unwrap")
    def test_uninstrument(self, mock_unwrap):
        """Test the _uninstrument method."""
        self.instrumentor._wrapped = [("module1", "function1"), ("module2", "function2")]
        self.instrumentor.handler = Mock()

        self.instrumentor._uninstrument()

        mock_unwrap.assert_any_call("langchain_core.callbacks", "BaseCallbackManager.__init__")
        mock_unwrap.assert_any_call("module1", "function1")
        mock_unwrap.assert_any_call("module2", "function2")
        self.assertIsNone(self.instrumentor.handler)


class TestBaseCallbackManagerInitWrapper(unittest.TestCase):
    """Test the _BaseCallbackManagerInitWrapper class."""

    def test_init_wrapper_add_handler(self):
        """Test that the wrapper adds the handler to the callback manager."""
        mock_handler = Mock(spec=OpenTelemetryCallbackHandler)

        wrapper_instance = _BaseCallbackManagerInitWrapper(mock_handler)

        original_func = Mock()
        instance = Mock()
        instance.inheritable_handlers = []

        wrapper_instance(original_func, instance, [], {})

        original_func.assert_called_once_with()
        instance.add_handler.assert_called_once_with(mock_handler, True)

    def test_init_wrapper_handler_already_exists(self):
        """Test that the wrapper doesn't add a duplicate handler."""
        mock_handler = Mock(spec=OpenTelemetryCallbackHandler)

        wrapper_instance = _BaseCallbackManagerInitWrapper(mock_handler)

        original_func = Mock()
        instance = Mock()

        mock_tracer = Mock()
        existing_handler = OpenTelemetryCallbackHandler(mock_tracer)
        instance.inheritable_handlers = [existing_handler]

        wrapper_instance(original_func, instance, [], {})

        original_func.assert_called_once_with()
        instance.add_handler.assert_not_called()


class TestSanitizeMetadataValue(unittest.TestCase):
    """Tests for the _sanitize_metadata_value function."""

    def test_sanitize_none(self):
        """Test that None values remain None."""
        self.assertIsNone(_sanitize_metadata_value(None))

    def test_sanitize_primitive_types(self):
        """Test that primitive types (bool, str, bytes, int, float) remain unchanged."""
        self.assertEqual(_sanitize_metadata_value(True), True)
        self.assertEqual(_sanitize_metadata_value(False), False)
        self.assertEqual(_sanitize_metadata_value("test_string"), "test_string")
        self.assertEqual(_sanitize_metadata_value(b"test_bytes"), b"test_bytes")
        self.assertEqual(_sanitize_metadata_value(123), 123)
        self.assertEqual(_sanitize_metadata_value(123.45), 123.45)

    def test_sanitize_lists_and_tuples(self):
        """Test that lists and tuples are properly sanitized."""
        self.assertEqual(_sanitize_metadata_value([1, 2, 3]), ["1", "2", "3"])

        self.assertEqual(_sanitize_metadata_value([1, "test", True, None]), ["1", "test", "True", "None"])

        self.assertEqual(_sanitize_metadata_value((1, 2, 3)), ["1", "2", "3"])

        self.assertEqual(_sanitize_metadata_value([1, [2, 3], 4]), ["1", "['2', '3']", "4"])

    def test_sanitize_complex_objects(self):
        """Test that complex objects are converted to strings."""
        self.assertEqual(_sanitize_metadata_value({"key": "value"}), "{'key': 'value'}")

        class TestObject:
            def __str__(self):
                return "TestObject"

        self.assertEqual(_sanitize_metadata_value(TestObject()), "TestObject")

        self.assertTrue(_sanitize_metadata_value({1, 2, 3}).startswith("{"))
        self.assertTrue(_sanitize_metadata_value({1, 2, 3}).endswith("}"))

        complex_struct = {"key1": [1, 2, 3], "key2": {"nested": "value"}, "key3": TestObject()}
        self.assertTrue(isinstance(_sanitize_metadata_value(complex_struct), str))


class TestOpenTelemetryCallbackHandlerExtended(unittest.TestCase):
    """Additional tests for OpenTelemetryCallbackHandler."""

    def setUp(self):
        self.mock_tracer = Mock()
        self.mock_span = Mock()
        self.mock_tracer.start_span.return_value = self.mock_span
        self.handler = OpenTelemetryCallbackHandler(self.mock_tracer)
        self.run_id = uuid.uuid4()
        self.parent_run_id = uuid.uuid4()

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_chat_model_start(self, mock_context_api):
        """Test the on_chat_model_start method."""
        mock_context_api.get_value.return_value = False

        # Create test messages
        messages = [[HumanMessage(content="Hello, how are you?"), AIMessage(content="I'm doing well, thank you!")]]

        # Create test serialized data
        serialized = {"name": "test_chat_model", "kwargs": {"name": "test_chat_model_name"}}

        # Create test kwargs with invocation_params
        kwargs = {"invocation_params": {"model_id": "gpt-4", "temperature": 0.7, "max_tokens": 100}}

        metadata = {"key": "value"}

        # Create a patched version of _create_span that also updates span_mapping
        def mocked_create_span(run_id, parent_run_id, name, kind, metadata):
            self.handler.span_mapping[run_id] = SpanHolder(self.mock_span, [], time.time(), "gpt-4")
            return self.mock_span

        with patch.object(self.handler, "_create_span", side_effect=mocked_create_span) as mock_create_span:
            # Call on_chat_model_start
            self.handler.on_chat_model_start(
                serialized=serialized,
                messages=messages,
                run_id=self.run_id,
                parent_run_id=self.parent_run_id,
                metadata=metadata,
                **kwargs,
            )

            # Verify _create_span was called with the right parameters
            mock_create_span.assert_called_once_with(
                self.run_id,
                self.parent_run_id,
                f"{GenAIOperationValues.CHAT} gpt-4",
                kind=SpanKind.CLIENT,
                metadata=metadata,
            )

            # Verify span attributes were set correctly
            self.mock_span.set_attribute.assert_any_call(
                SpanAttributes.GEN_AI_OPERATION_NAME, GenAIOperationValues.CHAT
            )

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_chain_error(self, mock_context_api):
        """Test the on_chain_error method."""
        mock_context_api.get_value.return_value = False

        # Create a test error
        test_error = ValueError("Chain error")

        # Add a span to the mapping
        self.handler.span_mapping[self.run_id] = SpanHolder(self.mock_span, [], time.time(), "gpt-4")

        # Patch the _handle_error method
        with patch.object(self.handler, "_handle_error") as mock_handle_error:
            # Call on_chain_error
            self.handler.on_chain_error(error=test_error, run_id=self.run_id, parent_run_id=self.parent_run_id)

            # Verify _handle_error was called with the right parameters
            mock_handle_error.assert_called_once_with(test_error, self.run_id, self.parent_run_id)

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_tool_error(self, mock_context_api):
        """Test the on_tool_error method."""
        mock_context_api.get_value.return_value = False

        # Create a test error
        test_error = ValueError("Tool error")

        # Add a span to the mapping
        self.handler.span_mapping[self.run_id] = SpanHolder(self.mock_span, [], time.time(), "gpt-4")

        # Patch the _handle_error method
        with patch.object(self.handler, "_handle_error") as mock_handle_error:
            # Call on_tool_error
            self.handler.on_tool_error(error=test_error, run_id=self.run_id, parent_run_id=self.parent_run_id)

            # Verify _handle_error was called with the right parameters
            mock_handle_error.assert_called_once_with(test_error, self.run_id, self.parent_run_id)

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_get_name_from_callback(self, mock_context_api):
        """Test the _get_name_from_callback method."""
        mock_context_api.get_value.return_value = False

        # Test with name in kwargs.name
        serialized = {"kwargs": {"name": "test_name_from_kwargs"}}
        name = self.handler._get_name_from_callback(serialized)
        self.assertEqual(name, "test_name_from_kwargs")

        # Test with name in kwargs parameter
        serialized = {}
        kwargs = {"name": "test_name_from_param"}
        name = self.handler._get_name_from_callback(serialized, **kwargs)
        self.assertEqual(name, "test_name_from_param")

        # Test with name in serialized
        serialized = {"name": "test_name_from_serialized"}
        name = self.handler._get_name_from_callback(serialized)
        self.assertEqual(name, "test_name_from_serialized")

        # Test with id in serialized
        serialized = {"id": "abc-123-def"}
        name = self.handler._get_name_from_callback(serialized)
        # self.assertEqual(name, "def")
        self.assertEqual(name, "f")

        # Test with no name information
        serialized = {}
        name = self.handler._get_name_from_callback(serialized)
        self.assertEqual(name, "unknown")

    def test_handle_error(self):
        """Test the _handle_error method directly."""
        # Add a span to the mapping
        self.handler.span_mapping[self.run_id] = SpanHolder(self.mock_span, [], time.time(), "gpt-4")

        # Create a test error
        test_error = ValueError("Test error")

        # Mock the context_api.get_value to return False (don't suppress)
        with patch(
            "amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api"
        ) as mock_context_api:
            mock_context_api.get_value.return_value = False

            # Patch the _end_span method
            with patch.object(self.handler, "_end_span") as mock_end_span:
                # Call _handle_error
                self.handler._handle_error(error=test_error, run_id=self.run_id, parent_run_id=self.parent_run_id)

                # Verify error status was set
                self.mock_span.set_status.assert_called_once()
                self.mock_span.record_exception.assert_called_once_with(test_error)
                mock_end_span.assert_called_once_with(self.mock_span, self.run_id)

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_llm_start_with_suppressed_instrumentation(self, mock_context_api):
        """Test that methods don't proceed when instrumentation is suppressed."""
        # Set suppression key to True
        mock_context_api.get_value.return_value = True

        with patch.object(self.handler, "_create_span") as mock_create_span:
            self.handler.on_llm_start(serialized={}, prompts=["test"], run_id=self.run_id)

            # Verify _create_span was not called
            mock_create_span.assert_not_called()

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_llm_end_without_span(self, mock_context_api):
        """Test on_llm_end when the run_id doesn't have a span."""
        mock_context_api.get_value.return_value = False

        # The run_id doesn't exist in span_mapping
        response = Mock()

        # This should not raise an exception
        self.handler.on_llm_end(
            response=response, run_id=uuid.uuid4()  # Using a different run_id that's not in span_mapping
        )

    @patch("amazon.opentelemetry.distro.opentelemetry.instrumentation.langchain_v2.callback_handler.context_api")
    def test_on_llm_end_with_different_token_usage_keys(self, mock_context_api):
        """Test on_llm_end with different token usage dictionary structures."""
        mock_context_api.get_value.return_value = False

        # Setup the span_mapping
        self.handler.span_mapping[self.run_id] = SpanHolder(self.mock_span, [], time.time(), "gpt-4")

        # Create a mock response with different token usage dictionary structures
        mock_response = Mock()

        # Test with prompt_tokens/completion_tokens
        mock_response.llm_output = {"token_usage": {"prompt_tokens": 10, "completion_tokens": 20}}

        with patch.object(self.handler, "_end_span"):
            self.handler.on_llm_end(response=mock_response, run_id=self.run_id)

            self.mock_span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_USAGE_INPUT_TOKENS, 10)
            self.mock_span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_USAGE_OUTPUT_TOKENS, 20)

        # Reset and test with input_token_count/generated_token_count
        self.mock_span.reset_mock()
        mock_response.llm_output = {"usage": {"input_token_count": 15, "generated_token_count": 25}}

        with patch.object(self.handler, "_end_span"):
            self.handler.on_llm_end(response=mock_response, run_id=self.run_id)

            self.mock_span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_USAGE_INPUT_TOKENS, 15)
            self.mock_span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_USAGE_OUTPUT_TOKENS, 25)

        # Reset and test with input_tokens/output_tokens
        self.mock_span.reset_mock()
        mock_response.llm_output = {"token_usage": {"input_tokens": 30, "output_tokens": 40}}

        with patch.object(self.handler, "_end_span"):
            self.handler.on_llm_end(response=mock_response, run_id=self.run_id)

            self.mock_span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_USAGE_INPUT_TOKENS, 30)
            self.mock_span.set_attribute.assert_any_call(SpanAttributes.GEN_AI_USAGE_OUTPUT_TOKENS, 40)


if __name__ == "__main__":
    unittest.main()
