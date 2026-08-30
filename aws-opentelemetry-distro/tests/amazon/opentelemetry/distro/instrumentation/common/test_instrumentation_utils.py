import json
from base64 import b64encode
from unittest import TestCase

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    attach_otel_context,
    content_to_parts,
    serialize_to_json_string,
    skip_instrumentation_if_suppressed,
    to_tool_attribute_value,
    try_detach,
    try_unwrap,
    try_wrap,
)
from opentelemetry import context
from opentelemetry.context import _SUPPRESS_HTTP_INSTRUMENTATION_KEY, _SUPPRESS_INSTRUMENTATION_KEY, create_key
from opentelemetry.trace import set_span_in_context


class TestInstrumentationUtils(TestCase):
    def test_content_to_parts_openai_text_blocks(self):
        self.assertEqual(
            content_to_parts(
                [
                    {"type": "input_text", "text": "input"},
                    {"type": "output_text", "text": "output"},
                    {"type": "summary_text", "text": "summary"},
                ]
            ),
            [
                {"type": "text", "content": "input"},
                {"type": "text", "content": "output"},
                {"type": "text", "content": "summary"},
            ],
        )

    def test_serialize_basic_types(self):
        self.assertEqual(serialize_to_json_string({"key": "value"}), '{"key": "value"}')
        self.assertEqual(serialize_to_json_string([1, 2, 3]), "[1, 2, 3]")
        self.assertEqual(serialize_to_json_string("hello"), '"hello"')

    def test_serialize_depth_truncation(self):
        deep = {"a": {"b": {"c": "val"}}}
        result = serialize_to_json_string(deep, max_depth=2)
        self.assertIn("...", result)

    def test_serialize_nested_structures(self):
        data = {"items": [{"name": "test", "nested": {"deep": True}}]}
        result = serialize_to_json_string(data, max_depth=5)
        self.assertIn("test", result)

    def test_try_wrap_and_unwrap(self):
        call_count = [0]

        def wrapper(wrapped_fn, instance, args, kwargs):
            call_count[0] += 1
            return wrapped_fn(*args, **kwargs)

        try_wrap("json", "dumps", wrapper)
        json.dumps({"test": True})
        self.assertEqual(call_count[0], 1)
        try_unwrap(json, "dumps")

    def test_try_wrap_nonexistent_module(self):
        try_wrap("nonexistent_module_xyz_123", "func", lambda *a, **k: None)

    def test_try_wrap_with_should_wrap_false(self):
        try_wrap("json", "dumps", lambda *a, **k: None, should_wrap=lambda: False)

    def test_serialize_non_serializable(self):
        obj = object()
        result = serialize_to_json_string(obj)
        self.assertEqual(result, str(obj))

    def test_serialize_bytes_base64_encoded(self):
        raw = b"\x89PNG\r\n"
        expected = b64encode(raw).decode()
        self.assertEqual(serialize_to_json_string(raw), json.dumps(expected))

    def test_serialize_bytes_nested_in_structure(self):
        raw = b"\x89PNG"
        result = serialize_to_json_string({"parts": [{"type": "blob", "content": raw}]})
        self.assertIn(b64encode(raw).decode(), result)
        self.assertIn('"content"', result)

    def test_to_tool_attribute_value_dict_with_bytes_base64_encoded(self):
        raw = b"\x89PNG\r\n"
        result = to_tool_attribute_value({"parts": [{"type": "blob", "content": raw}]})
        self.assertEqual(json.loads(result), {"parts": [{"type": "blob", "content": b64encode(raw).decode()}]})

    def test_to_tool_attribute_value_primitives_kept_native(self):
        self.assertEqual(to_tool_attribute_value("Hello, World!"), "Hello, World!")
        self.assertEqual(to_tool_attribute_value(42), 42)
        self.assertEqual(to_tool_attribute_value(3.14), 3.14)
        self.assertEqual(to_tool_attribute_value(True), True)
        self.assertEqual(to_tool_attribute_value(b"raw"), b"raw")

    def test_to_tool_attribute_value_none(self):
        self.assertIsNone(to_tool_attribute_value(None))

    def test_to_tool_attribute_value_dict_and_list_json_encoded(self):
        self.assertEqual(to_tool_attribute_value({"city": "Paris"}), '{"city":"Paris"}')
        self.assertEqual(to_tool_attribute_value([1, 2, 3]), "[1,2,3]")

    def test_to_tool_attribute_value_unserializable_falls_back_to_str(self):
        obj = object()
        self.assertEqual(to_tool_attribute_value(obj), str(obj))
        value = {"unsupported": obj}
        self.assertEqual(to_tool_attribute_value(value), str(value))

    def test_try_unwrap_not_wrapped(self):
        try_unwrap(json, "dumps")

    def test_try_unwrap_exception(self):
        try_unwrap("invalid", "x")

    def test_try_detach_invalid_token(self):
        token = context.attach(set_span_in_context(None))
        context.detach(token)
        try_detach(token)

    def test_attach_otel_context_with_http_suppression(self):
        key = create_key("test_attach_otel_context")
        previous_context = context.get_current()
        token = attach_otel_context(context.set_value(key, "value"), suppress_http=True)

        self.assertEqual(context.get_value(key), "value")
        self.assertTrue(context.get_value(_SUPPRESS_HTTP_INSTRUMENTATION_KEY))

        try_detach(token)
        self.assertIs(context.get_current(), previous_context)
        self.assertIsNone(context.get_value(_SUPPRESS_HTTP_INSTRUMENTATION_KEY))

    def test_skip_instrumentation_if_suppressed_allows_when_not_suppressed(self):
        call_count = [0]

        @skip_instrumentation_if_suppressed
        def my_callback(self):
            call_count[0] += 1

        my_callback(None)
        self.assertEqual(call_count[0], 1)

    def test_skip_instrumentation_if_suppressed_blocks_when_suppressed(self):
        call_count = [0]

        @skip_instrumentation_if_suppressed
        def my_callback(self):
            call_count[0] += 1

        token = context.attach(context.set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        try:
            result = my_callback(None)
            self.assertIsNone(result)
            self.assertEqual(call_count[0], 0)
        finally:
            context.detach(token)
