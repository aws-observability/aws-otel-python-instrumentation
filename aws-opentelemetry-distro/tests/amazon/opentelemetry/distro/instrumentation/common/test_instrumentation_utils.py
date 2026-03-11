import json
from unittest import TestCase

from amazon.opentelemetry.distro.instrumentation.common.instrumentation_utils import (
    serialize_to_json_string,
    try_detach,
    try_unwrap,
    try_wrap,
)
from opentelemetry import context
from opentelemetry.trace import set_span_in_context


class TestInstrumentationUtils(TestCase):
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

    def test_try_unwrap_not_wrapped(self):
        try_unwrap(json, "dumps")

    def test_try_unwrap_exception(self):
        try_unwrap("invalid", "x")

    def test_try_detach_invalid_token(self):
        token = context.attach(set_span_in_context(None))
        context.detach(token)
        try_detach(token)
