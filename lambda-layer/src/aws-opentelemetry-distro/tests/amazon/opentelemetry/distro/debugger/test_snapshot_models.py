# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest

from amazon.opentelemetry.distro.debugger._snapshot_models import (
    CapturedContext,
    CapturedThrowable,
    CapturedValue,
    Captures,
    InstrumentationDetails,
    InstrumentationLocation,
    Snapshot,
    StackFrame,
    ThreadInfo,
    TraceContext,
)


class TestCapturedValue(unittest.TestCase):
    """Tests for CapturedValue serialization to dict."""

    def test_primitive_value(self):
        cv = CapturedValue(type="int", value="42")
        d = cv.to_dict()
        self.assertEqual(d, {"type": "int", "value": "42"})

    def test_null_value(self):
        cv = CapturedValue(type="NoneType", is_null=True)
        d = cv.to_dict()
        self.assertEqual(d, {"type": "NoneType", "is_null": True})

    def test_not_captured_reason(self):
        cv = CapturedValue(type="str", not_captured_reason="depth")
        d = cv.to_dict()
        self.assertEqual(d, {"type": "str", "not_captured_reason": "depth"})
        # Should NOT contain value/fields/elements/entries
        self.assertNotIn("value", d)
        self.assertNotIn("fields", d)

    def test_fields_value(self):
        cv = CapturedValue(
            type="MyClass",
            fields={"x": CapturedValue(type="int", value="1")},
        )
        d = cv.to_dict()
        self.assertEqual(d["type"], "MyClass")
        self.assertEqual(d["fields"]["x"], {"type": "int", "value": "1"})

    def test_elements_value(self):
        cv = CapturedValue(
            type="list",
            elements=[
                CapturedValue(type="int", value="1"),
                CapturedValue(type="int", value="2"),
            ],
        )
        d = cv.to_dict()
        self.assertEqual(len(d["elements"]), 2)
        self.assertEqual(d["elements"][0]["value"], "1")

    def test_entries_value(self):
        cv = CapturedValue(
            type="dict",
            entries=[
                {
                    "key": CapturedValue(type="str", value="k"),
                    "value": CapturedValue(type="int", value="1"),
                }
            ],
        )
        d = cv.to_dict()
        self.assertEqual(len(d["entries"]), 1)
        self.assertEqual(d["entries"][0]["key"]["value"], "k")
        self.assertEqual(d["entries"][0]["value"]["value"], "1")

    def test_truncated_flag(self):
        cv = CapturedValue(type="str", value="abc", truncated=True, size=1000)
        d = cv.to_dict()
        self.assertTrue(d["truncated"])
        self.assertEqual(d["size"], 1000)

    def test_truncated_not_present_when_false(self):
        cv = CapturedValue(type="int", value="42")
        d = cv.to_dict()
        self.assertNotIn("truncated", d)
        self.assertNotIn("size", d)


class TestCapturedThrowable(unittest.TestCase):
    def test_to_dict(self):
        t = CapturedThrowable(
            type="ValueError",
            message="bad value",
            stacktrace=[StackFrame(file_name="test.py", function="foo", line_number=10)],
        )
        d = t.to_dict()
        self.assertEqual(d["type"], "ValueError")
        self.assertEqual(d["message"], "bad value")
        self.assertEqual(len(d["stacktrace"]), 1)
        self.assertEqual(d["stacktrace"][0]["file_path"], "test.py")


class TestCapturedContext(unittest.TestCase):
    def test_empty_context(self):
        ctx = CapturedContext()
        self.assertEqual(ctx.to_dict(), {})

    def test_with_arguments(self):
        ctx = CapturedContext(arguments={"x": CapturedValue(type="int", value="1")})
        d = ctx.to_dict()
        self.assertIn("arguments", d)
        self.assertEqual(d["arguments"]["x"]["value"], "1")

    def test_with_return_value(self):
        ctx = CapturedContext(return_value=CapturedValue(type="str", value="ok"))
        d = ctx.to_dict()
        self.assertIn("return_value", d)
        self.assertEqual(d["return_value"]["value"], "ok")

    def test_with_throwable(self):
        ctx = CapturedContext(throwable=CapturedThrowable(type="RuntimeError", message="oops"))
        d = ctx.to_dict()
        self.assertIn("throwable", d)
        self.assertEqual(d["throwable"]["type"], "RuntimeError")


class TestCaptures(unittest.TestCase):
    def test_function_level_captures(self):
        captures = Captures(
            entry=CapturedContext(arguments={"a": CapturedValue(type="int", value="1")}),
            return_context=CapturedContext(return_value=CapturedValue(type="int", value="2")),
        )
        d = captures.to_dict()
        self.assertIn("entry", d)
        self.assertIn("return", d)
        self.assertNotIn("lines", d)

    def test_line_level_captures(self):
        captures = Captures(
            lines={
                32: CapturedContext(locals={"x": CapturedValue(type="int", value="5")}),
            }
        )
        d = captures.to_dict()
        self.assertIn("lines", d)
        self.assertIn("32", d["lines"])
        self.assertEqual(d["lines"]["32"]["locals"]["x"]["value"], "5")


class TestSnapshot(unittest.TestCase):
    def test_minimal_snapshot(self):
        s = Snapshot(timestamp=1700000000000)
        d = s.to_dict()
        self.assertIn("id", d)
        self.assertEqual(d["timestamp"], 1700000000000)
        # language is no longer top-level per spec
        self.assertNotIn("language", d)
        # UUID format check
        self.assertEqual(len(d["id"].split("-")), 5)

    def test_full_snapshot(self):
        s = Snapshot(
            timestamp=1700000000000,
            duration=5000000,
            location_hash="hash123",
            instrumentation=InstrumentationDetails(
                location=InstrumentationLocation(
                    code_unit="myapp.services",
                    class_name="myapp.services",
                    method_name="process",
                    file_path="services.py",
                    line_number=42,
                ),
            ),
            trace=TraceContext(
                trace_id="0" * 32,
                span_id="0" * 16,
            ),
            thread=ThreadInfo(id=12345, name="MainThread"),
            stack=[StackFrame(file_name="app.py", function="main", line_number=10)],
            captures=Captures(
                entry=CapturedContext(arguments={"x": CapturedValue(type="int", value="1")}),
            ),
        )
        d = s.to_dict()
        self.assertEqual(d["duration"], 5000000)
        self.assertEqual(d["location_hash"], "hash123")
        self.assertNotIn("id", d["instrumentation"])
        self.assertEqual(d["instrumentation"]["location"]["code_unit"], "myapp.services")
        self.assertEqual(d["instrumentation"]["location"]["class_name"], "myapp.services")
        self.assertEqual(d["instrumentation"]["location"]["method_name"], "process")
        self.assertEqual(d["instrumentation"]["location"]["line_number"], 42)
        self.assertEqual(d["instrumentation"]["location"]["file_path"], "services.py")
        self.assertEqual(d["instrumentation"]["location"]["language"], "python")
        self.assertEqual(d["trace"]["trace_id"], "0" * 32)
        self.assertEqual(d["thread"]["name"], "MainThread")
        self.assertEqual(len(d["stack"]), 1)
        self.assertIn("captures", d)

    def test_optional_fields_omitted_when_none(self):
        s = Snapshot(timestamp=1700000000000)
        d = s.to_dict()
        self.assertNotIn("duration", d)
        self.assertNotIn("location_hash", d)
        self.assertNotIn("instrumentation", d)
        self.assertNotIn("trace", d)
        self.assertNotIn("thread", d)
        self.assertNotIn("stack", d)
        self.assertNotIn("captures", d)


class TestStackFrame(unittest.TestCase):
    def test_to_dict(self):
        f = StackFrame(file_name="test.py", function="foo", line_number=10)
        self.assertEqual(f.to_dict(), {"file_path": "test.py", "function": "foo", "line_number": 10})

    def test_default_line_number(self):
        f = StackFrame(file_name="test.py", function="bar")
        self.assertEqual(f.to_dict()["line_number"], 0)
