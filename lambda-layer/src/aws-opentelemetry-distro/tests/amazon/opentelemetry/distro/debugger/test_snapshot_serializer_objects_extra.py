# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Additional SnapshotSerializer tests for object/edge-case fallback branches.

Focuses on the safety branches in _serialize_object (no __dict__, throwing
__dict__ property, broken items()) and the serialize_variables fallbacks that
are not exercised by test_snapshot_serializer.py.
"""

import unittest

from amazon.opentelemetry.distro.debugger._snapshot_serializer import SnapshotSerializer


class TestSerializeObjectFallbacks(unittest.TestCase):
    def setUp(self):
        self.serializer = SnapshotSerializer()

    def test_object_without_dict_uses_repr(self):
        # __slots__ object has no __dict__ -> serializer falls back to repr().
        class Slotted:
            __slots__ = ()

            def __repr__(self):
                return "<slotted repr>"

        cv = self.serializer.serialize(Slotted())
        self.assertIn("Slotted", cv.type)
        self.assertEqual(cv.value, "<slotted repr>")

    def test_object_repr_failure_is_handled(self):
        class BadRepr:
            __slots__ = ()

            def __repr__(self):
                raise RuntimeError("repr exploded")

        cv = self.serializer.serialize(BadRepr())
        self.assertIn("BadRepr", cv.type)
        self.assertIn("repr failed", cv.value)

    def test_object_with_throwing_dict_property_falls_back(self):
        class TrickyDict:
            @property
            def __dict__(self):
                raise RuntimeError("no dict for you")

        cv = self.serializer.serialize(TrickyDict())
        self.assertEqual(cv.not_captured_reason, "fieldCount")

    def test_qualified_type_name_includes_module(self):
        class Local:
            def __init__(self):
                self.value = 1

        cv = self.serializer.serialize(Local())
        # Non-builtin types are prefixed with their module path.
        self.assertTrue(cv.type.endswith("Local"))
        self.assertIn(".", cv.type)


class TestSerializeVariablesFallbacks(unittest.TestCase):
    def test_serialize_variables_timeout_marks_remaining(self):
        # timeout_ms=0 => deadline already passed when serialize_variables loops.
        serializer = SnapshotSerializer(timeout_ms=0)
        result = serializer.serialize_variables({"a": 1, "b": [1, 2, 3]})
        self.assertEqual(set(result.keys()), {"a", "b"})
        # At least one value is marked not-captured due to timeout.
        reasons = {name: cv.not_captured_reason for name, cv in result.items()}
        self.assertIn("timeout", reasons.values())

    def test_serialize_top_level_timeout(self):
        serializer = SnapshotSerializer(timeout_ms=0)
        cv = serializer.serialize([1, 2, 3])
        self.assertEqual(cv.not_captured_reason, "timeout")


if __name__ == "__main__":
    unittest.main()
