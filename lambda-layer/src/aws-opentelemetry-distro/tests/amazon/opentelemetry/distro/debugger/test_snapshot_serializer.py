# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest

from amazon.opentelemetry.distro.debugger._snapshot_serializer import SnapshotSerializer


class TestSnapshotSerializerPrimitives(unittest.TestCase):
    """Tests for primitive type serialization."""

    def setUp(self):
        self.serializer = SnapshotSerializer()

    def test_none(self):
        cv = self.serializer.serialize(None)
        self.assertEqual(cv.type, "NoneType")
        self.assertTrue(cv.is_null)

    def test_bool_true(self):
        cv = self.serializer.serialize(True)
        self.assertEqual(cv.type, "bool")
        self.assertEqual(cv.value, "true")

    def test_bool_false(self):
        cv = self.serializer.serialize(False)
        self.assertEqual(cv.type, "bool")
        self.assertEqual(cv.value, "false")

    def test_int(self):
        cv = self.serializer.serialize(42)
        self.assertEqual(cv.type, "int")
        self.assertEqual(cv.value, "42")

    def test_float(self):
        cv = self.serializer.serialize(3.14)
        self.assertEqual(cv.type, "float")
        self.assertEqual(cv.value, "3.14")

    def test_string(self):
        cv = self.serializer.serialize("hello")
        self.assertEqual(cv.type, "str")
        self.assertEqual(cv.value, "hello")
        self.assertFalse(cv.truncated)

    def test_string_truncation(self):
        s = SnapshotSerializer(max_string_length=5)
        cv = s.serialize("abcdefgh")
        self.assertEqual(cv.type, "str")
        self.assertEqual(cv.value, "abcde")
        self.assertTrue(cv.truncated)
        self.assertEqual(cv.size, 8)


class TestSnapshotSerializerCollections(unittest.TestCase):
    """Tests for collection serialization."""

    def setUp(self):
        self.serializer = SnapshotSerializer()

    def test_list(self):
        cv = self.serializer.serialize([1, 2, 3])
        self.assertEqual(cv.type, "list")
        self.assertIsNotNone(cv.elements)
        self.assertEqual(len(cv.elements), 3)
        self.assertEqual(cv.elements[0].value, "1")

    def test_tuple(self):
        cv = self.serializer.serialize((1, 2))
        self.assertEqual(cv.type, "tuple")
        self.assertIsNotNone(cv.elements)
        self.assertEqual(len(cv.elements), 2)

    def test_set(self):
        cv = self.serializer.serialize({1})
        self.assertEqual(cv.type, "set")
        self.assertIsNotNone(cv.elements)
        self.assertEqual(len(cv.elements), 1)

    def test_dict(self):
        cv = self.serializer.serialize({"a": 1, "b": 2})
        self.assertEqual(cv.type, "dict")
        self.assertIsNotNone(cv.entries)
        self.assertEqual(len(cv.entries), 2)
        # Check key/value structure
        entry = cv.entries[0]
        self.assertIn("key", entry)
        self.assertIn("value", entry)

    def test_collection_size_limit(self):
        s = SnapshotSerializer(max_collection_size=2)
        cv = s.serialize([1, 2, 3, 4, 5])
        self.assertEqual(len(cv.elements), 2)
        self.assertTrue(cv.truncated)
        self.assertEqual(cv.size, 5)

    def test_dict_size_limit(self):
        s = SnapshotSerializer(max_collection_size=1)
        cv = s.serialize({"a": 1, "b": 2, "c": 3})
        self.assertEqual(len(cv.entries), 1)
        self.assertTrue(cv.truncated)
        self.assertEqual(cv.size, 3)


class TestSnapshotSerializerDepth(unittest.TestCase):
    """Tests for depth limiting."""

    def test_depth_limit(self):
        s = SnapshotSerializer(max_depth=1)
        cv = s.serialize({"a": {"b": {"c": 1}}})
        # Top-level dict is depth 0, inner dict is depth 1 (at limit)
        self.assertEqual(cv.type, "dict")
        inner = cv.entries[0]
        inner_val = inner["value"]
        # inner_val is the dict {"b": {"c": 1}} at depth 1
        # Its children would be at depth 2, which exceeds max_depth=1
        self.assertEqual(inner_val.type, "dict")

    def test_nested_list_depth(self):
        s = SnapshotSerializer(max_depth=1)
        cv = s.serialize([[1, 2], [3, 4]])
        self.assertEqual(cv.type, "list")
        # Inner lists are at depth 1, their elements at depth 2 -> should still work
        # because primitives don't check depth
        self.assertEqual(len(cv.elements), 2)


class TestSnapshotSerializerObjects(unittest.TestCase):
    """Tests for object serialization."""

    def test_object_with_dict(self):
        class Foo:
            def __init__(self):
                self.x = 1
                self.y = "hello"

        s = SnapshotSerializer()
        cv = s.serialize(Foo())
        self.assertIn("Foo", cv.type)
        self.assertIsNotNone(cv.fields)
        self.assertIn("x", cv.fields)
        self.assertEqual(cv.fields["x"].value, "1")
        self.assertEqual(cv.fields["y"].value, "hello")

    def test_object_field_limit(self):
        class ManyFields:
            def __init__(self):
                for i in range(30):
                    setattr(self, f"field_{i}", i)

        s = SnapshotSerializer(max_fields=5)
        cv = s.serialize(ManyFields())
        self.assertEqual(len(cv.fields), 5)
        self.assertEqual(cv.not_captured_reason, "fieldCount")


class TestSnapshotSerializerCircularRef(unittest.TestCase):
    """Tests for circular reference detection."""

    def test_circular_dict(self):
        d = {}
        d["self"] = d
        s = SnapshotSerializer()
        cv = s.serialize(d)
        # Should not hang; the self-reference should be caught
        self.assertEqual(cv.type, "dict")
        self.assertEqual(len(cv.entries), 1)
        inner = cv.entries[0]["value"]
        self.assertEqual(inner.not_captured_reason, "depth")

    def test_circular_list(self):
        lst = [1]
        lst.append(lst)
        s = SnapshotSerializer()
        cv = s.serialize(lst)
        self.assertEqual(cv.type, "list")
        self.assertEqual(len(cv.elements), 2)
        inner = cv.elements[1]
        self.assertEqual(inner.not_captured_reason, "depth")


class TestSnapshotSerializerSerializeVariables(unittest.TestCase):
    """Tests for serialize_variables batch method."""

    def test_serialize_variables(self):
        s = SnapshotSerializer()
        result = s.serialize_variables({"x": 1, "y": "hello", "z": None})
        self.assertEqual(len(result), 3)
        self.assertEqual(result["x"].value, "1")
        self.assertEqual(result["y"].value, "hello")
        self.assertTrue(result["z"].is_null)
