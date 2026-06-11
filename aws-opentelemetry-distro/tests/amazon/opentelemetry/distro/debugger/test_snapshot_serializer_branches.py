# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Branch-coverage tests for SnapshotSerializer timeout/error paths.

These complement test_snapshot_serializer.py and test_snapshot_serializer_objects_extra.py
by driving the mid-iteration timeout checks in _serialize_dict / _serialize_collection /
_serialize_object (reached by passing an already-elapsed deadline directly), the
broken-items() fallback, the repr-truncation helper, and the broad except branches in
serialize() and serialize_variables() (driven by making _serialize_value raise).
"""

import time
import unittest
from unittest import mock

from amazon.opentelemetry.distro.debugger._snapshot_serializer import SnapshotSerializer


class TestMidIterationTimeouts(unittest.TestCase):
    """Covers the per-element timeout checks inside the compound serializers (140, 161, 203)."""

    def setUp(self):
        self.serializer = SnapshotSerializer()

    def _elapsed_deadline(self):
        # A deadline already in the past so the first in-loop timeout check fires.
        return time.monotonic() - 1.0

    def test_dict_mid_iteration_timeout(self):
        cv = self.serializer._serialize_dict({"a": 1, "b": 2}, depth=0, deadline=self._elapsed_deadline(), seen=set())
        self.assertEqual(cv.type, "dict")
        self.assertEqual(cv.not_captured_reason, "timeout")

    def test_collection_mid_iteration_timeout(self):
        cv = self.serializer._serialize_collection([1, 2, 3], depth=0, deadline=self._elapsed_deadline(), seen=set())
        self.assertEqual(cv.type, "list")
        self.assertEqual(cv.not_captured_reason, "timeout")

    def test_object_field_mid_iteration_timeout(self):
        class Holder:
            def __init__(self):
                self.first = 1
                self.second = 2

        cv = self.serializer._serialize_object(Holder(), depth=0, deadline=self._elapsed_deadline(), seen=set())
        self.assertEqual(cv.not_captured_reason, "timeout")


class TestObjectBrokenItems(unittest.TestCase):
    """Covers the broken-items() fallback in _serialize_object (lines 195-197)."""

    def setUp(self):
        self.serializer = SnapshotSerializer()

    def test_broken_dict_items_falls_back_to_field_count(self):
        class BrokenItemsDict(dict):
            def items(self):
                raise RuntimeError("items exploded")

        broken = BrokenItemsDict()
        broken["x"] = 1

        class Holder:
            # __dict__ resolves successfully, but its items() raises -> except (195-197).
            @property
            def __dict__(self):
                return broken

        cv = self.serializer.serialize(Holder())
        self.assertEqual(cv.not_captured_reason, "fieldCount")


class TestTruncateStrHelper(unittest.TestCase):
    """Covers the truncation branch of _truncate_str (line 216) via the repr fallback."""

    def test_long_repr_is_truncated(self):
        class LongRepr:
            __slots__ = ()

            def __repr__(self):
                return "x" * 500

        serializer = SnapshotSerializer(max_string_length=10)
        cv = serializer.serialize(LongRepr())
        # No __dict__ -> repr fallback -> _truncate_str trims to max_string_length.
        self.assertEqual(len(cv.value), 10)


class TestSerializeBroadExcept(unittest.TestCase):
    """Covers the broad except in serialize (63-65) and serialize_variables (237-238)."""

    def test_serialize_swallows_serialize_value_error(self):
        serializer = SnapshotSerializer()
        with mock.patch.object(serializer, "_serialize_value", side_effect=RuntimeError("boom")):
            cv = serializer.serialize([1, 2, 3])
        self.assertEqual(cv.type, "list")
        self.assertEqual(cv.not_captured_reason, "serializationError")

    def test_serialize_variables_swallows_per_var_error(self):
        serializer = SnapshotSerializer()
        with mock.patch.object(serializer, "_serialize_value", side_effect=RuntimeError("boom")):
            result = serializer.serialize_variables({"alpha": 123})
        self.assertEqual(result["alpha"].type, "int")
        self.assertEqual(result["alpha"].not_captured_reason, "timeout")


if __name__ == "__main__":
    unittest.main()
