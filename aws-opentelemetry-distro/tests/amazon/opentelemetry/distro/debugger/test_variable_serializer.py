# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for the deprecated VariableSerializer backward-compat shim.

The VariableSerializer now delegates to SnapshotSerializer internally.
These tests verify the shim still works for callers that depend on it.
"""

import unittest
from unittest.mock import patch

from amazon.opentelemetry.distro.debugger._data_models import CaptureConfig
from amazon.opentelemetry.distro.debugger._variable_serializer import VariableSerializer


class TestVariableSerializer(unittest.TestCase):
    """Tests for VariableSerializer (deprecated shim over SnapshotSerializer)."""

    def test_safe_json_serialize_none(self):
        """Test serialization of None returns a string representation."""
        result = VariableSerializer.safe_json_serialize(None)
        # Now returns CapturedValue.to_dict() string since backend changed
        self.assertIsInstance(result, str)

    def test_safe_json_serialize_booleans(self):
        """Test serialization of booleans."""
        # Primitives return their value string directly
        self.assertEqual(VariableSerializer.safe_json_serialize(True), "true")
        self.assertEqual(VariableSerializer.safe_json_serialize(False), "false")

    def test_safe_json_serialize_numbers(self):
        """Test serialization of numbers."""
        self.assertEqual(VariableSerializer.safe_json_serialize(42), "42")
        self.assertEqual(VariableSerializer.safe_json_serialize(3.14), "3.14")

    def test_safe_json_serialize_strings(self):
        """Test serialization of strings returns the value."""
        result = VariableSerializer.safe_json_serialize("hello")
        self.assertEqual(result, "hello")

    def test_safe_json_serialize_string_truncation(self):
        """Test string truncation with max_length."""
        long_string = "a" * 150
        result = VariableSerializer.safe_json_serialize(long_string, max_length=20)
        # The new serializer truncates to max_length chars
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) <= 20)

    def test_safe_json_serialize_lists(self):
        """Test serialization of lists returns a string."""
        result = VariableSerializer.safe_json_serialize([1, 2, 3])
        self.assertIsInstance(result, str)

    def test_safe_json_serialize_dicts(self):
        """Test serialization of dicts returns a string."""
        result = VariableSerializer.safe_json_serialize({"a": 1})
        self.assertIsInstance(result, str)

    def test_safe_json_serialize_objects(self):
        """Test serialization of objects with __dict__."""

        class Simple:
            def __init__(self):
                self.x = 1
                self.y = "hello"

        result = VariableSerializer.safe_json_serialize(Simple())
        self.assertIsInstance(result, str)

    def test_capture_variables_basic(self):
        """Test basic variable capture returns string values."""
        config = CaptureConfig()
        variables = {"x": 5, "y": "test"}

        result = VariableSerializer.capture_variables_as_attributes(
            variables=variables,
            attribute_prefix="var.",
            capture_config=config,
        )

        self.assertIn("var.x", result)
        self.assertIn("var.y", result)
        # Values should be strings
        self.assertIsInstance(result["var.x"], str)
        self.assertIsInstance(result["var.y"], str)

    def test_capture_variables_with_filter(self):
        """Test variable capture with filter."""
        config = CaptureConfig()
        variables = {"x": 1, "y": 2, "z": 3}

        result = VariableSerializer.capture_variables_as_attributes(
            variables=variables,
            attribute_prefix="var.",
            capture_config=config,
            variable_filter=["x", "z"],
        )

        self.assertIn("var.x", result)
        self.assertIn("var.z", result)
        self.assertNotIn("var.y", result)

    def test_capture_variables_respects_max_fields(self):
        """Test that capture respects max_fields_per_object limit."""
        config = CaptureConfig(max_fields_per_object=2)
        variables = {"a": 1, "b": 2, "c": 3, "d": 4}

        result = VariableSerializer.capture_variables_as_attributes(
            variables=variables,
            attribute_prefix="var.",
            capture_config=config,
        )

        self.assertEqual(len(result), 2)

    def test_capture_variables_handles_serialization_error(self):
        """Test graceful handling of serialization errors."""
        config = CaptureConfig()

        with patch(
            "amazon.opentelemetry.distro.debugger._variable_serializer.SnapshotSerializer.serialize",
            side_effect=ValueError("test error"),
        ):
            result = VariableSerializer.capture_variables_as_attributes(
                variables={"bad": object()},
                attribute_prefix="var.",
                capture_config=config,
            )

        self.assertIn("var.bad", result)
        self.assertIn("<serialization error: ValueError>", result["var.bad"])

    def test_capture_variables_empty_dict(self):
        """Test capture with empty variables dict."""
        config = CaptureConfig()

        result = VariableSerializer.capture_variables_as_attributes(
            variables={},
            attribute_prefix="var.",
            capture_config=config,
        )

        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
