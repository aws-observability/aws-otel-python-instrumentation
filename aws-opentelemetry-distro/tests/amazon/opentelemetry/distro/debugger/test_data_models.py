# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import patch

from amazon.opentelemetry.distro.debugger._data_models import (
    DEFAULT_MAX_COLLECTION_DEPTH,
    DEFAULT_MAX_COLLECTION_WIDTH,
    DEFAULT_MAX_FIELDS_PER_OBJECT,
    DEFAULT_MAX_OBJECT_DEPTH,
    DEFAULT_MAX_STACK_FRAMES,
    DEFAULT_MAX_STACK_TRACE_SIZE,
    DEFAULT_MAX_STRING_LENGTH,
    DEFAULT_RETURN_ATTRIBUTE_NAME,
    BreakpointConfiguration,
    BreakpointState,
    CaptureConfig,
    FunctionBreakpointSet,
)


class TestCaptureConfig(unittest.TestCase):
    """Tests for CaptureConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = CaptureConfig()

        self.assertIsNone(config.capture_arguments)
        self.assertIsNone(config.capture_locals)
        self.assertFalse(config.capture_return)
        self.assertFalse(config.capture_stack_trace)
        self.assertEqual(config.arg_mappings, {})
        self.assertEqual(config.return_attribute_name, DEFAULT_RETURN_ATTRIBUTE_NAME)
        self.assertEqual(config.max_string_length, DEFAULT_MAX_STRING_LENGTH)
        self.assertEqual(config.max_collection_width, DEFAULT_MAX_COLLECTION_WIDTH)
        self.assertEqual(config.max_collection_depth, DEFAULT_MAX_COLLECTION_DEPTH)
        self.assertEqual(config.max_stack_frames, DEFAULT_MAX_STACK_FRAMES)
        self.assertEqual(config.max_stack_trace_size, DEFAULT_MAX_STACK_TRACE_SIZE)
        self.assertEqual(config.max_object_depth, DEFAULT_MAX_OBJECT_DEPTH)
        self.assertEqual(config.max_fields_per_object, DEFAULT_MAX_FIELDS_PER_OBJECT)

    def test_custom_values(self):
        """Test custom configuration values."""
        config = CaptureConfig(
            capture_arguments=["user_id", "order_id"],
            capture_return=True,
            arg_mappings={"user_id": "custom.user.id"},
            max_string_length=100,
        )

        self.assertEqual(config.capture_arguments, ["user_id", "order_id"])
        self.assertTrue(config.capture_return)
        self.assertEqual(config.arg_mappings, {"user_id": "custom.user.id"})
        self.assertEqual(config.max_string_length, 100)

    def test_validation_clamping(self):
        """Test clamping behavior."""
        test_cases = {
            "max_string_length": {
                "below_min": 0,
                "min_val": 1,
                "above_max": 300,
                "max_val": 255,
                "default": DEFAULT_MAX_STRING_LENGTH,
            },
            "max_collection_width": {
                "below_min": 0,
                "min_val": 1,
                "above_max": 25,
                "max_val": 20,
                "default": DEFAULT_MAX_COLLECTION_WIDTH,
            },
            "max_collection_depth": {
                "below_min": 0,
                "min_val": 1,
                "above_max": 10,
                "max_val": 5,
                "default": DEFAULT_MAX_COLLECTION_DEPTH,
            },
            "max_stack_frames": {
                "below_min": 0,
                "min_val": 1,
                "above_max": 25,
                "max_val": 20,
                "default": DEFAULT_MAX_STACK_FRAMES,
            },
            "max_stack_trace_size": {
                "below_min": 0,
                "min_val": 1,
                "above_max": 1500,
                "max_val": 1000,
                "default": DEFAULT_MAX_STACK_TRACE_SIZE,
            },
            "max_object_depth": {
                "below_min": 0,
                "min_val": 1,
                "above_max": 10,
                "max_val": 5,
                "default": DEFAULT_MAX_OBJECT_DEPTH,
            },
            "max_fields_per_object": {
                "below_min": 0,
                "min_val": 1,
                "above_max": 25,
                "max_val": 20,
                "default": DEFAULT_MAX_FIELDS_PER_OBJECT,
            },
        }

        for field_name, values in test_cases.items():
            with self.subTest(field=field_name):
                # Test below minimum - should clamp to minimum
                config = CaptureConfig(**{field_name: values["below_min"]})
                self.assertEqual(getattr(config, field_name), values["min_val"])

                # Test above maximum - should clamp to maximum
                config = CaptureConfig(**{field_name: values["above_max"]})
                self.assertEqual(getattr(config, field_name), values["max_val"])

                # Test invalid type - should use default
                config = CaptureConfig(**{field_name: "invalid"})
                self.assertEqual(getattr(config, field_name), values["default"])

    def test_validation_invalid_return_attribute_name(self):
        """Test validation uses default for invalid return_attribute_name."""
        test_cases = {
            "empty_string": ("", DEFAULT_RETURN_ATTRIBUTE_NAME),
            "whitespace_only": ("   ", DEFAULT_RETURN_ATTRIBUTE_NAME),
            "none_value": (None, DEFAULT_RETURN_ATTRIBUTE_NAME),
            "non_string_int": (123, DEFAULT_RETURN_ATTRIBUTE_NAME),
            "non_string_list": (["invalid"], DEFAULT_RETURN_ATTRIBUTE_NAME),
            "non_string_dict": ({"invalid": "value"}, DEFAULT_RETURN_ATTRIBUTE_NAME),
            "valid_string": ("custom.attribute", "custom.attribute"),
        }

        for case_name, (input_value, expected_value) in test_cases.items():
            with self.subTest(case=case_name):
                config = CaptureConfig(return_attribute_name=input_value)
                self.assertEqual(config.return_attribute_name, expected_value)


class TestBreakpointConfiguration(unittest.TestCase):
    """Tests for BreakpointConfiguration dataclass."""

    def test_valid_line_breakpoint(self):
        """Test valid line breakpoint (line_number > 0)."""
        config = BreakpointConfiguration(
            module="myapp.services",
            function_name="process_order",
            line_number=10,
            capture_config=CaptureConfig(),
            config_id="test-id-1",
        )

        self.assertEqual(config.function_key, "myapp.services.process_order")
        self.assertEqual(config.breakpoint_key, "myapp.services.process_order:10")
        self.assertTrue(config.is_valid)
        self.assertTrue(config.is_line_breakpoint)

    def test_line_breakpoint(self):
        """Test line breakpoint (line_number>0)."""
        config = BreakpointConfiguration(
            module="myapp.services",
            function_name="process_order",
            line_number=42,
            capture_config=CaptureConfig(),
            config_id="test-id-2",
        )

        self.assertEqual(config.function_key, "myapp.services.process_order")
        self.assertEqual(config.breakpoint_key, "myapp.services.process_order:42")
        self.assertTrue(config.is_valid)
        self.assertTrue(config.is_line_breakpoint)

    def test_class_method_breakpoint(self):
        """Test breakpoint for class method."""
        config = BreakpointConfiguration(
            module="myapp.services",
            function_name="OrderProcessor.process",
            line_number=10,
            capture_config=CaptureConfig(),
            config_id="test-id-3",
        )

        self.assertEqual(config.function_key, "myapp.services.OrderProcessor.process")
        self.assertEqual(config.breakpoint_key, "myapp.services.OrderProcessor.process:10")

    def test_from_api_config_complete(self):
        """Test parsing complete API response (new union format)."""
        api_config = {
            "Location": {
                "CodeLocation": {
                    "Language": "python",
                    "CodeUnit": "myapp.services",
                    "MethodName": "process_order",
                    "LineNumber": 42,
                }
            },
            "CaptureConfiguration": {
                "CodeCapture": {
                    "CaptureReturn": False,
                    "CaptureStackTrace": True,
                    "CaptureArguments": ["user_id", "order_id"],
                    "CaptureLocals": [],
                    "arg_mappings": {"user_id": "custom.user"},
                    "return_attribute_name": "custom.return",
                    "CaptureLimits": {
                        "MaxStringLength": 150,
                        "MaxStackFrames": 5,
                        "MaxStackTraceSize": 512,
                        "MaxHits": 50,
                    },
                }
            },
            "AttributeFilters": [{"service.name": "my-service"}],
            "ExpiresAt": 1700000000,
            "LocationHash": "config-123",
        }

        config = BreakpointConfiguration.from_api_config(api_config)

        self.assertEqual(config.module, "myapp.services")
        self.assertEqual(config.function_name, "process_order")
        self.assertEqual(config.line_number, 42)
        self.assertEqual(config.config_id, "config-123")
        self.assertIsNotNone(config.expires_at)
        self.assertEqual(len(config.attribute_filters), 1)

        self.assertEqual(config.capture_config.capture_arguments, ["user_id", "order_id"])
        self.assertFalse(config.capture_config.capture_return)
        self.assertTrue(config.capture_config.capture_stack_trace)
        self.assertEqual(config.capture_config.arg_mappings, {"user_id": "custom.user"})
        self.assertEqual(config.capture_config.return_attribute_name, "custom.return")
        self.assertEqual(config.capture_config.max_string_length, 150)

    def test_from_api_config_minimal(self):
        """Test parsing minimal API response with defaults."""
        api_config = {
            "Location": {
                "CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "myfunc", "LineNumber": 15}
            },
            "CaptureConfiguration": {"CodeCapture": {}},
            "LocationHash": "config-456",
        }

        config = BreakpointConfiguration.from_api_config(api_config)

        self.assertEqual(config.module, "myapp")
        self.assertEqual(config.function_name, "myfunc")
        self.assertEqual(config.line_number, 15)
        self.assertEqual(config.config_id, "config-456")
        self.assertIsNone(config.expires_at)
        self.assertEqual(config.attribute_filters, [])
        self.assertTrue(config.is_valid)

        self.assertIsNone(config.capture_config.capture_arguments)
        self.assertFalse(config.capture_config.capture_return)

    def test_from_api_config_missing_capture_fields_are_none(self):
        """Test that missing CaptureArguments/CaptureLocals result in None (do not capture)."""
        api_config = {
            "Location": {
                "CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "myfunc", "LineNumber": 15}
            },
            "CaptureConfiguration": {"CodeCapture": {"CaptureReturn": True}},
            "LocationHash": "config-789",
        }

        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertIsNone(config.capture_config.capture_arguments)
        self.assertIsNone(config.capture_config.capture_locals)

    def test_from_api_config_empty_capture_fields_mean_capture_all(self):
        """Test that empty [] CaptureArguments/CaptureLocals mean capture all."""
        api_config = {
            "Location": {
                "CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "myfunc", "LineNumber": 15}
            },
            "CaptureConfiguration": {"CodeCapture": {"CaptureArguments": [], "CaptureLocals": []}},
            "LocationHash": "config-790",
        }

        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertIsNotNone(config.capture_config.capture_arguments)
        self.assertEqual(config.capture_config.capture_arguments, [])
        self.assertIsNotNone(config.capture_config.capture_locals)
        self.assertEqual(config.capture_config.capture_locals, [])

    def test_from_api_config_invalid_type_capture_fields_treated_as_empty(self):
        """Test that invalid types for CaptureArguments/CaptureLocals are treated as [] (capture all)."""
        api_config = {
            "Location": {
                "CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "myfunc", "LineNumber": 15}
            },
            "CaptureConfiguration": {"CodeCapture": {"CaptureArguments": "not-a-list", "CaptureLocals": 123}},
            "LocationHash": "config-791",
        }

        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertIsNotNone(config.capture_config.capture_arguments)
        self.assertEqual(config.capture_config.capture_arguments, [])
        self.assertIsNotNone(config.capture_config.capture_locals)
        self.assertEqual(config.capture_config.capture_locals, [])

    def test_from_api_config_mixed_missing_and_present_capture_fields(self):
        """Test mixed scenario: CaptureArguments missing, CaptureLocals present as empty."""
        api_config = {
            "Location": {
                "CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "myfunc", "LineNumber": 15}
            },
            "CaptureConfiguration": {"CodeCapture": {"CaptureLocals": []}},
            "LocationHash": "config-792",
        }

        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config)
        self.assertIsNone(config.capture_config.capture_arguments)
        self.assertIsNotNone(config.capture_config.capture_locals)
        self.assertEqual(config.capture_config.capture_locals, [])

    def test_from_api_config_with_and_without_classname(self):
        """Test function name construction with and without ClassName."""
        test_cases = {
            "with_classname": {
                "api_config": {
                    "Location": {
                        "CodeLocation": {
                            "Language": "python",
                            "CodeUnit": "myapp",
                            "ClassName": "MyClass",
                            "MethodName": "my_method",
                        }
                    },
                    "LocationHash": "test-1",
                },
                "expected_function_name": "MyClass.my_method",
            },
            "without_classname": {
                "api_config": {
                    "Location": {
                        "CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "my_function"}
                    },
                    "LocationHash": "test-2",
                },
                "expected_function_name": "my_function",
            },
        }

        for case_name, test_data in test_cases.items():
            with self.subTest(case=case_name):
                config = BreakpointConfiguration.from_api_config(test_data["api_config"])
                self.assertEqual(config.function_name, test_data["expected_function_name"])

    def test_from_api_config_expires_at(self):
        """Test parsing ExpiresAt in different formats."""
        test_cases = {
            "unix_timestamp": {
                "api_config": {
                    "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func"}},
                    "ExpiresAt": 1700000000,
                    "LocationHash": "test-1",
                },
                "has_expires_at": True,
            },
            "iso_string": {
                "api_config": {
                    "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func"}},
                    "ExpiresAt": "2025-10-27T19:34:00Z",
                    "LocationHash": "test-2",
                },
                "has_expires_at": True,
            },
            "invalid_format": {
                "api_config": {
                    "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func"}},
                    "ExpiresAt": "invalid-date",
                    "LocationHash": "test-3",
                },
                "has_expires_at": False,
            },
            "missing": {
                "api_config": {
                    "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func"}},
                    "LocationHash": "test-4",
                },
                "has_expires_at": False,
            },
        }

        for case_name, test_data in test_cases.items():
            with self.subTest(case=case_name):
                config = BreakpointConfiguration.from_api_config(test_data["api_config"])
                if test_data["has_expires_at"]:
                    self.assertIsNotNone(config.expires_at)
                else:
                    self.assertIsNone(config.expires_at)

    def test_from_api_config_iso_without_offset_is_utc_aware(self):
        api_config = {
            "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func"}},
            "ExpiresAt": "2025-10-27T19:34:00",
            "CreatedAt": "2025-10-27T18:00:00",
            "LocationHash": "naive-iso",
        }
        config = BreakpointConfiguration.from_api_config(api_config)
        self.assertIsNotNone(config.expires_at)
        self.assertIsNotNone(config.expires_at.tzinfo)
        self.assertIsNotNone(config.created_at)
        self.assertIsNotNone(config.created_at.tzinfo)

    def test_from_api_config_invalid_cases(self):
        """Test all cases where from_api_config returns None."""
        test_cases = {
            "invalid_location_type": {"Location": "not_a_dict", "LocationHash": "test"},
            "non_python_language": {
                "Location": {"CodeLocation": {"Language": "java", "CodeUnit": "myapp", "MethodName": "func"}},
                "LocationHash": "test",
            },
            "missing_module": {
                "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "", "MethodName": "func"}},
                "LocationHash": "test",
            },
            "missing_function": {
                "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": ""}},
                "LocationHash": "test",
            },
            "negative_line_number": {
                "Location": {
                    "CodeLocation": {
                        "Language": "python",
                        "CodeUnit": "myapp",
                        "MethodName": "func",
                        "LineNumber": -1,
                    }
                },
                "LocationHash": "test",
            },
        }

        for case_name, api_config in test_cases.items():
            with self.subTest(case=case_name):
                config = BreakpointConfiguration.from_api_config(api_config)
                self.assertIsNone(config)

    def test_from_api_config_unexpected_exception(self):
        """Test that unexpected exceptions are caught and return None."""
        with patch("amazon.opentelemetry.distro.debugger._data_models.CaptureConfig") as mock_capture:
            mock_capture.side_effect = RuntimeError("Unexpected error")
            api_config = {
                "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func"}},
                "LocationHash": "test",
            }
            config = BreakpointConfiguration.from_api_config(api_config)
            self.assertIsNone(config)

    def test_from_api_config_default_values(self):
        """Test that default values are used when config fields are missing or invalid."""
        test_cases = {
            "missing_capture_config": {
                "api_config": {
                    "Location": {
                        "CodeLocation": {
                            "Language": "python",
                            "CodeUnit": "myapp",
                            "MethodName": "func",
                            "LineNumber": 10,
                        }
                    },
                    "LocationHash": "test",
                },
                "expected_defaults": {
                    "capture_arguments": None,
                    "capture_return": False,
                    "max_string_length": DEFAULT_MAX_STRING_LENGTH,
                    "return_attribute_name": DEFAULT_RETURN_ATTRIBUTE_NAME,
                },
            },
            "invalid_capture_config_type": {
                "api_config": {
                    "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func"}},
                    "CaptureConfiguration": "not_a_dict",
                    "LocationHash": "test",
                },
                "expected_defaults": {
                    "capture_arguments": None,
                    "capture_return": False,
                    "max_string_length": DEFAULT_MAX_STRING_LENGTH,
                },
            },
            "invalid_capture_limits_type": {
                "api_config": {
                    "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func"}},
                    "CaptureConfiguration": {"CodeCapture": {"CaptureLimits": "not_a_dict"}},
                    "LocationHash": "test",
                },
                "expected_defaults": {
                    "max_string_length": DEFAULT_MAX_STRING_LENGTH,
                    "max_collection_width": DEFAULT_MAX_COLLECTION_WIDTH,
                },
            },
            "invalid_field_types": {
                "api_config": {
                    "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func"}},
                    "CaptureConfiguration": {
                        "CodeCapture": {
                            "CaptureReturn": "not_a_bool",
                            "CaptureArguments": "not_a_list",
                            "CaptureLimits": {"MaxStringLength": "not_an_int"},
                        }
                    },
                    "LocationHash": "test",
                },
                "expected_defaults": {
                    "capture_return": False,
                    "capture_arguments": [],  # invalid type present -> [] (capture all)
                    "max_string_length": DEFAULT_MAX_STRING_LENGTH,
                },
            },
            "invalid_attribute_filters": {
                "api_config": {
                    "Location": {"CodeLocation": {"Language": "python", "CodeUnit": "myapp", "MethodName": "func"}},
                    "AttributeFilters": "not_a_list",
                    "LocationHash": "test",
                },
                "expected_defaults": {
                    "attribute_filters": [],
                },
            },
        }

        for case_name, test_data in test_cases.items():
            with self.subTest(case=case_name):
                config = BreakpointConfiguration.from_api_config(test_data["api_config"])
                self.assertIsNotNone(config)

                for field_name, expected_value in test_data["expected_defaults"].items():
                    if field_name == "attribute_filters":
                        actual_value = getattr(config, field_name)
                    else:
                        actual_value = getattr(config.capture_config, field_name)
                    self.assertEqual(
                        actual_value,
                        expected_value,
                        f"Field {field_name} should be {expected_value}, got {actual_value}",
                    )


class TestBreakpointState(unittest.TestCase):
    """Tests for BreakpointState dataclass."""

    def test_initial_state(self):
        """Test initial breakpoint state."""
        state = BreakpointState(breakpoint_key="myapp.func:10")

        self.assertEqual(state.breakpoint_key, "myapp.func:10")
        self.assertEqual(state.hit_count, 0)
        self.assertFalse(state.is_disabled)

    def test_state_with_hits(self):
        """Test breakpoint state with hits."""
        state = BreakpointState(breakpoint_key="myapp.func:10", hit_count=42)

        self.assertEqual(state.hit_count, 42)
        self.assertFalse(state.is_disabled)

    def test_disabled_state(self):
        """Test disabled breakpoint state."""
        state = BreakpointState(breakpoint_key="myapp.func:10", hit_count=100, is_disabled=True)

        self.assertEqual(state.hit_count, 100)
        self.assertTrue(state.is_disabled)


class TestFunctionBreakpointSet(unittest.TestCase):
    """Tests for FunctionBreakpointSet dataclass."""

    def test_empty_set(self):
        """Test empty breakpoint set."""
        bp_set = FunctionBreakpointSet(function_key="myapp.func", module="myapp", function_name="func")

        self.assertEqual(bp_set.function_key, "myapp.func")
        self.assertEqual(bp_set.line_numbers, set())
        self.assertFalse(bp_set.needs_wrapper)
        self.assertIsNone(bp_set.capture_config)
        self.assertFalse(bp_set.is_instrumented)

    def test_single_line_breakpoint(self):
        """Test set with single line breakpoint."""
        config = BreakpointConfiguration(
            module="myapp",
            function_name="func",
            line_number=10,
            capture_config=CaptureConfig(capture_arguments=["arg1"]),
            config_id="test-id",
        )

        bp_set = FunctionBreakpointSet(
            function_key="myapp.func", module="myapp", function_name="func", breakpoints={10: config}
        )

        self.assertEqual(bp_set.line_numbers, {10})
        self.assertTrue(bp_set.needs_wrapper)
        self.assertIsNotNone(bp_set.capture_config)
        self.assertEqual(bp_set.capture_config.capture_arguments, ["arg1"])

    def test_multiple_line_breakpoints(self):
        """Test set with multiple line breakpoints."""
        config10 = BreakpointConfiguration(
            module="myapp", function_name="func", line_number=10, capture_config=CaptureConfig(), config_id="test-id-10"
        )
        config20 = BreakpointConfiguration(
            module="myapp", function_name="func", line_number=20, capture_config=CaptureConfig(), config_id="test-id-20"
        )
        config30 = BreakpointConfiguration(
            module="myapp", function_name="func", line_number=30, capture_config=CaptureConfig(), config_id="test-id-30"
        )

        bp_set = FunctionBreakpointSet(
            function_key="myapp.func",
            module="myapp",
            function_name="func",
            breakpoints={10: config10, 20: config20, 30: config30},
        )

        self.assertEqual(bp_set.line_numbers, {10, 20, 30})
        self.assertTrue(bp_set.needs_wrapper)

    def test_with_states(self):
        """Test set with breakpoint states."""
        config = BreakpointConfiguration(
            module="myapp", function_name="func", line_number=10, capture_config=CaptureConfig(), config_id="test-id"
        )

        state = BreakpointState(breakpoint_key="myapp.func:10", hit_count=5)

        bp_set = FunctionBreakpointSet(
            function_key="myapp.func",
            module="myapp",
            function_name="func",
            breakpoints={10: config},
            states={"myapp.func:10": state},
        )

        self.assertEqual(len(bp_set.states), 1)
        self.assertEqual(bp_set.states["myapp.func:10"].hit_count, 5)


if __name__ == "__main__":
    unittest.main()
