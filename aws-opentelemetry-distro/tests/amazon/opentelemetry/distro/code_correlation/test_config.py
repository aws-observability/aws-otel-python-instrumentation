# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.code_correlation.config import _ENV_CONFIG, AwsCodeAttributesConfig


class TestAwsCodeAttributesConfig(TestCase):
    """Test the AwsCodeAttributesConfig class."""

    def test_init_with_defaults(self):
        """Test initialization with default parameters."""
        config = AwsCodeAttributesConfig()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_init_with_none_parameters(self):
        """Test initialization with None parameters."""
        config = AwsCodeAttributesConfig(include=None, exclude=None, stack_depth=0)

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_init_with_custom_parameters(self):
        """Test initialization with custom parameters."""
        include = ["myapp", "mylib"]
        exclude = ["third-party", "vendor"]
        stack_depth = 10

        config = AwsCodeAttributesConfig(include=include, exclude=exclude, stack_depth=stack_depth)

        self.assertEqual(config.include, include)
        self.assertEqual(config.exclude, exclude)
        self.assertEqual(config.stack_depth, stack_depth)

    def test_init_with_empty_lists(self):
        """Test initialization with empty lists."""
        config = AwsCodeAttributesConfig(include=[], exclude=[], stack_depth=5)

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 5)

    def test_to_dict(self):
        """Test conversion to dictionary."""
        config = AwsCodeAttributesConfig(include=["app1", "app2"], exclude=["lib1", "lib2"], stack_depth=15)

        result = config.to_dict()

        expected = {"include": ["app1", "app2"], "exclude": ["lib1", "lib2"], "stack_depth": 15}
        self.assertEqual(result, expected)

    def test_to_dict_with_defaults(self):
        """Test conversion to dictionary with default values."""
        config = AwsCodeAttributesConfig()

        result = config.to_dict()

        expected = {"include": [], "exclude": [], "stack_depth": 0}
        self.assertEqual(result, expected)

    def test_to_json_with_indent(self):
        """Test conversion to JSON with indentation."""
        config = AwsCodeAttributesConfig(include=["myapp"], exclude=["vendor"], stack_depth=5)

        result = config.to_json(indent=2)

        expected_dict = {"include": ["myapp"], "exclude": ["vendor"], "stack_depth": 5}
        expected_json = json.dumps(expected_dict, indent=2)
        self.assertEqual(result, expected_json)

    def test_to_json_without_indent(self):
        """Test conversion to JSON without indentation."""
        config = AwsCodeAttributesConfig(include=["myapp"], exclude=["vendor"], stack_depth=5)

        result = config.to_json(indent=None)

        expected_dict = {"include": ["myapp"], "exclude": ["vendor"], "stack_depth": 5}
        expected_json = json.dumps(expected_dict, indent=None)
        self.assertEqual(result, expected_json)

    def test_to_json_default_indent(self):
        """Test conversion to JSON with default indentation."""
        config = AwsCodeAttributesConfig(include=["myapp"], exclude=["vendor"], stack_depth=5)

        result = config.to_json()

        expected_dict = {"include": ["myapp"], "exclude": ["vendor"], "stack_depth": 5}
        expected_json = json.dumps(expected_dict, indent=2)
        self.assertEqual(result, expected_json)

    def test_repr(self):
        """Test string representation."""
        config = AwsCodeAttributesConfig(include=["app1", "app2"], exclude=["lib1"], stack_depth=10)

        result = repr(config)

        expected = "AwsCodeAttributesConfig(include=['app1', 'app2'], exclude=['lib1'], stack_depth=10)"
        self.assertEqual(result, expected)

    def test_repr_with_defaults(self):
        """Test string representation with default values."""
        config = AwsCodeAttributesConfig()

        result = repr(config)

        expected = "AwsCodeAttributesConfig(include=[], exclude=[], stack_depth=0)"
        self.assertEqual(result, expected)


class TestAwsCodeAttributesConfigFromEnv(TestCase):  # pylint: disable=too-many-public-methods
    """Test the from_env class method."""

    def setUp(self):
        """Set up test fixtures by clearing environment variable."""
        self.env_patcher = patch.dict(os.environ, {}, clear=False)
        self.env_patcher.start()
        if _ENV_CONFIG in os.environ:
            del os.environ[_ENV_CONFIG]

    def tearDown(self):
        """Clean up test fixtures."""
        self.env_patcher.stop()

    def test_from_env_no_environment_variable(self):
        """Test from_env when environment variable is not set."""
        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_from_env_empty_environment_variable(self):
        """Test from_env when environment variable is empty."""
        os.environ[_ENV_CONFIG] = ""

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_from_env_whitespace_only_environment_variable(self):
        """Test from_env when environment variable contains only whitespace."""
        os.environ[_ENV_CONFIG] = "   \t\n  "

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_from_env_empty_json_object(self):
        """Test from_env with empty JSON object."""
        os.environ[_ENV_CONFIG] = "{}"

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_from_env_complete_configuration(self):
        """Test from_env with complete configuration."""
        config_data = {"include": ["myapp", "mylib"], "exclude": ["third-party", "vendor"], "stack_depth": 15}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, ["myapp", "mylib"])
        self.assertEqual(config.exclude, ["third-party", "vendor"])
        self.assertEqual(config.stack_depth, 15)

    def test_from_env_partial_configuration(self):
        """Test from_env with partial configuration."""
        config_data = {"include": ["myapp"]}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, ["myapp"])
        self.assertEqual(config.exclude, [])  # Default value
        self.assertEqual(config.stack_depth, 0)  # Default value

    def test_from_env_only_exclude(self):
        """Test from_env with only exclude configuration."""
        config_data = {"exclude": ["vendor", "third-party"]}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, [])  # Default value
        self.assertEqual(config.exclude, ["vendor", "third-party"])
        self.assertEqual(config.stack_depth, 0)  # Default value

    def test_from_env_only_stack_depth(self):
        """Test from_env with only stack_depth configuration."""
        config_data = {"stack_depth": 25}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, [])  # Default value
        self.assertEqual(config.exclude, [])  # Default value
        self.assertEqual(config.stack_depth, 25)

    def test_from_env_zero_stack_depth(self):
        """Test from_env with zero stack_depth (unlimited)."""
        config_data = {"stack_depth": 0}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.stack_depth, 0)

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_negative_stack_depth(self, mock_logger):
        """Test from_env with negative stack_depth."""
        config_data = {"stack_depth": -5}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        # Negative stack_depth should be corrected to 0
        self.assertEqual(config.stack_depth, 0)

        # Should log a warning
        mock_logger.warning.assert_called_once()
        args = mock_logger.warning.call_args[0]
        self.assertIn("'stack_depth'", args[0])
        self.assertIn("must be non-negative", args[0])
        self.assertEqual(args[1], _ENV_CONFIG)
        self.assertEqual(args[2], -5)

    def test_from_env_empty_include_list(self):
        """Test from_env with explicitly empty include list."""
        config_data = {"include": [], "exclude": ["vendor"], "stack_depth": 5}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, ["vendor"])
        self.assertEqual(config.stack_depth, 5)

    def test_from_env_empty_exclude_list(self):
        """Test from_env with explicitly empty exclude list."""
        config_data = {"include": ["myapp"], "exclude": [], "stack_depth": 5}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, ["myapp"])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 5)

    def test_from_env_single_item_lists(self):
        """Test from_env with single-item lists."""
        config_data = {"include": ["single_app"], "exclude": ["single_vendor"]}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, ["single_app"])
        self.assertEqual(config.exclude, ["single_vendor"])

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_invalid_json(self, mock_logger):
        """Test from_env with invalid JSON."""
        os.environ[_ENV_CONFIG] = "invalid json {"

        config = AwsCodeAttributesConfig.from_env()

        # Should use default values when JSON is invalid
        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

        # Should log a warning
        mock_logger.warning.assert_called_once()
        args = mock_logger.warning.call_args[0]
        self.assertIn("Invalid JSON", args[0])
        self.assertEqual(args[1], _ENV_CONFIG)

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_malformed_json_syntax_error(self, mock_logger):
        """Test from_env with malformed JSON that causes syntax error."""
        os.environ[_ENV_CONFIG] = '{"include": ["app1", "app2"'  # Missing closing bracket and brace

        config = AwsCodeAttributesConfig.from_env()

        # Should use default values
        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

        # Should log a warning
        mock_logger.warning.assert_called_once()

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_non_object_json(self, mock_logger):
        """Test from_env with valid JSON that's not an object."""
        os.environ[_ENV_CONFIG] = '["not", "an", "object"]'

        config = AwsCodeAttributesConfig.from_env()

        # Should handle gracefully and use defaults
        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

        # Should log a warning about non-object JSON
        mock_logger.warning.assert_called_once()
        args = mock_logger.warning.call_args[0]
        self.assertIn("Configuration in", args[0])
        self.assertIn("must be a JSON object", args[0])
        self.assertEqual(args[1], _ENV_CONFIG)
        self.assertEqual(args[2], "list")

    def test_from_env_extra_fields_ignored(self):
        """Test from_env ignores extra fields in JSON."""
        config_data = {
            "include": ["myapp"],
            "exclude": ["vendor"],
            "stack_depth": 10,
            "extra_field": "ignored",
            "another_extra": 42,
        }
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.include, ["myapp"])
        self.assertEqual(config.exclude, ["vendor"])
        self.assertEqual(config.stack_depth, 10)
        # Extra fields should not affect the configuration

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_wrong_type_values(self, mock_logger):
        """Test from_env with wrong type values."""
        config_data = {
            "include": "not_a_list",  # Should be a list
            "exclude": 42,  # Should be a list
            "stack_depth": "not_a_number",  # Should be a number
        }
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        # Should validate and use defaults for invalid types
        self.assertEqual(config.include, [])  # Corrected to empty list
        self.assertEqual(config.exclude, [])  # Corrected to empty list
        self.assertEqual(config.stack_depth, 0)  # Corrected to default value

        # Should log warnings for all invalid types
        self.assertEqual(mock_logger.warning.call_count, 3)
        warning_calls = [call[0] for call in mock_logger.warning.call_args_list]

        # Check for include warning - format string and arguments
        include_warnings = [call for call in warning_calls if "must be a list" in call[0] and call[1] == "include"]
        self.assertEqual(len(include_warnings), 1)
        self.assertEqual(include_warnings[0][1], "include")
        self.assertEqual(include_warnings[0][2], _ENV_CONFIG)
        self.assertEqual(include_warnings[0][3], "str")

        # Check for exclude warning
        exclude_warnings = [call for call in warning_calls if "must be a list" in call[0] and call[1] == "exclude"]
        self.assertEqual(len(exclude_warnings), 1)
        self.assertEqual(exclude_warnings[0][1], "exclude")
        self.assertEqual(exclude_warnings[0][2], _ENV_CONFIG)
        self.assertEqual(exclude_warnings[0][3], "int")

        # Check for stack_depth warning
        stack_warnings = [
            call for call in warning_calls if "'stack_depth'" in call[0] and "must be an integer" in call[0]
        ]
        self.assertEqual(len(stack_warnings), 1)
        self.assertEqual(stack_warnings[0][1], _ENV_CONFIG)
        self.assertEqual(stack_warnings[0][2], "str")

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_null_values(self, mock_logger):
        """Test from_env with null values in JSON."""
        config_data = {"include": None, "exclude": None, "stack_depth": None}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        # get() should return None for null values, and validation should handle it
        self.assertEqual(config.include, [])  # Constructor converts None to []
        self.assertEqual(config.exclude, [])  # Constructor converts None to []
        self.assertEqual(config.stack_depth, 0)  # None should be corrected to default value

        # Should log warnings for invalid types
        self.assertEqual(mock_logger.warning.call_count, 3)
        warning_calls = [call[0] for call in mock_logger.warning.call_args_list]

        # Check for include warning
        include_warnings = [call for call in warning_calls if "must be a list" in call[0] and call[1] == "include"]
        self.assertEqual(len(include_warnings), 1)
        self.assertEqual(include_warnings[0][1], "include")
        self.assertEqual(include_warnings[0][2], _ENV_CONFIG)
        self.assertEqual(include_warnings[0][3], "NoneType")

        # Check for exclude warning
        exclude_warnings = [call for call in warning_calls if "must be a list" in call[0] and call[1] == "exclude"]
        self.assertEqual(len(exclude_warnings), 1)
        self.assertEqual(exclude_warnings[0][1], "exclude")
        self.assertEqual(exclude_warnings[0][2], _ENV_CONFIG)
        self.assertEqual(exclude_warnings[0][3], "NoneType")

        # Check for stack_depth warning
        stack_warnings = [
            call for call in warning_calls if "'stack_depth'" in call[0] and "must be an integer" in call[0]
        ]
        self.assertEqual(len(stack_warnings), 1)
        self.assertEqual(stack_warnings[0][1], _ENV_CONFIG)
        self.assertEqual(stack_warnings[0][2], "NoneType")

    def test_from_env_complex_package_names(self):
        """Test from_env with complex package names."""
        config_data = {
            "include": ["my.app.module", "com.company.service", "app_with_underscores", "app-with-dashes"],
            "exclude": ["third.party.lib", "vendor.package.name", "test_framework"],
        }
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(
            config.include, ["my.app.module", "com.company.service", "app_with_underscores", "app-with-dashes"]
        )
        self.assertEqual(config.exclude, ["third.party.lib", "vendor.package.name", "test_framework"])

    def test_from_env_large_stack_depth(self):
        """Test from_env with large stack depth value."""
        config_data = {"stack_depth": 999999}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        self.assertEqual(config.stack_depth, 999999)

    def test_env_constant_value(self):
        """Test that the environment variable constant has the expected value."""
        self.assertEqual(_ENV_CONFIG, "OTEL_AWS_CODE_ATTRIBUTES_CONFIG")

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_mixed_type_include_list(self, mock_logger):
        """Test from_env with include list containing mixed types."""
        config_data = {
            "include": ["valid_string", 123, True, None, "another_valid_string"],
            "exclude": [],
            "stack_depth": 5,
        }
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        # Should only keep string items
        self.assertEqual(config.include, ["valid_string", "another_valid_string"])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 5)

        # Should log warnings for non-string items
        self.assertEqual(mock_logger.warning.call_count, 3)  # Exactly 3 non-string items
        warning_calls = [call[0] for call in mock_logger.warning.call_args_list]

        # Check that warnings mention non-string items being skipped
        include_warnings = [
            call
            for call in warning_calls
            if "list item" in call[0] and "must be a string" in call[0] and call[1] == "include"
        ]
        self.assertEqual(len(include_warnings), 3)  # 123, True, None

        # Verify the specific types logged
        logged_types = [call[3] for call in include_warnings]
        self.assertIn("int", logged_types)  # 123
        self.assertIn("bool", logged_types)  # True
        self.assertIn("NoneType", logged_types)  # None

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_mixed_type_exclude_list(self, mock_logger):
        """Test from_env with exclude list containing mixed types."""
        config_data = {
            "include": [],
            "exclude": ["valid_exclude", 42, False, "another_valid_exclude", [1, 2, 3]],
            "stack_depth": 10,
        }
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        # Should only keep string items
        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, ["valid_exclude", "another_valid_exclude"])
        self.assertEqual(config.stack_depth, 10)

        # Should log warnings for non-string items
        self.assertEqual(mock_logger.warning.call_count, 3)  # Exactly 3 non-string items
        warning_calls = [call[0] for call in mock_logger.warning.call_args_list]

        # Check that warnings mention non-string items being skipped
        exclude_warnings = [
            call
            for call in warning_calls
            if "list item" in call[0] and "must be a string" in call[0] and call[1] == "exclude"
        ]
        self.assertEqual(len(exclude_warnings), 3)  # 42, False, [1, 2, 3]

        # Verify the specific types logged
        logged_types = [call[3] for call in exclude_warnings]
        self.assertIn("int", logged_types)  # 42
        self.assertIn("bool", logged_types)  # False
        self.assertIn("list", logged_types)  # [1, 2, 3]

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_all_non_string_list_items(self, mock_logger):
        """Test from_env with lists containing only non-string types."""
        config_data = {
            "include": [123, True, None, {"key": "value"}, [1, 2, 3]],
            "exclude": [456, False, 0, 1.5, {"another": "object"}],
            "stack_depth": 5,
        }
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        # Should result in empty lists since no valid strings
        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 5)

        # Should log warnings for all non-string items
        self.assertEqual(mock_logger.warning.call_count, 10)  # Exactly 10 non-string items
        warning_calls = [call[0] for call in mock_logger.warning.call_args_list]

        # Check include warnings
        include_warnings = [
            call
            for call in warning_calls
            if "list item" in call[0] and "must be a string" in call[0] and call[1] == "include"
        ]
        self.assertEqual(len(include_warnings), 5)

        # Check exclude warnings
        exclude_warnings = [
            call
            for call in warning_calls
            if "list item" in call[0] and "must be a string" in call[0] and call[1] == "exclude"
        ]
        self.assertEqual(len(exclude_warnings), 5)

        # Verify that different types are logged
        include_logged_types = [call[3] for call in include_warnings]
        exclude_logged_types = [call[3] for call in exclude_warnings]

        # For include: [123, True, None, {"key": "value"}, [1, 2, 3]]
        self.assertIn("int", include_logged_types)  # 123
        self.assertIn("bool", include_logged_types)  # True
        self.assertIn("NoneType", include_logged_types)  # None
        self.assertIn("dict", include_logged_types)  # {"key": "value"}
        self.assertIn("list", include_logged_types)  # [1, 2, 3]

        # For exclude: [456, False, 0, 1.5, {"another": "object"}]
        self.assertIn("int", exclude_logged_types)  # 456 and 0
        self.assertIn("bool", exclude_logged_types)  # False
        self.assertIn("float", exclude_logged_types)  # 1.5
        self.assertIn("dict", exclude_logged_types)  # {"another": "object"}

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_float_stack_depth(self, mock_logger):
        """Test from_env with float stack_depth."""
        config_data = {"include": ["myapp"], "exclude": ["vendor"], "stack_depth": 5.7}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        # Float should be treated as invalid and corrected to 0
        self.assertEqual(config.include, ["myapp"])
        self.assertEqual(config.exclude, ["vendor"])
        self.assertEqual(config.stack_depth, 0)

        # Should log warning for invalid stack_depth type
        mock_logger.warning.assert_called_once()
        args = mock_logger.warning.call_args[0]
        self.assertIn("'stack_depth'", args[0])
        self.assertIn("must be an integer", args[0])
        self.assertEqual(args[1], _ENV_CONFIG)
        self.assertEqual(args[2], "float")

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_empty_string_in_lists(self, mock_logger):
        """Test from_env with empty strings in lists."""
        config_data = {
            "include": ["valid", "", "also_valid", ""],
            "exclude": ["", "valid_exclude", ""],
            "stack_depth": 5,
        }
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeAttributesConfig.from_env()

        # Empty strings are still strings, so they should be preserved
        self.assertEqual(config.include, ["valid", "", "also_valid", ""])
        self.assertEqual(config.exclude, ["", "valid_exclude", ""])
        self.assertEqual(config.stack_depth, 5)

        # Should not log warnings since empty strings are valid strings
        mock_logger.warning.assert_not_called()


class TestAwsCodeAttributesConfigIntegration(TestCase):
    """Integration tests for AwsCodeAttributesConfig."""

    def setUp(self):
        """Set up test fixtures."""
        self.env_patcher = patch.dict(os.environ, {}, clear=False)
        self.env_patcher.start()
        if _ENV_CONFIG in os.environ:
            del os.environ[_ENV_CONFIG]

    def tearDown(self):
        """Clean up test fixtures."""
        self.env_patcher.stop()

    def test_roundtrip_to_dict_from_env(self):
        """Test roundtrip: config -> to_dict -> env -> from_env -> config."""
        original_config = AwsCodeAttributesConfig(
            include=["app1", "app2"], exclude=["vendor1", "vendor2"], stack_depth=20
        )

        # Convert to dict and then to JSON for environment
        config_dict = original_config.to_dict()
        os.environ[_ENV_CONFIG] = json.dumps(config_dict)

        # Create new config from environment
        new_config = AwsCodeAttributesConfig.from_env()

        # Should be equivalent
        self.assertEqual(new_config.include, original_config.include)
        self.assertEqual(new_config.exclude, original_config.exclude)
        self.assertEqual(new_config.stack_depth, original_config.stack_depth)
        self.assertEqual(new_config.to_dict(), original_config.to_dict())

    def test_roundtrip_to_json_from_env(self):
        """Test roundtrip: config -> to_json -> env -> from_env -> config."""
        original_config = AwsCodeAttributesConfig(include=["myapp"], exclude=["third-party"], stack_depth=5)

        # Convert to JSON for environment
        config_json = original_config.to_json(indent=None)  # Compact JSON
        os.environ[_ENV_CONFIG] = config_json

        # Create new config from environment
        new_config = AwsCodeAttributesConfig.from_env()

        # Should be equivalent
        self.assertEqual(new_config.include, original_config.include)
        self.assertEqual(new_config.exclude, original_config.exclude)
        self.assertEqual(new_config.stack_depth, original_config.stack_depth)
        self.assertEqual(new_config.to_json(indent=None), original_config.to_json(indent=None))

    def test_config_equality_comparison(self):
        """Test that configs with same values produce same representations."""
        config1 = AwsCodeAttributesConfig(include=["app"], exclude=["vendor"], stack_depth=10)

        config2 = AwsCodeAttributesConfig(include=["app"], exclude=["vendor"], stack_depth=10)

        # They should have the same string representation
        self.assertEqual(repr(config1), repr(config2))
        self.assertEqual(config1.to_dict(), config2.to_dict())
        self.assertEqual(config1.to_json(), config2.to_json())
