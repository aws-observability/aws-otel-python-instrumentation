# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.code_correlation.config import _ENV_CONFIG, AwsCodeCorrelationConfig


class TestAwsCodeCorrelationConfig(TestCase):
    """Test the AwsCodeCorrelationConfig class."""

    def test_init_with_defaults(self):
        """Test initialization with default parameters."""
        config = AwsCodeCorrelationConfig()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_init_with_none_parameters(self):
        """Test initialization with None parameters."""
        config = AwsCodeCorrelationConfig(include=None, exclude=None, stack_depth=0)

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_init_with_custom_parameters(self):
        """Test initialization with custom parameters."""
        include = ["myapp", "mylib"]
        exclude = ["thirdparty", "vendor"]
        stack_depth = 10

        config = AwsCodeCorrelationConfig(include=include, exclude=exclude, stack_depth=stack_depth)

        self.assertEqual(config.include, include)
        self.assertEqual(config.exclude, exclude)
        self.assertEqual(config.stack_depth, stack_depth)

    def test_init_with_empty_lists(self):
        """Test initialization with empty lists."""
        config = AwsCodeCorrelationConfig(include=[], exclude=[], stack_depth=5)

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 5)

    def test_to_dict(self):
        """Test conversion to dictionary."""
        config = AwsCodeCorrelationConfig(include=["app1", "app2"], exclude=["lib1", "lib2"], stack_depth=15)

        result = config.to_dict()

        expected = {"include": ["app1", "app2"], "exclude": ["lib1", "lib2"], "stack_depth": 15}
        self.assertEqual(result, expected)

    def test_to_dict_with_defaults(self):
        """Test conversion to dictionary with default values."""
        config = AwsCodeCorrelationConfig()

        result = config.to_dict()

        expected = {"include": [], "exclude": [], "stack_depth": 0}
        self.assertEqual(result, expected)

    def test_to_json_with_indent(self):
        """Test conversion to JSON with indentation."""
        config = AwsCodeCorrelationConfig(include=["myapp"], exclude=["vendor"], stack_depth=5)

        result = config.to_json(indent=2)

        expected_dict = {"include": ["myapp"], "exclude": ["vendor"], "stack_depth": 5}
        expected_json = json.dumps(expected_dict, indent=2)
        self.assertEqual(result, expected_json)

    def test_to_json_without_indent(self):
        """Test conversion to JSON without indentation."""
        config = AwsCodeCorrelationConfig(include=["myapp"], exclude=["vendor"], stack_depth=5)

        result = config.to_json(indent=None)

        expected_dict = {"include": ["myapp"], "exclude": ["vendor"], "stack_depth": 5}
        expected_json = json.dumps(expected_dict, indent=None)
        self.assertEqual(result, expected_json)

    def test_to_json_default_indent(self):
        """Test conversion to JSON with default indentation."""
        config = AwsCodeCorrelationConfig(include=["myapp"], exclude=["vendor"], stack_depth=5)

        result = config.to_json()

        expected_dict = {"include": ["myapp"], "exclude": ["vendor"], "stack_depth": 5}
        expected_json = json.dumps(expected_dict, indent=2)
        self.assertEqual(result, expected_json)

    def test_repr(self):
        """Test string representation."""
        config = AwsCodeCorrelationConfig(include=["app1", "app2"], exclude=["lib1"], stack_depth=10)

        result = repr(config)

        expected = "AwsCodeCorrelationConfig(" "include=['app1', 'app2'], " "exclude=['lib1'], " "stack_depth=10)"
        self.assertEqual(result, expected)

    def test_repr_with_defaults(self):
        """Test string representation with default values."""
        config = AwsCodeCorrelationConfig()

        result = repr(config)

        expected = "AwsCodeCorrelationConfig(" "include=[], " "exclude=[], " "stack_depth=0)"
        self.assertEqual(result, expected)


class TestAwsCodeCorrelationConfigFromEnv(TestCase):
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
        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_from_env_empty_environment_variable(self):
        """Test from_env when environment variable is empty."""
        os.environ[_ENV_CONFIG] = ""

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_from_env_whitespace_only_environment_variable(self):
        """Test from_env when environment variable contains only whitespace."""
        os.environ[_ENV_CONFIG] = "   \t\n  "

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_from_env_empty_json_object(self):
        """Test from_env with empty JSON object."""
        os.environ[_ENV_CONFIG] = "{}"

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 0)

    def test_from_env_complete_configuration(self):
        """Test from_env with complete configuration."""
        config_data = {"include": ["myapp", "mylib"], "exclude": ["thirdparty", "vendor"], "stack_depth": 15}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, ["myapp", "mylib"])
        self.assertEqual(config.exclude, ["thirdparty", "vendor"])
        self.assertEqual(config.stack_depth, 15)

    def test_from_env_partial_configuration(self):
        """Test from_env with partial configuration."""
        config_data = {"include": ["myapp"]}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, ["myapp"])
        self.assertEqual(config.exclude, [])  # Default value
        self.assertEqual(config.stack_depth, 0)  # Default value

    def test_from_env_only_exclude(self):
        """Test from_env with only exclude configuration."""
        config_data = {"exclude": ["vendor", "thirdparty"]}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, [])  # Default value
        self.assertEqual(config.exclude, ["vendor", "thirdparty"])
        self.assertEqual(config.stack_depth, 0)  # Default value

    def test_from_env_only_stack_depth(self):
        """Test from_env with only stack_depth configuration."""
        config_data = {"stack_depth": 25}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, [])  # Default value
        self.assertEqual(config.exclude, [])  # Default value
        self.assertEqual(config.stack_depth, 25)

    def test_from_env_zero_stack_depth(self):
        """Test from_env with zero stack_depth (unlimited)."""
        config_data = {"stack_depth": 0}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.stack_depth, 0)

    def test_from_env_negative_stack_depth(self):
        """Test from_env with negative stack_depth."""
        config_data = {"stack_depth": -5}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.stack_depth, -5)

    def test_from_env_empty_include_list(self):
        """Test from_env with explicitly empty include list."""
        config_data = {"include": [], "exclude": ["vendor"], "stack_depth": 5}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, [])
        self.assertEqual(config.exclude, ["vendor"])
        self.assertEqual(config.stack_depth, 5)

    def test_from_env_empty_exclude_list(self):
        """Test from_env with explicitly empty exclude list."""
        config_data = {"include": ["myapp"], "exclude": [], "stack_depth": 5}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, ["myapp"])
        self.assertEqual(config.exclude, [])
        self.assertEqual(config.stack_depth, 5)

    def test_from_env_single_item_lists(self):
        """Test from_env with single-item lists."""
        config_data = {"include": ["single_app"], "exclude": ["single_vendor"]}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, ["single_app"])
        self.assertEqual(config.exclude, ["single_vendor"])

    @patch("amazon.opentelemetry.distro.code_correlation.config._logger")
    def test_from_env_invalid_json(self, mock_logger):
        """Test from_env with invalid JSON."""
        os.environ[_ENV_CONFIG] = "invalid json {"

        config = AwsCodeCorrelationConfig.from_env()

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

        config = AwsCodeCorrelationConfig.from_env()

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

        config = AwsCodeCorrelationConfig.from_env()

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

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.include, ["myapp"])
        self.assertEqual(config.exclude, ["vendor"])
        self.assertEqual(config.stack_depth, 10)
        # Extra fields should not affect the configuration

    def test_from_env_wrong_type_values(self):
        """Test from_env with wrong type values."""
        config_data = {
            "include": "not_a_list",  # Should be a list
            "exclude": 42,  # Should be a list
            "stack_depth": "not_a_number",  # Should be a number
        }
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        # Should use the provided values even if they're wrong types
        # This tests that the code doesn't crash on type mismatches
        self.assertEqual(config.include, "not_a_list")
        self.assertEqual(config.exclude, 42)
        self.assertEqual(config.stack_depth, "not_a_number")

    def test_from_env_null_values(self):
        """Test from_env with null values in JSON."""
        config_data = {"include": None, "exclude": None, "stack_depth": None}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        # get() should return None for null values, and constructor should handle it
        self.assertEqual(config.include, [])  # Constructor converts None to []
        self.assertEqual(config.exclude, [])  # Constructor converts None to []
        self.assertEqual(config.stack_depth, None)  # None is passed through for stack_depth

    def test_from_env_complex_package_names(self):
        """Test from_env with complex package names."""
        config_data = {
            "include": ["my.app.module", "com.company.service", "app_with_underscores", "app-with-dashes"],
            "exclude": ["third.party.lib", "vendor.package.name", "test_framework"],
        }
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(
            config.include, ["my.app.module", "com.company.service", "app_with_underscores", "app-with-dashes"]
        )
        self.assertEqual(config.exclude, ["third.party.lib", "vendor.package.name", "test_framework"])

    def test_from_env_large_stack_depth(self):
        """Test from_env with large stack depth value."""
        config_data = {"stack_depth": 999999}
        os.environ[_ENV_CONFIG] = json.dumps(config_data)

        config = AwsCodeCorrelationConfig.from_env()

        self.assertEqual(config.stack_depth, 999999)

    def test_env_constant_value(self):
        """Test that the environment variable constant has the expected value."""
        self.assertEqual(_ENV_CONFIG, "OTEL_AWS_CODE_CORRELATION_CONFIG")


class TestAwsCodeCorrelationConfigIntegration(TestCase):
    """Integration tests for AwsCodeCorrelationConfig."""

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
        original_config = AwsCodeCorrelationConfig(
            include=["app1", "app2"], exclude=["vendor1", "vendor2"], stack_depth=20
        )

        # Convert to dict and then to JSON for environment
        config_dict = original_config.to_dict()
        os.environ[_ENV_CONFIG] = json.dumps(config_dict)

        # Create new config from environment
        new_config = AwsCodeCorrelationConfig.from_env()

        # Should be equivalent
        self.assertEqual(new_config.include, original_config.include)
        self.assertEqual(new_config.exclude, original_config.exclude)
        self.assertEqual(new_config.stack_depth, original_config.stack_depth)
        self.assertEqual(new_config.to_dict(), original_config.to_dict())

    def test_roundtrip_to_json_from_env(self):
        """Test roundtrip: config -> to_json -> env -> from_env -> config."""
        original_config = AwsCodeCorrelationConfig(include=["myapp"], exclude=["thirdparty"], stack_depth=5)

        # Convert to JSON for environment
        config_json = original_config.to_json(indent=None)  # Compact JSON
        os.environ[_ENV_CONFIG] = config_json

        # Create new config from environment
        new_config = AwsCodeCorrelationConfig.from_env()

        # Should be equivalent
        self.assertEqual(new_config.include, original_config.include)
        self.assertEqual(new_config.exclude, original_config.exclude)
        self.assertEqual(new_config.stack_depth, original_config.stack_depth)
        self.assertEqual(new_config.to_json(indent=None), original_config.to_json(indent=None))

    def test_config_equality_comparison(self):
        """Test that configs with same values produce same representations."""
        config1 = AwsCodeCorrelationConfig(include=["app"], exclude=["vendor"], stack_depth=10)

        config2 = AwsCodeCorrelationConfig(include=["app"], exclude=["vendor"], stack_depth=10)

        # They should have the same string representation
        self.assertEqual(repr(config1), repr(config2))
        self.assertEqual(config1.to_dict(), config2.to_dict())
        self.assertEqual(config1.to_json(), config2.to_json())
