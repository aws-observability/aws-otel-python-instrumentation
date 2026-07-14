# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Configuration management for AWS OpenTelemetry code correlation features.

This module provides a configuration class that handles environment variable
parsing for code correlation settings, including package inclusion/exclusion
rules and stack depth configuration.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

# Environment variable constants
_ENV_CONFIG = "OTEL_AWS_CODE_ATTRIBUTES_CONFIG"

_logger = logging.getLogger(__name__)


class AwsCodeAttributesConfig:
    """
    Configuration manager for AWS OpenTelemetry code correlation features.

    This class encapsulates the parsing of environment variables that control
    code correlation behavior, including package inclusion/exclusion lists
    and stack trace depth configuration.

    Environment Variables:
        OTEL_AWS_CODE_ATTRIBUTES_CONFIG: JSON configuration with detailed settings

    Example Configuration:
        export OTEL_AWS_CODE_ATTRIBUTES_CONFIG='{
            "include": ["myapp", "mylib"],
            "exclude": ["third-party", "vendor"],
            "stack_depth": 5
        }'
    """

    def __init__(
        self, include: Optional[List[str]] = None, exclude: Optional[List[str]] = None, stack_depth: int = 0
    ) -> None:
        """
        Initialize the configuration object.

        Args:
            include: List of package names to include (default: empty list)
            exclude: List of package names to exclude (default: empty list)
            stack_depth: Maximum stack trace depth (default: 0, meaning unlimited)
        """
        self.include = include or []
        self.exclude = exclude or []
        self.stack_depth = stack_depth

    @classmethod
    def from_env(cls) -> "AwsCodeAttributesConfig":
        """
        Create configuration instance from environment variables.

        Returns:
            AwsCodeAttributesConfig: Configured instance
        """
        config_data = cls._parse_config_data()
        include_value = cls._validate_string_list(config_data, "include")
        exclude_value = cls._validate_string_list(config_data, "exclude")
        stack_depth_value = cls._validate_stack_depth(config_data)

        return cls(
            include=include_value,
            exclude=exclude_value,
            stack_depth=stack_depth_value,
        )

    @classmethod
    def _parse_config_data(cls) -> Dict[str, Any]:
        """Parse configuration data from environment variable."""
        config_str = os.getenv(_ENV_CONFIG, "{}").strip()
        if not config_str:
            config_str = "{}"

        try:
            config_data = json.loads(config_str)
        except json.JSONDecodeError as json_error:
            _logger.warning("Invalid JSON in %s: %s. Using empty configuration.", _ENV_CONFIG, json_error)
            return {}

        if not isinstance(config_data, dict):
            _logger.warning(
                "Configuration in %s must be a JSON object, got %s. Using empty configuration.",
                _ENV_CONFIG,
                type(config_data).__name__,
            )
            return {}

        return config_data

    @classmethod
    def _validate_string_list(cls, config_data: Dict[str, Any], field_name: str) -> List[str]:
        """Validate and extract a string list from config data."""
        field_value = config_data.get(field_name, [])
        if not isinstance(field_value, list):
            _logger.warning(
                "Configuration '%s' in %s must be a list, got %s. Using empty list.",
                field_name,
                _ENV_CONFIG,
                type(field_value).__name__,
            )
            return []

        validated_list = []
        for item in field_value:
            if isinstance(item, str):
                validated_list.append(item)
            else:
                _logger.warning(
                    "Configuration '%s' list item in %s must be a string, got %s. Skipping item.",
                    field_name,
                    _ENV_CONFIG,
                    type(item).__name__,
                )
        return validated_list

    @classmethod
    def _validate_stack_depth(cls, config_data: Dict[str, Any]) -> int:
        """Validate and extract stack depth from config data."""
        stack_depth_value = config_data.get("stack_depth", 0)
        if not isinstance(stack_depth_value, int):
            _logger.warning(
                "Configuration 'stack_depth' in %s must be an integer, got %s. Using default value 0.",
                _ENV_CONFIG,
                type(stack_depth_value).__name__,
            )
            return 0

        if stack_depth_value < 0:
            _logger.warning(
                "Configuration 'stack_depth' in %s must be non-negative, got %s. Using default value 0.",
                _ENV_CONFIG,
                stack_depth_value,
            )
            return 0

        return stack_depth_value

    def to_dict(self) -> Dict[str, Any]:
        """
        Export configuration as a dictionary.

        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        return {"include": self.include, "exclude": self.exclude, "stack_depth": self.stack_depth}

    def to_json(self, indent: Optional[int] = 2) -> str:
        """
        Export configuration as a JSON string.

        Args:
            indent: JSON indentation level (None for compact format)

        Returns:
            str: JSON representation of the configuration
        """
        return json.dumps(self.to_dict(), indent=indent)

    def __repr__(self) -> str:
        """Return string representation of the configuration."""
        return (
            f"AwsCodeAttributesConfig("
            f"include={self.include}, "
            f"exclude={self.exclude}, "
            f"stack_depth={self.stack_depth})"
        )
