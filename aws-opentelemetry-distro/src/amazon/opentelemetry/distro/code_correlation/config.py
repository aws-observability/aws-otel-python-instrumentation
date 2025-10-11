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
_ENV_CONFIG = "OTEL_AWS_CODE_CORRELATION_CONFIG"

_logger = logging.getLogger(__name__)


class AwsCodeCorrelationConfig:
    """
    Configuration manager for AWS OpenTelemetry code correlation features.

    This class encapsulates the parsing of environment variables that control
    code correlation behavior, including package inclusion/exclusion lists
    and stack trace depth configuration.

    Environment Variables:
        OTEL_AWS_CODE_CORRELATION_CONFIG: JSON configuration with detailed settings

    Example Configuration:
        export OTEL_AWS_CODE_CORRELATION_CONFIG='{
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
    def from_env(cls) -> "AwsCodeCorrelationConfig":
        """
        Create configuration instance from environment variables.

        Returns:
            AwsCodeCorrelationConfig: Configured instance
        """
        # Parse JSON configuration
        config_str = os.getenv(_ENV_CONFIG, "{}").strip()
        if not config_str:
            config_str = "{}"

        try:
            config_data = json.loads(config_str)
        except json.JSONDecodeError as json_error:
            _logger.warning("Invalid JSON in %s: %s. Using empty configuration.", _ENV_CONFIG, json_error)
            config_data = {}

        # Ensure config_data is a dictionary
        if not isinstance(config_data, dict):
            _logger.warning(
                "Configuration in %s must be a JSON object, got %s. Using empty configuration.",
                _ENV_CONFIG,
                type(config_data).__name__,
            )
            config_data = {}

        return cls(
            include=config_data.get("include", []),
            exclude=config_data.get("exclude", []),
            stack_depth=config_data.get("stack_depth", 0),
        )

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
            f"AwsCodeCorrelationConfig("
            f"include={self.include}, "
            f"exclude={self.exclude}, "
            f"stack_depth={self.stack_depth})"
        )
