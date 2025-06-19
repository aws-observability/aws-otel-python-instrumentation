# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from importlib.metadata import PackageNotFoundError
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro._utils import AGENT_OBSERVABILITY_ENABLED, is_agent_observability_enabled, is_installed


class TestUtils(TestCase):
    def setUp(self):
        # Store original env var if it exists
        self.original_env = os.environ.get(AGENT_OBSERVABILITY_ENABLED)

    def tearDown(self):
        # Restore original env var
        if self.original_env is not None:
            os.environ[AGENT_OBSERVABILITY_ENABLED] = self.original_env
        elif AGENT_OBSERVABILITY_ENABLED in os.environ:
            del os.environ[AGENT_OBSERVABILITY_ENABLED]

    def test_is_installed_package_not_found(self):
        """Test is_installed returns False when package is not found"""
        with patch("amazon.opentelemetry.distro._utils.version") as mock_version:
            # Simulate package not found
            mock_version.side_effect = PackageNotFoundError("test-package")

            result = is_installed("test-package>=1.0.0")
            self.assertFalse(result)

    def test_is_installed(self):
        """Test is_installed returns True when version matches the specifier"""
        with patch("amazon.opentelemetry.distro._utils.version") as mock_version:
            # Package is installed and version matches requirement
            mock_version.return_value = "2.5.0"

            # Test with compatible version requirement
            result = is_installed("test-package>=2.0.0")
            self.assertTrue(result)

            # Test with exact version match
            mock_version.return_value = "1.0.0"
            result = is_installed("test-package==1.0.0")
            self.assertTrue(result)

            # Test with version range
            mock_version.return_value = "1.5.0"
            result = is_installed("test-package>=1.0,<2.0")
            self.assertTrue(result)

    def test_is_installed_version_mismatch(self):
        """Test is_installed returns False when version doesn't match"""
        with patch("amazon.opentelemetry.distro._utils.version") as mock_version:
            # Package is installed but version doesn't match requirement
            mock_version.return_value = "1.0.0"

            # Test with incompatible version requirement
            result = is_installed("test-package>=2.0.0")
            self.assertFalse(result)

    def test_is_agent_observability_enabled_various_values(self):
        """Test is_agent_observability_enabled with various environment variable values"""
        # Test with "True" (uppercase)
        os.environ[AGENT_OBSERVABILITY_ENABLED] = "True"
        self.assertTrue(is_agent_observability_enabled())

        # Test with "TRUE" (all caps)
        os.environ[AGENT_OBSERVABILITY_ENABLED] = "TRUE"
        self.assertTrue(is_agent_observability_enabled())

        # Test with "true" (lowercase)
        os.environ[AGENT_OBSERVABILITY_ENABLED] = "true"
        self.assertTrue(is_agent_observability_enabled())

        # Test with "false"
        os.environ[AGENT_OBSERVABILITY_ENABLED] = "false"
        self.assertFalse(is_agent_observability_enabled())

        # Test with "False"
        os.environ[AGENT_OBSERVABILITY_ENABLED] = "False"
        self.assertFalse(is_agent_observability_enabled())

        # Test with arbitrary string
        os.environ[AGENT_OBSERVABILITY_ENABLED] = "yes"
        self.assertFalse(is_agent_observability_enabled())

        # Test with empty string
        os.environ[AGENT_OBSERVABILITY_ENABLED] = ""
        self.assertFalse(is_agent_observability_enabled())

        # Test when env var is not set
        if AGENT_OBSERVABILITY_ENABLED in os.environ:
            del os.environ[AGENT_OBSERVABILITY_ENABLED]
        self.assertFalse(is_agent_observability_enabled())
