# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from importlib.metadata import PackageNotFoundError
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro._utils import (
    AGENT_OBSERVABILITY_ENABLED,
    get_aws_region,
    is_agent_observability_enabled,
    is_installed,
)


class TestUtils(TestCase):
    def setUp(self):
        # Store original env var if it exists
        self.original_env = os.environ.get(AGENT_OBSERVABILITY_ENABLED)
        # Clear it to ensure clean state
        if AGENT_OBSERVABILITY_ENABLED in os.environ:
            del os.environ[AGENT_OBSERVABILITY_ENABLED]

    def tearDown(self):
        # First clear the env var
        if AGENT_OBSERVABILITY_ENABLED in os.environ:
            del os.environ[AGENT_OBSERVABILITY_ENABLED]
        # Then restore original if it existed
        if self.original_env is not None:
            os.environ[AGENT_OBSERVABILITY_ENABLED] = self.original_env

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

    def test_get_aws_region_with_botocore(self):
        """Test get_aws_region when botocore is available and returns a region"""
        with patch("amazon.opentelemetry.distro._utils.is_installed") as mock_is_installed:
            mock_is_installed.return_value = True

            # Create a mock botocore session
            mock_session_class = MagicMock()
            mock_session_instance = MagicMock()
            mock_session_instance.region_name = "us-east-1"
            mock_session_class.Session.return_value = mock_session_instance

            # Patch the import statement directly in the function
            with patch.dict("sys.modules", {"botocore": MagicMock(session=mock_session_class)}):
                region = get_aws_region()
                self.assertEqual(region, "us-east-1")

    def test_get_aws_region_without_botocore(self):
        """Test get_aws_region when botocore is not installed"""
        with patch("amazon.opentelemetry.distro._utils.is_installed") as mock_is_installed:
            mock_is_installed.return_value = False

            region = get_aws_region()
            self.assertIsNone(region)

    def test_get_aws_region_botocore_no_region(self):
        """Test get_aws_region when botocore is available but returns no region"""
        with patch("amazon.opentelemetry.distro._utils.is_installed") as mock_is_installed:
            mock_is_installed.return_value = True

            # Create a mock botocore session with no region
            mock_session_class = MagicMock()
            mock_session_instance = MagicMock()
            mock_session_instance.region_name = None
            mock_session_class.Session.return_value = mock_session_instance

            # Patch the import statement directly in the function
            with patch.dict("sys.modules", {"botocore": MagicMock(session=mock_session_class)}):
                region = get_aws_region()
                self.assertIsNone(region)

    def test_get_aws_region_botocore_import_error(self):
        """Test get_aws_region when botocore import fails"""
        with patch("amazon.opentelemetry.distro._utils.is_installed") as mock_is_installed:
            mock_is_installed.return_value = True

            # Mock ImportError when trying to import botocore
            with patch.dict("sys.modules", {"botocore": None}):
                with patch("builtins.__import__", side_effect=ImportError("Botocore not found")):
                    region = get_aws_region()
                    self.assertIsNone(region)

    def test_get_aws_region_botocore_attribute_error(self):
        """Test get_aws_region when botocore has attribute errors"""
        with patch("amazon.opentelemetry.distro._utils.is_installed") as mock_is_installed:
            mock_is_installed.return_value = True

            # Mock the botocore import with AttributeError on Session
            mock_session_module = MagicMock()
            mock_session_module.Session.side_effect = AttributeError("Session class not found")

            # Patch the import statement directly in the function
            with patch.dict("sys.modules", {"botocore": MagicMock(session=mock_session_module)}):
                region = get_aws_region()
                self.assertIsNone(region)
