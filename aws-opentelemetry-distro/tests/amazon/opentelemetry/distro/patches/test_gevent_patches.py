# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import sys
from importlib.metadata import PackageNotFoundError
from unittest import TestCase
from unittest.mock import MagicMock, patch

# Create a mock gevent module that can be imported
# This allows tests to run without gevent installed
# The mock must be done before the import and that's why the #noqa: E402
_mock_gevent = MagicMock()
_mock_gevent.monkey = MagicMock()
sys.modules["gevent"] = _mock_gevent
sys.modules["gevent.monkey"] = _mock_gevent.monkey

from amazon.opentelemetry.distro.patches._gevent_patches import (  # noqa: E402
    AWS_GEVENT_PATCH_MODULES,
    _is_gevent_installed,
    apply_gevent_monkey_patch,
)


class TestGeventPatches(TestCase):
    """Test suite for gevent monkey patching functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Clear environment variable before each test
        if AWS_GEVENT_PATCH_MODULES in os.environ:
            del os.environ[AWS_GEVENT_PATCH_MODULES]

    def tearDown(self):
        """Clean up after each test."""
        # Clear environment variable after each test
        if AWS_GEVENT_PATCH_MODULES in os.environ:
            del os.environ[AWS_GEVENT_PATCH_MODULES]

    def test_is_gevent_installed_when_installed(self):
        """Test _is_gevent_installed returns True when gevent is installed."""
        with patch("amazon.opentelemetry.distro.patches._gevent_patches.version") as mock_version:
            mock_version.return_value = "23.9.1"
            result = _is_gevent_installed()
            self.assertTrue(result)
            mock_version.assert_called_once()

    def test_is_gevent_installed_when_not_installed(self):
        """Test _is_gevent_installed returns False when gevent is not installed."""
        with patch("amazon.opentelemetry.distro.patches._gevent_patches.version") as mock_version:
            mock_version.side_effect = PackageNotFoundError("gevent not found")
            result = _is_gevent_installed()
            self.assertFalse(result)
            mock_version.assert_called_once()

    def test_apply_gevent_monkey_patch_when_gevent_not_installed(self):
        """Test apply_gevent_monkey_patch does nothing when gevent is not installed."""
        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = False
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            # When gevent is not installed, the function returns early
            mock_patch_all.assert_not_called()

    def test_apply_gevent_monkey_patch_with_default_all(self):
        """Test apply_gevent_monkey_patch with default 'all' behavior."""
        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            mock_patch_all.assert_called_once_with()

    def test_apply_gevent_monkey_patch_with_explicit_all(self):
        """Test apply_gevent_monkey_patch with explicit 'all' environment variable."""
        os.environ[AWS_GEVENT_PATCH_MODULES] = "all"

        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            mock_patch_all.assert_called_once_with()

    def test_apply_gevent_monkey_patch_with_none(self):
        """Test apply_gevent_monkey_patch with 'none' skips patching."""
        os.environ[AWS_GEVENT_PATCH_MODULES] = "none"

        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            mock_patch_all.assert_not_called()

    def test_apply_gevent_monkey_patch_with_single_module(self):
        """Test apply_gevent_monkey_patch with a single module."""
        os.environ[AWS_GEVENT_PATCH_MODULES] = "socket"

        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            mock_patch_all.assert_called_once_with(
                socket=True,
                time=False,
                select=False,
                thread=False,
                os=False,
                ssl=False,
                subprocess=False,
                sys=False,
                builtins=False,
                signal=False,
                queue=False,
                contextvars=False,
            )

    def test_apply_gevent_monkey_patch_with_multiple_modules(self):
        """Test apply_gevent_monkey_patch with multiple comma-separated modules."""
        os.environ[AWS_GEVENT_PATCH_MODULES] = "socket, thread, time"

        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            mock_patch_all.assert_called_once_with(
                socket=True,
                time=True,
                select=False,
                thread=True,
                os=False,
                ssl=False,
                subprocess=False,
                sys=False,
                builtins=False,
                signal=False,
                queue=False,
                contextvars=False,
            )

    def test_apply_gevent_monkey_patch_with_all_modules(self):
        """Test apply_gevent_monkey_patch with all available modules."""
        os.environ[AWS_GEVENT_PATCH_MODULES] = (
            "os, thread, time, sys, socket, select, ssl, subprocess, builtins, signal, queue, contextvars"
        )

        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            mock_patch_all.assert_called_once_with(
                socket=True,
                time=True,
                select=True,
                thread=True,
                os=True,
                ssl=True,
                subprocess=True,
                sys=True,
                builtins=True,
                signal=True,
                queue=True,
                contextvars=True,
            )

    def test_apply_gevent_monkey_patch_with_whitespace_in_list(self):
        """Test apply_gevent_monkey_patch handles whitespace in module list."""
        os.environ[AWS_GEVENT_PATCH_MODULES] = "  socket  ,  thread  ,  time  "

        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            mock_patch_all.assert_called_once_with(
                socket=True,
                time=True,
                select=False,
                thread=True,
                os=False,
                ssl=False,
                subprocess=False,
                sys=False,
                builtins=False,
                signal=False,
                queue=False,
                contextvars=False,
            )

    def test_apply_gevent_monkey_patch_with_subset_of_modules(self):
        """Test apply_gevent_monkey_patch with a subset of modules."""
        os.environ[AWS_GEVENT_PATCH_MODULES] = "ssl, subprocess, signal"

        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            mock_patch_all.assert_called_once_with(
                socket=False,
                time=False,
                select=False,
                thread=False,
                os=False,
                ssl=True,
                subprocess=True,
                sys=False,
                builtins=False,
                signal=True,
                queue=False,
                contextvars=False,
            )

    def test_apply_gevent_monkey_patch_with_invalid_module_names(self):
        """Test apply_gevent_monkey_patch ignores invalid module names."""
        os.environ[AWS_GEVENT_PATCH_MODULES] = "socket, invalid_module, thread"

        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            # invalid_module should be ignored, only socket and thread should be True
            mock_patch_all.assert_called_once_with(
                socket=True,
                time=False,
                select=False,
                thread=True,
                os=False,
                ssl=False,
                subprocess=False,
                sys=False,
                builtins=False,
                signal=False,
                queue=False,
                contextvars=False,
            )

    def test_apply_gevent_monkey_patch_handles_exception(self):
        """Test apply_gevent_monkey_patch handles exceptions gracefully."""
        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            mock_patch_all.side_effect = Exception("Monkey patching failed")
            # Should not raise exception
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            mock_patch_all.assert_called_once()

    def test_apply_gevent_monkey_patch_with_empty_string(self):
        """Test apply_gevent_monkey_patch with empty string environment variable."""
        os.environ[AWS_GEVENT_PATCH_MODULES] = ""

        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            # Empty string should result in all False
            mock_patch_all.assert_called_once_with(
                socket=False,
                time=False,
                select=False,
                thread=False,
                os=False,
                ssl=False,
                subprocess=False,
                sys=False,
                builtins=False,
                signal=False,
                queue=False,
                contextvars=False,
            )

    def test_apply_gevent_monkey_patch_with_case_sensitivity(self):
        """Test apply_gevent_monkey_patch is case-sensitive for module names."""
        os.environ[AWS_GEVENT_PATCH_MODULES] = "Socket, THREAD, time"

        with patch(
            "amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed"
        ) as mock_is_installed, patch("gevent.monkey.patch_all") as mock_patch_all:
            mock_is_installed.return_value = True
            apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
            # Only 'time' should match (case-sensitive), Socket and THREAD should not
            mock_patch_all.assert_called_once_with(
                socket=False,
                time=True,
                select=False,
                thread=False,
                os=False,
                ssl=False,
                subprocess=False,
                sys=False,
                builtins=False,
                signal=False,
                queue=False,
                contextvars=False,
            )

    def test_apply_gevent_monkey_patch_import_error_handling(self):
        """Test apply_gevent_monkey_patch handles import errors gracefully."""
        with patch("amazon.opentelemetry.distro.patches._gevent_patches._is_gevent_installed") as mock_is_installed:
            mock_is_installed.return_value = True
            # Mock the import to raise ImportError
            with patch("builtins.__import__", side_effect=ImportError("Cannot import gevent.monkey")):
                # Should not raise exception
                apply_gevent_monkey_patch()
            mock_is_installed.assert_called_once()
