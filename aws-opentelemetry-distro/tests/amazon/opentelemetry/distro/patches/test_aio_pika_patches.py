# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for Aio-Pika patches functionality."""

import unittest
from unittest.mock import Mock, patch

from amazon.opentelemetry.distro.patches._aio_pika_patches import (
    _apply_aio_pika_instrumentation_patches,
    patch_callback_decorator_decorate,
)


class TestAioPikaPatches(unittest.TestCase):
    """Test cases for Aio-Pika patches functionality."""

    def test_patch_callback_decorator_decorate(self):
        """Test patch_callback_decorator_decorate function."""
        # Mock the original decorate method
        original_decorate = Mock()

        # Mock CallbackDecorator instance
        mock_decorator = Mock()
        # pylint: disable=protected-access
        mock_decorator._tracer = Mock()
        mock_decorator._get_span = Mock(return_value=Mock())

        # Mock callback function
        mock_callback = Mock()

        # Create the patched decorate method
        patched_decorate = patch_callback_decorator_decorate(original_decorate)

        # Call the patched method
        result = patched_decorate(mock_decorator, mock_callback)

        # Verify original_decorate was called once (with enhanced callback, not original)
        original_decorate.assert_called_once()
        # Check that the first argument is the decorator instance
        call_args = original_decorate.call_args[0]
        self.assertEqual(call_args[0], mock_decorator)
        # Check that the second argument is a callable (the enhanced callback)
        self.assertTrue(callable(call_args[1]))

        # Verify we got a function back (the enhanced decorated callback)
        self.assertTrue(callable(result))

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches." "get_code_correlation_enabled_status")
    def test_apply_aio_pika_instrumentation_patches_disabled(self, mock_get_status):
        """Test patches are not applied when code correlation is disabled."""
        mock_get_status.return_value = False

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.aio_pika": None,
                "opentelemetry.instrumentation.aio_pika.callback_decorator": None,
            },
        ):
            # Should not raise exception when code correlation is disabled
            _apply_aio_pika_instrumentation_patches()

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches." "get_code_correlation_enabled_status")
    def test_apply_aio_pika_instrumentation_patches_enabled(self, mock_get_status):
        """Test patches are applied when code correlation is enabled."""
        mock_get_status.return_value = True

        # Mock CallbackDecorator
        mock_callback_decorator = Mock()

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.aio_pika": Mock(),
                "opentelemetry.instrumentation.aio_pika.callback_decorator": Mock(
                    CallbackDecorator=mock_callback_decorator
                ),
            },
        ):
            _apply_aio_pika_instrumentation_patches()

            # Verify the decorate method was patched
            self.assertTrue(hasattr(mock_callback_decorator, "decorate"))

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches." "get_code_correlation_enabled_status")
    def test_apply_aio_pika_instrumentation_patches_import_error(self, mock_get_status):
        """Test patches handle import errors gracefully."""
        mock_get_status.return_value = True

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.aio_pika": None,
                "opentelemetry.instrumentation.aio_pika.callback_decorator": None,
            },
        ):
            # Should not raise exception when import fails
            _apply_aio_pika_instrumentation_patches()

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.logger")
    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches." "get_code_correlation_enabled_status")
    def test_apply_aio_pika_instrumentation_patches_exception_handling(self, mock_get_status, mock_logger):
        """Test patches handle general exceptions gracefully."""
        mock_get_status.side_effect = Exception("Test exception")

        # Should handle exceptions gracefully
        _apply_aio_pika_instrumentation_patches()

        # Verify warning was logged
        mock_logger.warning.assert_called_once()
