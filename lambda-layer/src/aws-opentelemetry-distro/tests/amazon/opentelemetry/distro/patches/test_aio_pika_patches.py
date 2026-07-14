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

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.logger")
    def test_apply_aio_pika_instrumentation_patches_success(self, mock_logger):
        """Test patches are applied successfully."""
        # Mock CallbackDecorator
        mock_callback_decorator = Mock()
        original_decorate = Mock()
        mock_callback_decorator.decorate = original_decorate

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

            # Verify the decorate method was patched (should be different from original)
            self.assertNotEqual(mock_callback_decorator.decorate, original_decorate)

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.logger")
    def test_apply_aio_pika_instrumentation_patches_import_error(self, mock_logger):
        """Test patches handle import errors gracefully."""
        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.aio_pika": None,
                "opentelemetry.instrumentation.aio_pika.callback_decorator": None,
            },
        ):
            # Should not raise exception when import fails
            _apply_aio_pika_instrumentation_patches()

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Aio-Pika patches", args[0])

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.logger")
    def test_apply_aio_pika_instrumentation_patches_exception_handling(self, mock_logger):
        """Test patches handle general exceptions gracefully."""

        # Mock import that raises an exception
        def failing_import(*args, **kwargs):
            if "callback_decorator" in str(args):
                raise Exception("Test exception")
            return Mock()

        with patch("builtins.__import__", side_effect=failing_import):
            # Should handle exceptions gracefully
            _apply_aio_pika_instrumentation_patches()

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Aio-Pika patches", args[0])
