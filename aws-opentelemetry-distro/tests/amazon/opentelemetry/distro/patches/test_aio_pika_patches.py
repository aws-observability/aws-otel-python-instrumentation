# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

from amazon.opentelemetry.distro.patches._aio_pika_patches import (
    _apply_aio_pika_instrumentation_patches,
    patch_callback_decorator_decorate,
)


class TestAioPikaPatches(unittest.TestCase):

    def test_patch_callback_decorator_decorate(self):
        # Mock the original decorate method
        original_decorate = Mock()

        # Mock CallbackDecorator instance
        mock_decorator = Mock()
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

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.trace.get_current_span")
    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.add_code_attributes_to_span")
    def test_enhanced_callback_with_no_span(self, mock_add_attributes, mock_get_span):
        """Test enhanced_callback when no current span exists"""
        # Arrange
        mock_get_span.return_value = None
        original_decorate = Mock()
        mock_callback = AsyncMock()
        mock_message = Mock()

        # Create patched decorate function
        patched_decorate = patch_callback_decorator_decorate(original_decorate)

        # Call patched decorate to get the enhanced callback
        patched_decorate(Mock(), mock_callback)

        # Get the enhanced callback from the call
        enhanced_callback = original_decorate.call_args[0][1]

        # Act
        asyncio.run(enhanced_callback(mock_message))

        # Assert
        mock_get_span.assert_called_once()
        mock_add_attributes.assert_not_called()
        mock_callback.assert_called_once_with(mock_message)

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.trace.get_current_span")
    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.add_code_attributes_to_span")
    def test_enhanced_callback_with_non_recording_span(self, mock_add_attributes, mock_get_span):
        """Test enhanced_callback when span is not recording"""
        # Arrange
        mock_span = Mock()
        mock_span.is_recording.return_value = False
        mock_get_span.return_value = mock_span

        original_decorate = Mock()
        mock_callback = AsyncMock()
        mock_message = Mock()

        # Create patched decorate function
        patched_decorate = patch_callback_decorator_decorate(original_decorate)

        # Call patched decorate to get the enhanced callback
        patched_decorate(Mock(), mock_callback)

        # Get the enhanced callback from the call
        enhanced_callback = original_decorate.call_args[0][1]

        # Act
        asyncio.run(enhanced_callback(mock_message))

        # Assert
        mock_get_span.assert_called_once()
        mock_span.is_recording.assert_called_once()
        mock_add_attributes.assert_not_called()
        mock_callback.assert_called_once_with(mock_message)

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.trace.get_current_span")
    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.add_code_attributes_to_span")
    def test_enhanced_callback_with_exception_in_add_attributes(self, mock_add_attributes, mock_get_span):
        """Test enhanced_callback when add_code_attributes_to_span raises exception"""
        # Arrange
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_get_span.return_value = mock_span
        mock_add_attributes.side_effect = Exception("Test exception")

        original_decorate = Mock()
        mock_callback = AsyncMock()
        mock_message = Mock()

        # Create patched decorate function
        patched_decorate = patch_callback_decorator_decorate(original_decorate)

        # Call patched decorate to get the enhanced callback
        patched_decorate(Mock(), mock_callback)

        # Get the enhanced callback from the call
        enhanced_callback = original_decorate.call_args[0][1]

        # Act
        asyncio.run(enhanced_callback(mock_message))

        # Assert
        mock_get_span.assert_called_once()
        mock_span.is_recording.assert_called_once()
        mock_add_attributes.assert_called_once_with(mock_span, mock_callback)
        # Should still call original callback despite exception
        mock_callback.assert_called_once_with(mock_message)

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.trace.get_current_span")
    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.add_code_attributes_to_span")
    def test_enhanced_callback_successful_execution(self, mock_add_attributes, mock_get_span):
        """Test enhanced_callback normal execution path"""
        # Arrange
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_get_span.return_value = mock_span

        original_decorate = Mock()
        mock_callback = AsyncMock()
        mock_message = Mock()

        # Create patched decorate function
        patched_decorate = patch_callback_decorator_decorate(original_decorate)

        # Call patched decorate to get the enhanced callback
        patched_decorate(Mock(), mock_callback)

        # Get the enhanced callback from the call
        enhanced_callback = original_decorate.call_args[0][1]

        # Act
        asyncio.run(enhanced_callback(mock_message))

        # Assert
        mock_get_span.assert_called_once()
        mock_span.is_recording.assert_called_once()
        mock_add_attributes.assert_called_once_with(mock_span, mock_callback)
        mock_callback.assert_called_once_with(mock_message)

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.get_code_correlation_enabled_status")
    def test_apply_aio_pika_instrumentation_patches_disabled(self, mock_get_status):
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

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.get_code_correlation_enabled_status")
    def test_apply_aio_pika_instrumentation_patches_enabled(self, mock_get_status):
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

    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.get_code_correlation_enabled_status")
    def test_apply_aio_pika_instrumentation_patches_import_error(self, mock_get_status):
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
    @patch("amazon.opentelemetry.distro.patches._aio_pika_patches.get_code_correlation_enabled_status")
    def test_apply_aio_pika_instrumentation_patches_exception_handling(self, mock_get_status, mock_logger):
        mock_get_status.side_effect = Exception("Test exception")

        # Should handle exceptions gracefully
        _apply_aio_pika_instrumentation_patches()

        # Verify warning was logged
        mock_logger.warning.assert_called_once()
