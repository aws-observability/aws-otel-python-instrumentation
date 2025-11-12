# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest.mock import Mock, patch

from amazon.opentelemetry.distro.patches._pika_patches import (
    _apply_pika_instrumentation_patches,
    patch_decorate_callback,
)
from opentelemetry.test.test_base import TestBase


class TestPikaPatches(TestBase):
    """Test Pika patches functionality."""

    def test_apply_pika_instrumentation_patches_success(self):
        """Test Pika instrumentation patches when import is successful."""
        # Mock pika utils
        mock_utils = Mock()
        mock_original_decorate_callback = Mock()
        mock_utils._decorate_callback = mock_original_decorate_callback

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.pika": Mock(utils=mock_utils),
                "opentelemetry.instrumentation.pika.utils": mock_utils,
            },
        ):
            _apply_pika_instrumentation_patches()

            # Verify that the _decorate_callback method was replaced
            self.assertNotEqual(mock_utils._decorate_callback, mock_original_decorate_callback)

    @patch("amazon.opentelemetry.distro.patches._pika_patches.logger")
    def test_apply_pika_instrumentation_patches_import_error(self, mock_logger):
        """Test Pika instrumentation patches with import error."""
        # Patch the specific import that would fail
        with patch.dict(
            "sys.modules",
            {"opentelemetry.instrumentation.pika": None, "opentelemetry.instrumentation.pika.utils": None},
        ):
            _apply_pika_instrumentation_patches()

            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Pika patches: pika utils not available", args[0])

    @patch("amazon.opentelemetry.distro.patches._pika_patches.logger")
    def test_apply_pika_instrumentation_patches_exception(self, mock_logger):
        """Test Pika instrumentation patches with general exception."""

        # Mock import that raises an exception
        def failing_import(*args, **kwargs):
            if "pika" in str(args):
                raise Exception("Unexpected error")
            return Mock()

        with patch("builtins.__import__", side_effect=failing_import):
            _apply_pika_instrumentation_patches()

            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Pika patches", args[0])


class TestPatchDecorateCallback(TestBase):
    """Test patch_decorate_callback functionality."""

    def test_patch_decorate_callback_wrapper(self):
        """Test that patch_decorate_callback creates proper wrapper."""
        original_function = Mock(return_value="decorated_callback_result")
        original_function.__name__ = "original_decorate_callback"

        patched_function = patch_decorate_callback(original_function)

        # Check that functools.wraps was applied
        self.assertEqual(patched_function.__name__, "original_decorate_callback")

        # Test calling the patched function
        mock_callback = Mock()
        mock_tracer = Mock()
        task_name = "test_task"
        mock_consume_hook = Mock()

        result = patched_function(mock_callback, mock_tracer, task_name, mock_consume_hook)

        # Original function should be called with enhanced consume hook
        original_function.assert_called_once()
        args, _kwargs = original_function.call_args
        self.assertEqual(args[0], mock_callback)
        self.assertEqual(args[1], mock_tracer)
        self.assertEqual(args[2], task_name)
        # The fourth argument should be our enhanced consume hook, not the original
        self.assertNotEqual(args[3], mock_consume_hook)
        self.assertEqual(result, "decorated_callback_result")

    @patch("amazon.opentelemetry.distro.patches._pika_patches.add_code_attributes_to_span")
    def test_enhanced_consume_hook_success(self, mock_add_attributes):
        """Test enhanced consume hook with successful code attribute addition."""
        original_function = Mock(return_value="decorated_callback_result")
        mock_callback = Mock()
        mock_consume_hook = Mock()

        patched_function = patch_decorate_callback(original_function)

        # Call the patched function to get the enhanced consume hook
        patched_function(mock_callback, Mock(), "test_task", mock_consume_hook)

        # Get the enhanced consume hook from the call
        enhanced_consume_hook = original_function.call_args[0][3]

        # Test the enhanced consume hook
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_body = "test_body"
        mock_properties = {"test": "properties"}

        enhanced_consume_hook(mock_span, mock_body, mock_properties)

        # Verify code attributes were added
        mock_add_attributes.assert_called_once_with(mock_span, mock_callback)

        # Verify original consume hook was called
        mock_consume_hook.assert_called_once_with(mock_span, mock_body, mock_properties)

    @patch("amazon.opentelemetry.distro.patches._pika_patches.add_code_attributes_to_span")
    def test_enhanced_consume_hook_non_recording_span(self, mock_add_attributes):
        """Test enhanced consume hook with non-recording span."""
        original_function = Mock(return_value="decorated_callback_result")
        mock_callback = Mock()
        mock_consume_hook = Mock()

        patched_function = patch_decorate_callback(original_function)

        # Call the patched function to get the enhanced consume hook
        patched_function(mock_callback, Mock(), "test_task", mock_consume_hook)

        # Get the enhanced consume hook from the call
        enhanced_consume_hook = original_function.call_args[0][3]

        # Test the enhanced consume hook with non-recording span
        mock_span = Mock()
        mock_span.is_recording.return_value = False
        mock_body = "test_body"
        mock_properties = {"test": "properties"}

        enhanced_consume_hook(mock_span, mock_body, mock_properties)

        # Code attributes should not be added for non-recording span
        mock_add_attributes.assert_not_called()

        # Original consume hook should still be called
        mock_consume_hook.assert_called_once_with(mock_span, mock_body, mock_properties)

    @patch("amazon.opentelemetry.distro.patches._pika_patches.add_code_attributes_to_span")
    def test_enhanced_consume_hook_none_span(self, mock_add_attributes):
        """Test enhanced consume hook with None span."""
        original_function = Mock(return_value="decorated_callback_result")
        mock_callback = Mock()
        mock_consume_hook = Mock()

        patched_function = patch_decorate_callback(original_function)

        # Call the patched function to get the enhanced consume hook
        patched_function(mock_callback, Mock(), "test_task", mock_consume_hook)

        # Get the enhanced consume hook from the call
        enhanced_consume_hook = original_function.call_args[0][3]

        # Test the enhanced consume hook with None span
        mock_body = "test_body"
        mock_properties = {"test": "properties"}

        enhanced_consume_hook(None, mock_body, mock_properties)

        # Code attributes should not be added for None span
        mock_add_attributes.assert_not_called()

        # Original consume hook should still be called
        mock_consume_hook.assert_called_once_with(None, mock_body, mock_properties)

    def test_enhanced_consume_hook_no_original_consume_hook(self):
        """Test enhanced consume hook when no original consume hook is provided."""
        original_function = Mock(return_value="decorated_callback_result")
        mock_callback = Mock()

        patched_function = patch_decorate_callback(original_function)

        # Call the patched function with no consume hook
        patched_function(mock_callback, Mock(), "test_task", None)

        # Get the enhanced consume hook from the call
        enhanced_consume_hook = original_function.call_args[0][3]

        # Test the enhanced consume hook
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_body = "test_body"
        mock_properties = {"test": "properties"}

        # Should not raise exception even without original consume hook
        enhanced_consume_hook(mock_span, mock_body, mock_properties)

    @patch("amazon.opentelemetry.distro.patches._pika_patches.add_code_attributes_to_span")
    def test_enhanced_consume_hook_add_attributes_exception(self, mock_add_attributes):
        """Test enhanced consume hook handles add_code_attributes_to_span exceptions."""
        mock_add_attributes.side_effect = Exception("Add attributes error")

        original_function = Mock(return_value="decorated_callback_result")
        mock_callback = Mock()
        mock_consume_hook = Mock()

        patched_function = patch_decorate_callback(original_function)

        # Call the patched function to get the enhanced consume hook
        patched_function(mock_callback, Mock(), "test_task", mock_consume_hook)

        # Get the enhanced consume hook from the call
        enhanced_consume_hook = original_function.call_args[0][3]

        # Test the enhanced consume hook
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_body = "test_body"
        mock_properties = {"test": "properties"}

        # Should not raise exception despite add_code_attributes_to_span failing
        enhanced_consume_hook(mock_span, mock_body, mock_properties)

        # Verify add_code_attributes_to_span was called (and failed)
        mock_add_attributes.assert_called_once_with(mock_span, mock_callback)

        # Original consume hook should still be called
        mock_consume_hook.assert_called_once_with(mock_span, mock_body, mock_properties)

    def test_enhanced_consume_hook_original_hook_exception(self):
        """Test enhanced consume hook handles original consume hook exceptions."""
        original_function = Mock(return_value="decorated_callback_result")
        mock_callback = Mock()
        mock_consume_hook = Mock()
        mock_consume_hook.side_effect = Exception("Original hook error")

        patched_function = patch_decorate_callback(original_function)

        # Call the patched function to get the enhanced consume hook
        patched_function(mock_callback, Mock(), "test_task", mock_consume_hook)

        # Get the enhanced consume hook from the call
        enhanced_consume_hook = original_function.call_args[0][3]

        # Test the enhanced consume hook
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_body = "test_body"
        mock_properties = {"test": "properties"}

        # Should not raise exception despite original consume hook failing
        enhanced_consume_hook(mock_span, mock_body, mock_properties)

        # Verify original consume hook was called (and failed)
        mock_consume_hook.assert_called_once_with(mock_span, mock_body, mock_properties)


class TestPikaPatchesIntegration(TestBase):
    """Test Pika patches integration scenarios."""

    def test_full_patch_application_flow(self):
        """Test the complete flow of applying Pika patches."""
        # Create a realistic mock setup
        mock_utils = Mock()
        original_decorate_callback = Mock(__name__="original_decorate_callback")
        mock_utils._decorate_callback = original_decorate_callback

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.pika": Mock(utils=mock_utils),
                "opentelemetry.instrumentation.pika.utils": mock_utils,
            },
        ):
            _apply_pika_instrumentation_patches()

            # Verify the method was replaced with a wrapped version
            self.assertNotEqual(mock_utils._decorate_callback, original_decorate_callback)
            self.assertEqual(mock_utils._decorate_callback.__name__, "original_decorate_callback")

            # Test calling the patched method
            mock_callback = Mock()
            mock_tracer = Mock()
            task_name = "test_task"
            mock_consume_hook = Mock()

            # Should not raise exceptions
            mock_utils._decorate_callback(mock_callback, mock_tracer, task_name, mock_consume_hook)

            # Original function should be called
            original_decorate_callback.assert_called_once()

    @patch("amazon.opentelemetry.distro.patches._pika_patches.add_code_attributes_to_span")
    def test_end_to_end_enhanced_consume_hook(self, mock_add_attributes):
        """Test end-to-end flow with enhanced consume hook."""
        # Create a realistic mock setup
        mock_utils = Mock()
        original_decorate_callback = Mock(return_value="decorated_result")
        mock_utils._decorate_callback = original_decorate_callback

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.pika": Mock(utils=mock_utils),
                "opentelemetry.instrumentation.pika.utils": mock_utils,
            },
        ):
            _apply_pika_instrumentation_patches()

            # Now use the patched method
            mock_callback = Mock()
            mock_tracer = Mock()
            task_name = "test_task"
            mock_consume_hook = Mock()

            result = mock_utils._decorate_callback(mock_callback, mock_tracer, task_name, mock_consume_hook)

            # Get the enhanced consume hook that was passed to the original function
            enhanced_consume_hook = original_decorate_callback.call_args[0][3]

            # Test using the enhanced consume hook
            mock_span = Mock()
            mock_span.is_recording.return_value = True
            mock_body = "test_body"
            mock_properties = {"test": "properties"}

            enhanced_consume_hook(mock_span, mock_body, mock_properties)

            # Verify code attributes were added
            mock_add_attributes.assert_called_once_with(mock_span, mock_callback)

            # Verify original consume hook was called
            mock_consume_hook.assert_called_once_with(mock_span, mock_body, mock_properties)

            # Verify result was returned
            self.assertEqual(result, "decorated_result")
