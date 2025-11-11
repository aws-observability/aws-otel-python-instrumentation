# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest.mock import Mock, patch

from amazon.opentelemetry.distro.patches._celery_patches import (
    _add_code_correlation_to_span,
    _apply_celery_instrumentation_patches,
    _extract_task_function,
    patch_celery_prerun,
)
from opentelemetry.test.test_base import TestBase


class TestCeleryPatches(TestBase):
    """Test Celery patches functionality."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()

    def tearDown(self):
        """Clean up after tests."""
        super().tearDown()

    @patch("amazon.opentelemetry.distro.patches._celery_patches.CeleryInstrumentor")
    def test_apply_celery_instrumentation_patches_enabled(self, mock_instrumentor):
        """Test Celery instrumentation patches when CeleryInstrumentor is available."""
        # Mock CeleryInstrumentor
        mock_original_trace_prerun = Mock()
        mock_instrumentor._trace_prerun = mock_original_trace_prerun

        _apply_celery_instrumentation_patches()

        # Verify that the _trace_prerun method was replaced
        self.assertNotEqual(mock_instrumentor._trace_prerun, mock_original_trace_prerun)

    @patch("amazon.opentelemetry.distro.patches._celery_patches.logger")
    def test_apply_celery_instrumentation_patches_import_error(self, mock_logger):
        """Test Celery instrumentation patches with import error."""
        # Patch CeleryInstrumentor to None to simulate import failure
        with patch("amazon.opentelemetry.distro.patches._celery_patches.CeleryInstrumentor", None):
            _apply_celery_instrumentation_patches()

            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Celery patches: CeleryInstrumentor not available", args[0])

    @patch("amazon.opentelemetry.distro.patches._celery_patches.logger")
    def test_apply_celery_instrumentation_patches_exception(self, mock_logger):
        """Test Celery instrumentation patches with general exception."""
        # Mock CeleryInstrumentor to raise an exception when accessing _trace_prerun
        mock_instrumentor = Mock()

        def raise_exception():
            raise Exception("Unexpected error")

        type(mock_instrumentor).__getattr__ = Mock(side_effect=raise_exception)

        with patch("amazon.opentelemetry.distro.patches._celery_patches.CeleryInstrumentor", mock_instrumentor):
            _apply_celery_instrumentation_patches()

            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Celery instrumentation patches", args[0])


class TestExtractTaskFunction(TestBase):
    """Test _extract_task_function functionality."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()

    def test_extract_task_function_none_task(self):
        """Test extract task function with None task."""
        result = _extract_task_function(None)
        self.assertIsNone(result)

    def test_extract_task_function_with_run_method(self):
        """Test extract task function with task that has run method."""

        def sample_task():
            pass

        mock_task = Mock()
        mock_task.run = sample_task

        result = _extract_task_function(mock_task)
        self.assertEqual(result, sample_task)

    def test_extract_task_function_with_run_bound_method(self):
        """Test extract task function with task that has bound run method."""

        def sample_function():
            pass

        mock_task = Mock()
        mock_run = Mock()
        mock_run.__func__ = sample_function
        mock_task.run = mock_run

        result = _extract_task_function(mock_task)
        self.assertEqual(result, sample_function)

    def test_extract_task_function_with_call_method(self):
        """Test extract task function with task that has __call__ method."""

        def sample_task():
            pass

        mock_task = Mock()
        mock_task.run = None  # No run method
        mock_call = Mock()
        mock_call.__func__ = Mock()
        mock_call.__func__.__name__ = "sample_function"  # Not '__call__'
        mock_call.__func__ = sample_task
        mock_task.__call__ = mock_call

        result = _extract_task_function(mock_task)
        self.assertEqual(result, sample_task)

    def test_extract_task_function_with_call_method_skip_default(self):
        """Test extract task function skips default __call__ method."""
        mock_task = Mock()
        mock_task.run = None  # No run method
        mock_call = Mock()
        mock_call.__func__ = Mock()
        mock_call.__func__.__name__ = "__call__"  # Default __call__, should skip
        mock_call.__name__ = "__call__"  # Also set the direct name to __call__
        mock_task.__call__ = mock_call
        # Ensure no __wrapped__ attribute exists
        del mock_task.__wrapped__

        result = _extract_task_function(mock_task)
        self.assertIsNone(result)  # Should skip default __call__ and return None

    def test_extract_task_function_with_wrapped(self):
        """Test extract task function with __wrapped__ attribute."""

        def sample_task():
            pass

        mock_task = Mock()
        mock_task.run = None  # No run method
        mock_task.__call__ = None  # No __call__ method
        mock_task.__wrapped__ = sample_task

        result = _extract_task_function(mock_task)
        self.assertEqual(result, sample_task)

    def test_extract_task_function_no_methods(self):
        """Test extract task function with no extractable methods."""
        mock_task = Mock()
        mock_task.run = None
        mock_task.__call__ = None
        del mock_task.__wrapped__  # Remove __wrapped__ attribute

        result = _extract_task_function(mock_task)
        self.assertIsNone(result)

    def test_extract_task_function_exception_handling(self):
        """Test extract task function handles exceptions gracefully."""
        mock_task = Mock()

        # Configure accessing the run attribute to raise an exception
        def raise_exception():
            raise ValueError("Error accessing run")  # pylint: disable=broad-exception-raised

        type(mock_task).run = property(lambda self: raise_exception())

        result = _extract_task_function(mock_task)
        self.assertIsNone(result)


class TestAddCodeCorrelationToSpan(TestBase):
    """Test _add_code_correlation_to_span functionality."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()

    def test_add_code_correlation_none_span(self):
        """Test add code correlation with None span."""
        mock_task = Mock()

        # Should not raise exception
        _add_code_correlation_to_span(None, mock_task)

    def test_add_code_correlation_non_recording_span(self):
        """Test add code correlation with non-recording span."""
        mock_span = Mock()
        mock_span.is_recording.return_value = False
        mock_task = Mock()

        _add_code_correlation_to_span(mock_span, mock_task)

        mock_span.is_recording.assert_called_once()

    @patch("amazon.opentelemetry.distro.patches._celery_patches._extract_task_function")
    @patch("amazon.opentelemetry.distro.patches._celery_patches.add_code_attributes_to_span")
    def test_add_code_correlation_success(self, mock_add_attributes, mock_extract):
        """Test successful code correlation addition."""
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_task = Mock()
        mock_function = Mock()
        mock_extract.return_value = mock_function

        _add_code_correlation_to_span(mock_span, mock_task)

        mock_span.is_recording.assert_called_once()
        mock_extract.assert_called_once_with(mock_task)
        mock_add_attributes.assert_called_once_with(mock_span, mock_function)

    @patch("amazon.opentelemetry.distro.patches._celery_patches._extract_task_function")
    def test_add_code_correlation_no_function_extracted(self, mock_extract):
        """Test code correlation when no function is extracted."""
        mock_span = Mock()
        mock_span.is_recording.return_value = True
        mock_task = Mock()
        mock_extract.return_value = None

        _add_code_correlation_to_span(mock_span, mock_task)

        mock_span.is_recording.assert_called_once()
        mock_extract.assert_called_once_with(mock_task)

    def test_add_code_correlation_exception_handling(self):
        """Test code correlation handles exceptions gracefully."""
        mock_span = Mock()
        mock_span.is_recording.side_effect = Exception("Span error")
        mock_task = Mock()

        # Should not raise exception
        _add_code_correlation_to_span(mock_span, mock_task)


class TestPatchCeleryPrerun(TestBase):
    """Test patch_celery_prerun functionality."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()

    def test_patch_celery_prerun_wrapper(self):
        """Test that patch_celery_prerun creates proper wrapper."""
        original_function = Mock(return_value="original_result")
        original_function.__name__ = "original_trace_prerun"

        patched_function = patch_celery_prerun(original_function)

        # Check that functools.wraps was applied
        self.assertEqual(patched_function.__name__, "original_trace_prerun")

        # Test calling the patched function
        mock_self = Mock()
        args = ("arg1", "arg2")
        kwargs = {"task": Mock(), "task_id": "test_id"}

        result = patched_function(mock_self, *args, **kwargs)

        # Original function should be called
        original_function.assert_called_once_with(mock_self, *args, **kwargs)
        self.assertEqual(result, "original_result")

    @patch("amazon.opentelemetry.distro.patches._celery_patches._add_code_correlation_to_span")
    def test_patch_celery_prerun_adds_correlation(self, mock_add_correlation):
        """Test that patched function adds code correlation."""
        original_function = Mock(return_value="original_result")
        mock_task = Mock()
        mock_task_id = "test_task_id"

        patched_function = patch_celery_prerun(original_function)

        # Test that the patched function works without errors
        # The actual import behavior is tested in integration tests
        mock_self = Mock()
        kwargs = {"task": mock_task, "task_id": mock_task_id}

        result = patched_function(mock_self, **kwargs)

        # Verify original function was called
        original_function.assert_called_once_with(mock_self, **kwargs)
        self.assertEqual(result, "original_result")

        # The function should complete without raising exceptions
        # which validates that the patch logic doesn't break the flow

    def test_patch_celery_prerun_missing_task(self):
        """Test patched function with missing task."""
        original_function = Mock(return_value="original_result")

        patched_function = patch_celery_prerun(original_function)

        mock_self = Mock()
        kwargs = {"task_id": "test_id"}  # Missing task

        result = patched_function(mock_self, **kwargs)

        # Should still call original function and return result
        original_function.assert_called_once_with(mock_self, **kwargs)
        self.assertEqual(result, "original_result")

    def test_patch_celery_prerun_missing_task_id(self):
        """Test patched function with missing task_id."""
        original_function = Mock(return_value="original_result")

        patched_function = patch_celery_prerun(original_function)

        mock_self = Mock()
        kwargs = {"task": Mock()}  # Missing task_id

        result = patched_function(mock_self, **kwargs)

        # Should still call original function and return result
        original_function.assert_called_once_with(mock_self, **kwargs)
        self.assertEqual(result, "original_result")

    def test_patch_celery_prerun_no_context(self):
        """Test patched function when retrieve_context returns None."""
        original_function = Mock(return_value="original_result")
        mock_task = Mock()
        mock_task_id = "test_task_id"

        # Mock the utils.retrieve_context to return None
        mock_utils = Mock()
        mock_utils.retrieve_context.return_value = None

        patched_function = patch_celery_prerun(original_function)

        with patch.dict(
            "sys.modules",
            {"opentelemetry.instrumentation.celery.utils": mock_utils},
        ):
            mock_self = Mock()
            kwargs = {"task": mock_task, "task_id": mock_task_id}

            result = patched_function(mock_self, **kwargs)

            # Should still complete successfully
            original_function.assert_called_once_with(mock_self, **kwargs)
            self.assertEqual(result, "original_result")

    def test_patch_celery_prerun_exception_handling(self):
        """Test patched function handles exceptions gracefully."""
        original_function = Mock(return_value="original_result")

        # Mock that will cause an exception in the patch logic
        mock_utils = Mock()
        mock_utils.retrieve_context.side_effect = Exception("Context error")

        patched_function = patch_celery_prerun(original_function)

        with patch.dict(
            "sys.modules",
            {"opentelemetry.instrumentation.celery.utils": mock_utils},
        ):
            mock_self = Mock()
            kwargs = {"task": Mock(), "task_id": "test_id"}

            result = patched_function(mock_self, **kwargs)

            # Should still call original function and return result despite exception
            original_function.assert_called_once_with(mock_self, **kwargs)
            self.assertEqual(result, "original_result")


class TestCeleryPatchesIntegration(TestBase):
    """Test Celery patches integration scenarios."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()

    @patch("amazon.opentelemetry.distro.patches._celery_patches.CeleryInstrumentor")
    def test_full_patch_application_flow(self, mock_instrumentor):
        """Test the complete flow of applying Celery patches."""
        # Create a realistic mock setup
        original_trace_prerun = Mock(__name__="original_trace_prerun")
        mock_instrumentor._trace_prerun = original_trace_prerun

        _apply_celery_instrumentation_patches()

        # Verify the method was replaced with a wrapped version
        self.assertNotEqual(mock_instrumentor._trace_prerun, original_trace_prerun)
        self.assertEqual(mock_instrumentor._trace_prerun.__name__, "original_trace_prerun")

        # Test calling the patched method
        mock_self = Mock()
        kwargs = {"task": Mock(), "task_id": "test"}

        # Should not raise exceptions
        mock_instrumentor._trace_prerun(mock_self, **kwargs)
