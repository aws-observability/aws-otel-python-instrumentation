# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.patches._starlette_patches import _apply_starlette_instrumentation_patches


class TestStarlettePatch(TestCase):
    """Test the Starlette instrumentation patches."""

    @patch("amazon.opentelemetry.distro.patches._starlette_patches.AGENT_OBSERVABILITY_ENABLED", True)
    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_starlette_patch_applied_successfully(self, mock_logger):
        """Test that the Starlette instrumentation patch is applied successfully."""
        # Create a mock StarletteInstrumentor class
        mock_instrumentor_class = MagicMock()
        mock_instrumentor_class.__name__ = "StarletteInstrumentor"

        class MockMiddleware:
            def __init__(self, app, **kwargs):
                pass

        mock_middleware_class = MockMiddleware
        original_init = mock_middleware_class.__init__

        # Create mock modules
        mock_starlette_module = MagicMock()
        mock_starlette_module.StarletteInstrumentor = mock_instrumentor_class

        mock_asgi_module = MagicMock()
        mock_asgi_module.OpenTelemetryMiddleware = mock_middleware_class

        # Mock the imports
        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.starlette": mock_starlette_module,
                "opentelemetry.instrumentation.asgi": mock_asgi_module,
            },
        ):
            # Apply the patch
            _apply_starlette_instrumentation_patches()

            # Verify the instrumentation_dependencies method was replaced
            self.assertTrue(hasattr(mock_instrumentor_class, "instrumentation_dependencies"))

            # Test the patched method returns the expected value
            mock_instance = MagicMock()
            result = mock_instrumentor_class.instrumentation_dependencies(mock_instance)
            self.assertEqual(result, ("starlette >= 0.13",))

            self.assertNotEqual(mock_middleware_class.__init__, original_init)

            # Test middleware patching sets exclude flags
            mock_middleware_instance = MagicMock()
            mock_middleware_instance.exclude_receive_span = False
            mock_middleware_instance.exclude_send_span = False

            mock_middleware_class.__init__(mock_middleware_instance, "app")

            self.assertTrue(mock_middleware_instance.exclude_receive_span)
            self.assertTrue(mock_middleware_instance.exclude_send_span)

            # Verify logging
            mock_logger.debug.assert_called_once_with(
                "Successfully patched Starlette instrumentation_dependencies method"
            )

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_starlette_patch_handles_import_error(self, mock_logger):
        """Test that the patch handles import errors gracefully."""
        # Mock the import to fail by removing the module
        with patch.dict("sys.modules", {"opentelemetry.instrumentation.starlette": None}):
            # This should not raise an exception
            _apply_starlette_instrumentation_patches()

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Starlette instrumentation patches", args[0])

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_starlette_patch_handles_attribute_error(self, mock_logger):
        """Test that the patch handles attribute errors gracefully."""

        # Create a metaclass that raises AttributeError when setting class attributes
        class ErrorMeta(type):
            def __setattr__(cls, name, value):
                if name == "instrumentation_dependencies":
                    raise AttributeError("Cannot set attribute")
                super().__setattr__(name, value)

        # Create a class with the error-raising metaclass
        class MockStarletteInstrumentor(metaclass=ErrorMeta):
            pass

        # Create a mock module
        mock_starlette_module = MagicMock()
        mock_starlette_module.StarletteInstrumentor = MockStarletteInstrumentor

        with patch.dict("sys.modules", {"opentelemetry.instrumentation.starlette": mock_starlette_module}):
            # This should not raise an exception
            _apply_starlette_instrumentation_patches()

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Starlette instrumentation patches", args[0])

    def test_starlette_patch_logs_failure_with_no_logger_patch(self):  # pylint: disable=no-self-use
        """Test that the patch handles exceptions gracefully without logger mock."""
        # Mock the import to fail
        with patch.dict("sys.modules", {"opentelemetry.instrumentation.starlette": None}):
            # This should not raise an exception even without logger mock
            _apply_starlette_instrumentation_patches()

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_starlette_patch_with_exception_during_import(self, mock_logger):
        """Test that the patch handles exceptions during import."""

        # Create a module that raises exception when accessing StarletteInstrumentor
        class FailingModule:
            @property
            def StarletteInstrumentor(self):  # pylint: disable=invalid-name
                raise RuntimeError("Import failed")

        failing_module = FailingModule()

        with patch.dict("sys.modules", {"opentelemetry.instrumentation.starlette": failing_module}):
            # This should not raise an exception
            _apply_starlette_instrumentation_patches()

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Starlette instrumentation patches", args[0])
