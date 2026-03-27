# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.distro.patches._starlette_patches import (
    _apply_starlette_code_attributes_patch,
    _apply_starlette_instrumentation_patches,
)


class TestStarlettePatch(TestCase):
    """Test the Starlette instrumentation patches."""

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_starlette_patch_applied_successfully(self, mock_logger):
        """Test that the Starlette instrumentation patch is applied successfully."""
        for agent_enabled in [True, False]:
            with self.subTest(agent_enabled=agent_enabled):
                # Reset mock for each sub-test
                mock_logger.reset_mock()

                with patch.dict("os.environ", {"AGENT_OBSERVABILITY_ENABLED": "true" if agent_enabled else "false"}):
                    # Create a mock StarletteInstrumentor class
                    mock_instrumentor_class = MagicMock()
                    mock_instrumentor_class.__name__ = "StarletteInstrumentor"

                    def create_middleware_class():
                        class MockMiddleware:
                            def __init__(self, app, **kwargs):
                                pass

                        return MockMiddleware

                    mock_middleware_class = create_middleware_class()

                    mock_starlette_module = MagicMock()
                    mock_starlette_module.StarletteInstrumentor = mock_instrumentor_class

                    mock_asgi_module = MagicMock()
                    mock_asgi_module.OpenTelemetryMiddleware = mock_middleware_class

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

                        mock_middleware_instance = MagicMock()
                        mock_middleware_instance.exclude_receive_span = False
                        mock_middleware_instance.exclude_send_span = False
                        mock_middleware_class.__init__(mock_middleware_instance, "app")

                        # Test middleware patching sets exclude flags
                        if agent_enabled:
                            self.assertTrue(mock_middleware_instance.exclude_receive_span)
                            self.assertTrue(mock_middleware_instance.exclude_send_span)
                        else:
                            self.assertFalse(mock_middleware_instance.exclude_receive_span)
                            self.assertFalse(mock_middleware_instance.exclude_send_span)

                        # Verify logging - expect two debug calls from both patch functions
                        self.assertEqual(mock_logger.debug.call_count, 2)

                        # Check that both expected debug messages were logged
                        debug_calls = [call[0][0] for call in mock_logger.debug.call_args_list]
                        self.assertIn("Successfully patched Starlette instrumentation_dependencies method", debug_calls)
                        self.assertIn(
                            "Starlette instrumentation code attributes patch applied successfully", debug_calls
                        )

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_starlette_patch_handles_import_error(self, mock_logger):
        """Test that the patch handles import errors gracefully."""
        # Mock the import to fail by removing the module
        with patch.dict("sys.modules", {"opentelemetry.instrumentation.starlette": None}):
            # This should not raise an exception
            _apply_starlette_instrumentation_patches()

            # Verify warning was logged - expect two warnings from both sub-functions
            self.assertEqual(mock_logger.warning.call_count, 2)

            # Check that both expected warning messages were logged
            warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
            self.assertIn("Failed to apply Starlette instrumentation patches", warning_calls[0])
            self.assertIn("Failed to apply Starlette code attributes patch", warning_calls[1])


class TestStarletteCodeAttributesPatch(TestCase):
    """Test the Starlette code attributes instrumentation patches using real Route class."""

    def setUp(self):
        """Set up test fixtures."""

        # Sample endpoint functions for testing
        def sample_endpoint():
            return {"message": "Hello World"}

        def another_endpoint():
            return {"message": "Another endpoint"}

        self.sample_endpoint = sample_endpoint
        self.another_endpoint = another_endpoint

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_code_attributes_patch_applied_successfully(self, mock_logger):
        """Test that the code attributes patch is applied successfully using real Route class."""
        try:
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not available")

        # Create a mock StarletteInstrumentor class with proper methods
        class MockStarletteInstrumentor:
            def __init__(self):
                pass

            def _instrument(self, **kwargs):
                pass

            def _uninstrument(self, **kwargs):
                pass

        mock_instrumentor_class = MockStarletteInstrumentor
        mock_instrumentor = MockStarletteInstrumentor()

        # Mock the code correlation decorator
        mock_record_code_attributes = MagicMock()

        def mock_decorator(func):
            """Mock decorator that marks function as decorated."""

            # Create a wrapper that preserves the original function
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            # Mark as decorated
            wrapper._current_span_code_attributes_decorated = True
            wrapper._original_endpoint = func
            wrapper.__name__ = getattr(func, "__name__", "decorated_endpoint")
            return wrapper

        mock_record_code_attributes.side_effect = mock_decorator

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.starlette": MagicMock(StarletteInstrumentor=mock_instrumentor_class),
                "amazon.opentelemetry.distro.code_correlation": MagicMock(
                    record_code_attributes=mock_record_code_attributes
                ),
            },
        ):
            # Store original Route.__init__
            original_route_init = Route.__init__

            try:
                # Apply the patch
                _apply_starlette_code_attributes_patch()

                # Verify the instrumentor methods were patched
                self.assertTrue(hasattr(mock_instrumentor_class, "_instrument"))
                self.assertTrue(hasattr(mock_instrumentor_class, "_uninstrument"))

                # Call the patched _instrument method to set up instrumentation
                mock_instrumentor._instrument()

                # Verify Route.__init__ was modified
                self.assertNotEqual(Route.__init__, original_route_init)

                # Create a route with the patched Route class
                route = Route("/test", endpoint=self.sample_endpoint)

                # Verify the endpoint was decorated
                mock_record_code_attributes.assert_called_once_with(self.sample_endpoint)

                # Test that the route was created successfully
                self.assertEqual(route.path, "/test")
                self.assertIsNotNone(route.endpoint)

                # Verify the endpoint is decorated
                self.assertTrue(hasattr(route.endpoint, "_current_span_code_attributes_decorated"))
                self.assertEqual(route.endpoint._original_endpoint, self.sample_endpoint)

                # Test uninstrumentation
                mock_instrumentor._uninstrument()

                # Verify Route.__init__ was restored
                self.assertEqual(Route.__init__, original_route_init)

                # Verify logging
                mock_logger.debug.assert_called_with(
                    "Starlette instrumentation code attributes patch applied successfully"
                )

            finally:
                # Restore original Route.__init__
                Route.__init__ = original_route_init

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_code_attributes_patch_with_none_endpoint(self, mock_logger):
        """Test that the patch handles None endpoint gracefully."""
        try:
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not available")

        mock_instrumentor_class = MagicMock()
        mock_instrumentor = MagicMock()
        mock_instrumentor_class.return_value = mock_instrumentor

        mock_record_code_attributes = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.starlette": MagicMock(StarletteInstrumentor=mock_instrumentor_class),
                "amazon.opentelemetry.distro.code_correlation": MagicMock(
                    record_code_attributes=mock_record_code_attributes
                ),
            },
        ):
            original_route_init = Route.__init__

            try:
                _apply_starlette_code_attributes_patch()
                mock_instrumentor_class._instrument(mock_instrumentor)

                # Create route with None endpoint
                route = Route("/test", endpoint=None)

                # Verify no decoration was attempted
                mock_record_code_attributes.assert_not_called()

                # Verify route was created successfully
                self.assertEqual(route.path, "/test")
                self.assertIsNone(route.endpoint)

            finally:
                Route.__init__ = original_route_init

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_code_attributes_patch_avoids_double_decoration(self, mock_logger):
        """Test that the patch avoids double decoration of endpoints."""
        try:
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not available")

        mock_instrumentor_class = MagicMock()
        mock_instrumentor = MagicMock()
        mock_instrumentor_class.return_value = mock_instrumentor

        mock_record_code_attributes = MagicMock()

        # Create an already decorated endpoint
        def already_decorated_endpoint():
            return {"message": "Already decorated"}

        already_decorated_endpoint._current_span_code_attributes_decorated = True

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.starlette": MagicMock(StarletteInstrumentor=mock_instrumentor_class),
                "amazon.opentelemetry.distro.code_correlation": MagicMock(
                    record_code_attributes=mock_record_code_attributes
                ),
            },
        ):
            original_route_init = Route.__init__

            try:
                _apply_starlette_code_attributes_patch()
                mock_instrumentor_class._instrument(mock_instrumentor)

                # Create route with already decorated endpoint
                route = Route("/test", endpoint=already_decorated_endpoint)

                # Verify no additional decoration was attempted
                mock_record_code_attributes.assert_not_called()

                # Verify route was created successfully
                self.assertEqual(route.path, "/test")
                self.assertEqual(route.endpoint, already_decorated_endpoint)

            finally:
                Route.__init__ = original_route_init

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_code_attributes_patch_handles_non_callable_endpoint(self, mock_logger):
        """Test that the patch handles non-callable endpoints gracefully."""
        try:
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not available")

        mock_instrumentor_class = MagicMock()
        mock_instrumentor = MagicMock()
        mock_instrumentor_class.return_value = mock_instrumentor

        mock_record_code_attributes = MagicMock()

        # Non-callable endpoint
        non_callable_endpoint = "not_a_function"

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.starlette": MagicMock(StarletteInstrumentor=mock_instrumentor_class),
                "amazon.opentelemetry.distro.code_correlation": MagicMock(
                    record_code_attributes=mock_record_code_attributes
                ),
            },
        ):
            original_route_init = Route.__init__

            try:
                _apply_starlette_code_attributes_patch()
                mock_instrumentor_class._instrument(mock_instrumentor)

                # Create route with non-callable endpoint
                route = Route("/test", endpoint=non_callable_endpoint)

                # Verify no decoration was attempted
                mock_record_code_attributes.assert_not_called()

                # Verify route was created successfully
                self.assertEqual(route.path, "/test")
                self.assertEqual(route.endpoint, non_callable_endpoint)

            finally:
                Route.__init__ = original_route_init

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_code_attributes_patch_handles_decorator_error(self, mock_logger):
        """Test that the patch handles decorator errors gracefully."""
        try:
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not available")

        # Create a mock StarletteInstrumentor class with proper methods
        class MockStarletteInstrumentor:
            def __init__(self):
                pass

            def _instrument(self, **kwargs):
                pass

            def _uninstrument(self, **kwargs):
                pass

        mock_instrumentor_class = MockStarletteInstrumentor
        mock_instrumentor = MockStarletteInstrumentor()

        # Mock decorator that raises exception
        mock_record_code_attributes = MagicMock()
        mock_record_code_attributes.side_effect = RuntimeError("Decorator failed")

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.starlette": MagicMock(StarletteInstrumentor=mock_instrumentor_class),
                "amazon.opentelemetry.distro.code_correlation": MagicMock(
                    record_code_attributes=mock_record_code_attributes
                ),
            },
        ):
            original_route_init = Route.__init__

            try:
                _apply_starlette_code_attributes_patch()
                mock_instrumentor._instrument()

                # Create route - should not raise exception despite decorator error
                route = Route("/test", endpoint=self.sample_endpoint)

                # Verify route was created successfully with original endpoint
                self.assertEqual(route.path, "/test")
                self.assertEqual(route.endpoint, self.sample_endpoint)

                # Verify warning was logged
                mock_logger.warning.assert_called()
                args = mock_logger.warning.call_args[0]
                self.assertIn("Failed to apply code attributes decorator to endpoint", args[0])

            finally:
                Route.__init__ = original_route_init

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_code_attributes_patch_uninstrument_restores_original(self, mock_logger):
        """Test that uninstrumentation properly restores the original Route.__init__."""
        try:
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not available")

        # Create a mock StarletteInstrumentor class with proper methods
        class MockStarletteInstrumentor:
            def __init__(self):
                pass

            def _instrument(self, **kwargs):
                pass

            def _uninstrument(self, **kwargs):
                pass

        mock_instrumentor_class = MockStarletteInstrumentor
        mock_instrumentor = MockStarletteInstrumentor()

        mock_record_code_attributes = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.starlette": MagicMock(StarletteInstrumentor=mock_instrumentor_class),
                "amazon.opentelemetry.distro.code_correlation": MagicMock(
                    record_code_attributes=mock_record_code_attributes
                ),
            },
        ):
            original_route_init = Route.__init__

            try:
                _apply_starlette_code_attributes_patch()

                # Apply instrumentation - this is when Route.__init__ gets wrapped
                mock_instrumentor._instrument()
                patched_init = Route.__init__

                # Verify Route.__init__ was patched
                self.assertNotEqual(patched_init, original_route_init)

                # Apply uninstrumentation
                mock_instrumentor._uninstrument()

                # Verify Route.__init__ was restored
                self.assertEqual(Route.__init__, original_route_init)

            finally:
                Route.__init__ = original_route_init

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_code_attributes_patch_route_with_kwargs(self, mock_logger):
        """Test that the patch works with routes that have additional kwargs."""
        try:
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not available")

        # Create a mock StarletteInstrumentor class with proper methods
        class MockStarletteInstrumentor:
            def __init__(self):
                pass

            def _instrument(self, **kwargs):
                pass

            def _uninstrument(self, **kwargs):
                pass

        mock_instrumentor_class = MockStarletteInstrumentor
        mock_instrumentor = MockStarletteInstrumentor()

        mock_record_code_attributes = MagicMock()
        mock_record_code_attributes.side_effect = lambda func: func

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.starlette": MagicMock(StarletteInstrumentor=mock_instrumentor_class),
                "amazon.opentelemetry.distro.code_correlation": MagicMock(
                    record_code_attributes=mock_record_code_attributes
                ),
            },
        ):
            original_route_init = Route.__init__

            try:
                _apply_starlette_code_attributes_patch()
                mock_instrumentor._instrument()

                # Create route with additional kwargs
                route = Route("/test", endpoint=self.sample_endpoint, methods=["GET", "POST"], name="test_route")

                # Verify decorator was called
                mock_record_code_attributes.assert_called_once_with(self.sample_endpoint)

                # Verify route was created successfully with all attributes
                self.assertEqual(route.path, "/test")
                # Starlette automatically adds HEAD when GET is specified
                self.assertIn("GET", route.methods)
                self.assertIn("POST", route.methods)
                self.assertEqual(route.name, "test_route")
                self.assertIsNotNone(route.endpoint)

            finally:
                Route.__init__ = original_route_init

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_code_attributes_patch_handles_import_error(self, mock_logger):
        """Test that the patch handles import errors gracefully."""
        # Mock import failure
        with patch.dict("sys.modules", {"starlette.routing": None}):
            # This should not raise an exception
            _apply_starlette_code_attributes_patch()

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Starlette code attributes patch", args[0])

    @patch("amazon.opentelemetry.distro.patches._starlette_patches._logger")
    def test_code_attributes_patch_handles_general_exception(self, mock_logger):
        """Test that the patch handles general exceptions gracefully."""

        # Mock import to cause exception - simulate an issue in module loading
        def failing_import(*args, **kwargs):
            raise RuntimeError("General failure")

        with patch("builtins.__import__", side_effect=failing_import):
            # This should not raise an exception
            _apply_starlette_code_attributes_patch()

            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            self.assertIn("Failed to apply Starlette code attributes patch", args[0])

    def test_code_attributes_patch_multiple_routes(self):
        """Test that the patch works correctly with multiple routes."""
        try:
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not available")

        # Create a mock StarletteInstrumentor class with proper methods
        class MockStarletteInstrumentor:
            def __init__(self):
                pass

            def _instrument(self, **kwargs):
                pass

            def _uninstrument(self, **kwargs):
                pass

        mock_instrumentor_class = MockStarletteInstrumentor
        mock_instrumentor = MockStarletteInstrumentor()

        mock_record_code_attributes = MagicMock()
        mock_record_code_attributes.side_effect = lambda func: func

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.starlette": MagicMock(StarletteInstrumentor=mock_instrumentor_class),
                "amazon.opentelemetry.distro.code_correlation": MagicMock(
                    record_code_attributes=mock_record_code_attributes
                ),
            },
        ):
            original_route_init = Route.__init__

            try:
                _apply_starlette_code_attributes_patch()
                mock_instrumentor._instrument()

                # Create multiple routes
                route1 = Route("/test1", endpoint=self.sample_endpoint)
                route2 = Route("/test2", endpoint=self.another_endpoint)

                # Verify both endpoints were decorated
                self.assertEqual(mock_record_code_attributes.call_count, 2)
                mock_record_code_attributes.assert_any_call(self.sample_endpoint)
                mock_record_code_attributes.assert_any_call(self.another_endpoint)

                # Verify routes were created successfully
                self.assertEqual(route1.path, "/test1")
                self.assertEqual(route2.path, "/test2")
                self.assertIsNotNone(route1.endpoint)
                self.assertIsNotNone(route2.endpoint)

            finally:
                Route.__init__ = original_route_init

    def test_code_attributes_patch_route_class_methods(self):
        """Test that the patch preserves Route class methods and attributes."""
        try:
            from starlette.routing import Route
        except ImportError:
            self.skipTest("Starlette not available")

        # Create a mock StarletteInstrumentor class with proper methods
        class MockStarletteInstrumentor:
            def __init__(self):
                pass

            def _instrument(self, **kwargs):
                pass

            def _uninstrument(self, **kwargs):
                pass

        mock_instrumentor_class = MockStarletteInstrumentor
        mock_instrumentor = MockStarletteInstrumentor()

        mock_record_code_attributes = MagicMock()
        mock_record_code_attributes.side_effect = lambda func: func

        with patch.dict(
            "sys.modules",
            {
                "opentelemetry.instrumentation.starlette": MagicMock(StarletteInstrumentor=mock_instrumentor_class),
                "amazon.opentelemetry.distro.code_correlation": MagicMock(
                    record_code_attributes=mock_record_code_attributes
                ),
            },
        ):
            original_route_init = Route.__init__

            try:
                _apply_starlette_code_attributes_patch()
                mock_instrumentor._instrument()

                # Create a route
                route = Route("/test", endpoint=self.sample_endpoint, methods=["GET"])

                # Verify Route methods still work
                self.assertTrue(hasattr(route, "matches"))
                self.assertTrue(hasattr(route, "url_path_for"))

                # Test that the route still functions as expected
                self.assertEqual(route.path, "/test")
                # Starlette automatically adds HEAD when GET is specified
                self.assertIn("GET", route.methods)
                self.assertIn("HEAD", route.methods)

            finally:
                Route.__init__ = original_route_init
