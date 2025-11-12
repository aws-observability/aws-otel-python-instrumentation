# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest.mock import Mock, patch

from amazon.opentelemetry.distro.patches._django_patches import (
    _apply_django_code_attributes_patch,
    _apply_django_instrumentation_patches,
)
from opentelemetry.test.test_base import TestBase

try:
    import django
    from django.conf import settings
    from django.http import HttpResponse
    from django.test import RequestFactory
    from django.urls import path

    from opentelemetry.instrumentation.django import DjangoInstrumentor
    from opentelemetry.instrumentation.django.middleware.otel_middleware import _DjangoMiddleware

    DJANGO_AVAILABLE = True
except ImportError:
    DJANGO_AVAILABLE = False


class TestDjangoPatches(TestBase):
    """Test Django patches functionality."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()

    def tearDown(self):
        """Clean up after tests."""
        super().tearDown()

    def test_apply_django_instrumentation_patches_enabled(self):
        """Test Django instrumentation patches when code correlation is enabled."""
        with patch(
            "amazon.opentelemetry.distro.patches._django_patches._apply_django_code_attributes_patch"
        ) as mock_patch:
            _apply_django_instrumentation_patches()
            mock_patch.assert_called_once()


class TestDjangoCodeAttributesPatches(TestBase):
    """Test Django code attributes patches functionality."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()

    def tearDown(self):
        """Clean up after tests."""
        super().tearDown()

    @patch("amazon.opentelemetry.distro.patches._django_patches._logger")
    def test_apply_django_code_attributes_patch_success(self, mock_logger):
        """Test successful application of Django code attributes patch."""
        # Mock Django modules and classes
        mock_django_instrumentor = Mock()
        mock_middleware_class = Mock()

        # Mock the original methods
        original_instrument = Mock()
        original_uninstrument = Mock()

        mock_django_instrumentor._instrument = original_instrument
        mock_django_instrumentor._uninstrument = original_uninstrument

        with patch.dict(
            "sys.modules",
            {
                "amazon.opentelemetry.distro.code_correlation": Mock(),
                "opentelemetry.instrumentation.django": Mock(DjangoInstrumentor=mock_django_instrumentor),
                "opentelemetry.instrumentation.django.middleware.otel_middleware": Mock(
                    _DjangoMiddleware=mock_middleware_class
                ),
            },
        ):
            _apply_django_code_attributes_patch()
            mock_logger.debug.assert_called_with("Django instrumentation code attributes patch applied successfully")

    @patch("amazon.opentelemetry.distro.patches._django_patches._logger")
    def test_apply_django_code_attributes_patch_import_error(self, mock_logger):
        """Test Django code attributes patch with import error."""
        with patch("builtins.__import__", side_effect=ImportError("Module not found")):
            _apply_django_code_attributes_patch()
            # Check that warning was called with the format string and an ImportError
            mock_logger.warning.assert_called()
            args, _kwargs = mock_logger.warning.call_args
            self.assertEqual(args[0], "Failed to apply Django code attributes patch: %s")
            self.assertIsInstance(args[1], ImportError)
            self.assertEqual(str(args[1]), "Module not found")

    def test_apply_django_code_attributes_patch_exception_handling(self):
        """Test Django code attributes patch handles exceptions gracefully."""
        with patch("amazon.opentelemetry.distro.patches._django_patches._logger"):
            # Test that the function doesn't raise exceptions even with import failures
            _apply_django_code_attributes_patch()
            # Should complete without errors regardless of Django availability
            # If we get here, no exception was raised


class TestDjangoRealIntegration(TestBase):
    """Test Django patches with real Django integration."""

    def setUp(self):
        """Set up test fixtures with Django configuration."""
        super().setUp()
        if not DJANGO_AVAILABLE:
            self.skipTest("Django not available")

        # Configure Django with minimal settings
        if not settings.configured:
            settings.configure(
                DEBUG=True,
                SECRET_KEY="test-secret-key-for-django-patches-test",
                ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"],
                INSTALLED_APPS=[
                    "django.contrib.contenttypes",
                    "django.contrib.auth",
                ],
                MIDDLEWARE=[
                    "opentelemetry.instrumentation.django.middleware.otel_middleware._DjangoMiddleware",
                ],
                ROOT_URLCONF=__name__,
                USE_TZ=True,
            )

        django.setup()
        self.factory = RequestFactory()

    def tearDown(self):
        """Clean up after tests."""
        # Uninstrument Django if it was instrumented
        try:
            instrumentor = DjangoInstrumentor()
            if instrumentor.is_instrumented_by_opentelemetry:
                instrumentor.uninstrument()
        except Exception:
            pass
        super().tearDown()

    def test_django_view_function_patch_process_view(self):
        """Test Django patch with real view function triggering process_view method."""

        # Define a simple Django view function
        def test_view(request):
            """Test view function for Django patch testing."""
            return HttpResponse("Hello from test view")

        # Apply the Django code attributes patch
        _apply_django_code_attributes_patch()

        # Instrument Django
        instrumentor = DjangoInstrumentor()
        instrumentor.instrument()

        try:
            # Create a mock span
            from unittest.mock import Mock

            mock_span = Mock()
            mock_span.is_recording.return_value = True

            # Create Django request
            request = self.factory.get("/test/")

            # Create middleware instance
            middleware = _DjangoMiddleware(get_response=lambda req: HttpResponse())

            # Manually set up the request environment as Django middleware would
            middleware_key = middleware._environ_activation_key
            span_key = middleware._environ_span_key

            request.META[middleware_key] = "test_activation"
            request.META[span_key] = mock_span

            # Call process_view method which should trigger the patch
            middleware.process_view(request, test_view, [], {})

            # The original process_view returns None, so we don't assign result

            # Verify span methods were called (this confirms the patched code ran)
            mock_span.is_recording.assert_called()

            # Test passes if no exceptions are raised and the method returns correctly
            # The main goal is to ensure the removal of _code_cache doesn't break functionality

        finally:
            # Clean up instrumentation
            instrumentor.uninstrument()

    def test_django_class_based_view_patch_process_view(self):
        """Test Django patch with class-based view to test handler targeting logic."""

        # Define a class-based Django view
        class TestClassView:
            """Test class-based view for Django patch testing."""

            def get(self, request):
                return HttpResponse("Hello from class view")

        # Create a mock view function that mimics Django's class-based view structure
        def mock_view_func(request):
            return HttpResponse("Mock response")

        # Add view_class attribute to simulate Django's class-based view wrapper
        mock_view_func.view_class = TestClassView

        # Apply the Django code attributes patch
        _apply_django_code_attributes_patch()

        # Instrument Django
        instrumentor = DjangoInstrumentor()
        instrumentor.instrument()

        try:
            # Create a mock span
            from unittest.mock import Mock

            mock_span = Mock()
            mock_span.is_recording.return_value = True

            # Create Django request with GET method
            request = self.factory.get("/test/")

            # Create middleware instance
            middleware = _DjangoMiddleware(get_response=lambda req: HttpResponse())

            # Manually set up the request environment as Django middleware would
            middleware_key = middleware._environ_activation_key
            span_key = middleware._environ_span_key

            request.META[middleware_key] = "test_activation"
            request.META[span_key] = mock_span

            # Call process_view method with the class-based view function
            # This should trigger the class-based view logic where it extracts the handler
            middleware.process_view(request, mock_view_func, [], {})

            # The original process_view returns None, so we don't assign result

            # Verify span methods were called (this confirms the patched code ran)
            mock_span.is_recording.assert_called()

            # Test passes if no exceptions are raised and the method returns correctly
            # The main goal is to ensure the removal of _code_cache doesn't break functionality

        finally:
            # Clean up instrumentation
            instrumentor.uninstrument()


# Simple URL pattern for Django testing (referenced by ROOT_URLCONF)
def dummy_view(request):
    return HttpResponse("dummy")


urlpatterns = [
    path("test/", dummy_view, name="test"),
]
