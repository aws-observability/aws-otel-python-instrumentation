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

    @patch("amazon.opentelemetry.distro.patches._django_patches.get_code_correlation_enabled_status")
    def test_apply_django_instrumentation_patches_enabled(self, mock_get_status):
        """Test Django instrumentation patches when code correlation is enabled."""
        mock_get_status.return_value = True

        with patch(
            "amazon.opentelemetry.distro.patches._django_patches._apply_django_code_attributes_patch"
        ) as mock_patch:
            _apply_django_instrumentation_patches()
            mock_get_status.assert_called_once()
            mock_patch.assert_called_once()

    @patch("amazon.opentelemetry.distro.patches._django_patches.get_code_correlation_enabled_status")
    def test_apply_django_instrumentation_patches_disabled(self, mock_get_status):
        """Test Django instrumentation patches when code correlation is disabled."""
        mock_get_status.return_value = False

        with patch(
            "amazon.opentelemetry.distro.patches._django_patches._apply_django_code_attributes_patch"
        ) as mock_patch:
            _apply_django_instrumentation_patches()
            mock_get_status.assert_called_once()
            mock_patch.assert_not_called()

    @patch("amazon.opentelemetry.distro.patches._django_patches.get_code_correlation_enabled_status")
    def test_apply_django_instrumentation_patches_none_status(self, mock_get_status):
        """Test Django instrumentation patches when status is None."""
        mock_get_status.return_value = None

        with patch(
            "amazon.opentelemetry.distro.patches._django_patches._apply_django_code_attributes_patch"
        ) as mock_patch:
            _apply_django_instrumentation_patches()
            mock_get_status.assert_called_once()
            mock_patch.assert_not_called()


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
            args, kwargs = mock_logger.warning.call_args
            self.assertEqual(args[0], "Failed to apply Django code attributes patch: %s")
            self.assertIsInstance(args[1], ImportError)
            self.assertEqual(str(args[1]), "Module not found")

    def test_apply_django_code_attributes_patch_exception_handling(self):
        """Test Django code attributes patch handles exceptions gracefully."""
        with patch("amazon.opentelemetry.distro.patches._django_patches._logger"):
            # Test that the function doesn't raise exceptions even with import failures
            _apply_django_code_attributes_patch()
            # Should complete without errors regardless of Django availability
            self.assertTrue(True)  # If we get here, no exception was raised


@patch("amazon.opentelemetry.distro.patches._django_patches.get_code_correlation_enabled_status", return_value=True)
class TestDjangoRealIntegration(TestBase):
    """Test Django patches with real Django integration."""

    def setUp(self):
        """Set up test fixtures with Django configuration."""
        super().setUp()
        self.skipTest("Django not available") if not DJANGO_AVAILABLE else None

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

    def test_django_view_function_patch_process_view(self, mock_get_status):
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
            # Create a mock span without attributes (to trigger fallback logic in lines 122-132)
            from unittest.mock import Mock

            mock_span = Mock()
            mock_span.is_recording.return_value = True
            mock_span.attributes = None  # This will trigger the fallback path
            mock_span.set_attribute = Mock()

            # Create Django request
            request = self.factory.get("/test/")

            # Create middleware instance
            middleware = _DjangoMiddleware(get_response=lambda req: HttpResponse())

            # Manually set up the request environment as Django middleware would
            middleware_key = middleware._environ_activation_key
            span_key = middleware._environ_span_key

            request.META[middleware_key] = "test_activation"
            request.META[span_key] = mock_span

            # Verify the middleware has the code cache attribute after patching
            self.assertTrue(hasattr(_DjangoMiddleware, "_code_cache"))

            # Clear any existing cache
            _DjangoMiddleware._code_cache.clear()

            # Call process_view method which should trigger the patch
            result = middleware.process_view(request, test_view, [], {})

            # The result should be None (original process_view returns None)
            self.assertIsNone(result)

            # Verify span methods were called
            mock_span.is_recording.assert_called()

            # Check that code attributes were added to the span via fallback logic
            # This should have triggered the fallback code in lines 122-132
            cache = _DjangoMiddleware._code_cache
            self.assertIn(test_view, cache)

            # Verify cache contains expected code attribute keys (from fallback logic)
            cached_attrs = cache[test_view]
            self.assertIn("code.function.name", cached_attrs)
            self.assertEqual(cached_attrs.get("code.function.name"), "test_view")
            self.assertIn("code.file.path", cached_attrs)
            self.assertIn("code.line.number", cached_attrs)

            # Verify span.set_attribute was called with cached attributes
            mock_span.set_attribute.assert_called()

        finally:
            # Clean up instrumentation
            instrumentor.uninstrument()

    def test_django_class_based_view_patch_process_view(self, mock_get_status):
        """Test Django patch with class-based view to cover lines 128-132."""

        # Define a class-based Django view
        class TestClassView:
            """Test class-based view for Django patch testing."""

            def get(self, request):
                return HttpResponse("Hello from class view")

        # Apply the Django code attributes patch
        _apply_django_code_attributes_patch()

        # Instrument Django
        instrumentor = DjangoInstrumentor()
        instrumentor.instrument()

        try:
            # Create a mock span without attributes (to trigger fallback logic)
            from unittest.mock import Mock

            mock_span = Mock()
            mock_span.is_recording.return_value = True
            mock_span.attributes = None  # This will trigger the fallback path
            mock_span.set_attribute = Mock()

            # Create Django request
            request = self.factory.get("/test/")

            # Create middleware instance
            middleware = _DjangoMiddleware(get_response=lambda req: HttpResponse())

            # Manually set up the request environment as Django middleware would
            middleware_key = middleware._environ_activation_key
            span_key = middleware._environ_span_key

            request.META[middleware_key] = "test_activation"
            request.META[span_key] = mock_span

            # Clear any existing cache
            _DjangoMiddleware._code_cache.clear()

            # Create a class instance to use as target
            # This should trigger the inspect.isclass() path in lines 128-132
            test_class = TestClassView

            # Call process_view method with the class (not instance) as view_func
            # This should trigger the fallback logic with inspect.isclass(target) = True
            result = middleware.process_view(request, test_class, [], {})

            # The result should be None (original process_view returns None)
            self.assertIsNone(result)

            # Verify span methods were called
            mock_span.is_recording.assert_called()

            # Check that code attributes were added to the span via fallback logic
            # This should have triggered the inspect.isclass() code in lines 128-132
            cache = _DjangoMiddleware._code_cache
            self.assertIn(test_class, cache)

            # Verify cache contains expected code attribute keys (from inspect.isclass fallback)
            cached_attrs = cache[test_class]
            self.assertIn("code.function.name", cached_attrs)
            self.assertEqual(cached_attrs.get("code.function.name"), "TestClassView")
            self.assertIn("code.file.path", cached_attrs)

            # Verify span.set_attribute was called with cached attributes
            mock_span.set_attribute.assert_called()

        finally:
            # Clean up instrumentation
            instrumentor.uninstrument()


# Simple URL pattern for Django testing (referenced by ROOT_URLCONF)
def dummy_view(request):
    return HttpResponse("dummy")


urlpatterns = [
    path("test/", dummy_view, name="test"),
]
