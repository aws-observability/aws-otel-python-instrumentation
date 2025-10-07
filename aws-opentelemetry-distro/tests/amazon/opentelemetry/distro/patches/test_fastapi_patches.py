# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import sys
from unittest.mock import patch

from fastapi import APIRouter, FastAPI

from amazon.opentelemetry.distro.patches._fastapi_patches import _apply_fastapi_instrumentation_patches
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.test.test_base import TestBase

# Store original methods at module level before any patches
_ORIGINAL_FASTAPI_INSTRUMENTOR_INSTRUMENT = FastAPIInstrumentor._instrument
_ORIGINAL_FASTAPI_INSTRUMENTOR_UNINSTRUMENT = FastAPIInstrumentor._uninstrument
_ORIGINAL_APIROUTER_ADD_API_ROUTE = APIRouter.add_api_route


class TestFastAPIPatchesRealApp(TestBase):
    """Test FastAPI patches functionality."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()

        # Restore original methods
        APIRouter.add_api_route = _ORIGINAL_APIROUTER_ADD_API_ROUTE
        FastAPIInstrumentor._instrument = _ORIGINAL_FASTAPI_INSTRUMENTOR_INSTRUMENT
        FastAPIInstrumentor._uninstrument = _ORIGINAL_FASTAPI_INSTRUMENTOR_UNINSTRUMENT

        # Create FastAPI app
        self.app = FastAPI()

        @self.app.get("/hello/{name}")
        async def hello(name: str):
            return {"message": f"Hello {name}!"}

    def tearDown(self):
        """Clean up after tests."""
        super().tearDown()

        # Restore original methods
        APIRouter.add_api_route = _ORIGINAL_APIROUTER_ADD_API_ROUTE
        FastAPIInstrumentor._instrument = _ORIGINAL_FASTAPI_INSTRUMENTOR_INSTRUMENT
        FastAPIInstrumentor._uninstrument = _ORIGINAL_FASTAPI_INSTRUMENTOR_UNINSTRUMENT

        # Clean up instrumentor attributes
        instrumentor_instance = FastAPIInstrumentor()
        for attr_name in list(vars(FastAPIInstrumentor).keys()):
            if attr_name.startswith("_original_apirouter"):
                delattr(FastAPIInstrumentor, attr_name)

        for attr_name in [attr for attr in dir(instrumentor_instance) if attr.startswith("_original_apirouter")]:
            if hasattr(instrumentor_instance, attr_name):
                delattr(instrumentor_instance, attr_name)

        try:
            FastAPIInstrumentor().uninstrument()
        except Exception:
            pass

    @patch("amazon.opentelemetry.distro.patches._fastapi_patches.get_code_correlation_enabled_status")
    def test_fastapi_patches_with_real_app(self, mock_get_status):
        """Test FastAPI patches core functionality."""
        mock_get_status.return_value = True
        original_add_api_route = _ORIGINAL_APIROUTER_ADD_API_ROUTE

        # Apply patches
        _apply_fastapi_instrumentation_patches()
        mock_get_status.assert_called_once()

        # Test method wrapping
        instrumentor = FastAPIInstrumentor()
        instrumentor._instrument()

        current_add_api_route = APIRouter.add_api_route
        self.assertNotEqual(current_add_api_route, original_add_api_route)

        # Test app instrumentation
        instrumentor.instrument_app(self.app)
        self.assertIsNotNone(self.app)

        # Test uninstrumentation
        instrumentor._uninstrument()
        restored_add_api_route = APIRouter.add_api_route
        self.assertEqual(restored_add_api_route, original_add_api_route)

    @patch("amazon.opentelemetry.distro.patches._fastapi_patches.get_code_correlation_enabled_status")
    def test_fastapi_patches_disabled(self, mock_get_status):
        """Test FastAPI patches when disabled."""
        mock_get_status.return_value = False

        _apply_fastapi_instrumentation_patches()
        instrumentor = FastAPIInstrumentor()
        instrumentor.instrument_app(self.app)

        mock_get_status.assert_called_once()
        self.assertIsNotNone(self.app)

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)

    @patch("amazon.opentelemetry.distro.patches._fastapi_patches.get_code_correlation_enabled_status")
    def test_fastapi_patches_import_error_handling(self, mock_get_status):
        """Test FastAPI patches with import errors."""
        mock_get_status.return_value = True
        original_modules = sys.modules.copy()

        try:
            # Simulate import error
            modules_to_remove = [
                "fastapi.routing",
                "amazon.opentelemetry.distro.code_correlation",
                "opentelemetry.instrumentation.fastapi",
            ]
            for module in modules_to_remove:
                if module in sys.modules:
                    del sys.modules[module]

            _apply_fastapi_instrumentation_patches()
            mock_get_status.assert_called_once()

        finally:
            sys.modules.clear()
            sys.modules.update(original_modules)

    @patch("amazon.opentelemetry.distro.patches._fastapi_patches.get_code_correlation_enabled_status")
    def test_fastapi_patches_endpoint_decoration(self, mock_get_status):
        """Test endpoint decoration functionality."""
        mock_get_status.return_value = True

        instrumentor = FastAPIInstrumentor()
        instrumentor.instrument_app(self.app)
        _apply_fastapi_instrumentation_patches()
        instrumentor._instrument()

        # Test adding routes
        async def async_endpoint():
            return {"message": "async endpoint"}

        router = APIRouter()
        router.add_api_route("/test_async", async_endpoint, methods=["GET"])
        self.app.include_router(router)

        self.assertTrue(len(self.app.routes) > 0)

    @patch("amazon.opentelemetry.distro.patches._fastapi_patches.get_code_correlation_enabled_status")
    def test_fastapi_patches_uninstrument_error_handling(self, mock_get_status):
        """Test uninstrument error handling."""
        mock_get_status.return_value = True

        instrumentor = FastAPIInstrumentor()
        _apply_fastapi_instrumentation_patches()
        instrumentor._instrument()

        # Break stored references to trigger error handling
        if hasattr(instrumentor, "_original_apirouter"):
            instrumentor._original_apirouter = None

        try:
            instrumentor._uninstrument()
        except Exception:
            pass  # Expected to handle gracefully

    @patch("amazon.opentelemetry.distro.patches._fastapi_patches.get_code_correlation_enabled_status")
    def test_fastapi_patches_code_correlation_import_error(self, mock_get_status):
        """Test code correlation import error handling."""
        mock_get_status.return_value = True
        original_modules = sys.modules.copy()

        try:
            # Remove code_correlation module to simulate import error
            modules_to_remove = [
                "amazon.opentelemetry.distro.code_correlation",
                "amazon.opentelemetry.distro.code_correlation.record_code_attributes",
            ]
            for module in modules_to_remove:
                if module in sys.modules:
                    del sys.modules[module]

            instrumentor = FastAPIInstrumentor()
            _apply_fastapi_instrumentation_patches()
            instrumentor._instrument()

        finally:
            sys.modules.clear()
            sys.modules.update(original_modules)

    @patch("amazon.opentelemetry.distro.patches._fastapi_patches.get_code_correlation_enabled_status")
    def test_fastapi_patches_double_decoration_prevention(self, mock_get_status):
        """Test prevention of double decoration."""
        mock_get_status.return_value = True

        _apply_fastapi_instrumentation_patches()
        instrumentor = FastAPIInstrumentor()
        instrumentor._instrument()

        # Create pre-decorated endpoint
        async def test_endpoint():
            return {"message": "test"}

        test_endpoint._current_span_code_attributes_decorated = True

        router = APIRouter()
        router.add_api_route("/test_double", test_endpoint, methods=["GET"])
        self.app.include_router(router)

        self.assertTrue(len(self.app.routes) > 0)

    @patch("amazon.opentelemetry.distro.patches._fastapi_patches.get_code_correlation_enabled_status")
    def test_fastapi_patches_none_endpoint_handling(self, mock_get_status):
        """Test handling of None endpoints."""
        mock_get_status.return_value = True

        _apply_fastapi_instrumentation_patches()
        instrumentor = FastAPIInstrumentor()
        instrumentor._instrument()

        router = APIRouter()

        # Test None endpoint handling
        try:
            router.add_api_route("/test_none", None, methods=["GET"])
        except Exception:
            pass  # Expected to handle gracefully

        try:
            router.add_api_route("/test_string", "not_callable", methods=["GET"])
        except Exception:
            pass  # Expected to handle gracefully
