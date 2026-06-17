# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

import amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation as django_mod
from amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation import (
    ServiceEventsDjangoMiddleware,
    _finalize_request,
    _get_endpoint_name,
    _get_request_body,
    _get_route_pattern,
    install_django_hooks,
)
from amazon.opentelemetry.distro.serviceevents.python_monitor import _ServiceEventsMonitorState


class TestGetRoutePattern(TestCase):
    """Tests for _get_route_pattern."""

    def test_get_route_pattern_from_resolver_match(self):
        """resolver_match.route is returned verbatim (slash-less, as Django stores it)."""
        request = MagicMock()
        request.resolver_match = MagicMock()
        request.resolver_match.route = "users/<int:id>"
        result = _get_route_pattern(request)
        self.assertEqual(result, "users/<int:id>")

    def test_get_route_pattern_from_path_info(self):
        """Falls back to path_info when resolver_match is None."""
        request = MagicMock()
        request.resolver_match = None
        request.path_info = "/api/users/42"
        result = _get_route_pattern(request)
        self.assertEqual(result, "/api/users/42")

    def test_get_route_pattern_from_path(self):
        """Falls back to path when both resolver_match and path_info are unavailable."""
        request = MagicMock()
        request.resolver_match = None
        request.path_info = None
        request.path = "/api/users/42"
        result = _get_route_pattern(request)
        self.assertEqual(result, "/api/users/42")

    def test_get_route_pattern_preserves_slashless_route(self):
        """A slash-less Django route is returned as-is (no leading slash added), to
        match Application Signals which derives the same value from span.name."""
        request = MagicMock()
        request.resolver_match = MagicMock()
        request.resolver_match.route = "api/orders"
        result = _get_route_pattern(request)
        self.assertEqual(result, "api/orders")


class TestGetRequestBody(TestCase):
    """Tests for _get_request_body."""

    def test_get_request_body_json(self):
        """Returns parsed JSON when request.body contains valid JSON."""
        request = MagicMock()
        request.body = b'{"key": "value"}'
        request.content_type = "application/json"
        result = _get_request_body(request)
        self.assertEqual(result, {"key": "value"})

    def test_get_request_body_form(self):
        """Returns form dict when JSON parsing fails but form data exists."""
        request = MagicMock()
        # Make body raise on JSON parse
        request.body = b"not-json"
        request.content_type = "application/x-www-form-urlencoded"
        request.POST = MagicMock()
        request.POST.dict.return_value = {"field": "data"}
        # Ensure JSON path fails
        with patch("json.loads", side_effect=ValueError("not json")):
            result = _get_request_body(request)
        self.assertEqual(result, {"field": "data"})

    def test_get_request_body_raw(self):
        """Returns raw body decoded as string when JSON and form fail."""
        request = MagicMock()
        request.body = b"raw body content"
        request.content_type = "text/plain"
        request.POST = MagicMock()
        request.POST.dict.side_effect = Exception("no form")
        # Ensure JSON path fails
        with patch("json.loads", side_effect=ValueError("not json")):
            result = _get_request_body(request)
        self.assertEqual(result, "raw body content")

    def test_get_request_body_json_parse_error_falls_through(self):
        """A JSON content-type whose body is invalid JSON swallows the error and falls through."""
        request = MagicMock()
        # content_type contains "json" so the JSON branch runs, but the body is not valid JSON.
        request.content_type = "application/json"
        request.body = b"{not valid json"
        request.POST = MagicMock()
        request.POST.dict.return_value = {"field": "data"}
        result = _get_request_body(request)
        # JSON parse raises -> caught -> form data returned instead.
        self.assertEqual(result, {"field": "data"})

    def test_get_request_body_json_none_value_falls_through(self):
        """JSON parsing to None skips the JSON return and falls through to later branches."""
        request = MagicMock()
        request.content_type = "application/json"
        # Valid JSON that decodes to None: the `if body is not None` guard fails.
        request.body = b"null"
        request.POST = MagicMock()
        request.POST.dict.return_value = {"form": "value"}
        result = _get_request_body(request)
        self.assertEqual(result, {"form": "value"})

    def test_get_request_body_none(self):
        """Returns None when all extraction methods fail."""
        request = MagicMock()
        type(request).body = PropertyMock(side_effect=Exception("no body"))
        request.POST = MagicMock()
        request.POST.dict.side_effect = Exception("no form")
        result = _get_request_body(request)
        self.assertIsNone(result)

    def test_get_request_body_large_payload(self):
        """Returns truncation message for payloads over 10KB."""
        request = MagicMock()
        large_data = b"x" * 20000
        request.body = large_data
        request.content_type = "application/octet-stream"
        request.POST = MagicMock()
        request.POST.dict.side_effect = Exception("no form")
        # Ensure JSON path fails
        with patch("json.loads", side_effect=ValueError("not json")):
            result = _get_request_body(request)
        self.assertIn("payload too large", result)
        self.assertIn("20000", result)


class TestGetEndpointName(TestCase):
    """Tests for _get_endpoint_name."""

    def test_get_endpoint_name_from_resolver(self):
        """resolver_match.view_name is returned when available."""
        request = MagicMock()
        request.resolver_match = MagicMock()
        request.resolver_match.view_name = "user_detail"
        result = _get_endpoint_name(request)
        self.assertEqual(result, "user_detail")

    def test_get_endpoint_name_no_resolver(self):
        """Returns 'unknown' when resolver_match is None."""
        request = MagicMock()
        request.resolver_match = None
        result = _get_endpoint_name(request)
        self.assertEqual(result, "unknown")


class TestInstallDjangoHooks(TestCase):
    """Tests for install_django_hooks."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        django_mod._endpoint_collector = None
        django_mod._incident_snapshot_collector = None
        django_mod._serviceevents_config = None

    def test_install_django_hooks_stores_collectors(self):
        """Module globals are set after install_django_hooks call."""
        mock_ec = MagicMock()
        mock_isc = MagicMock()
        mock_cfg = MagicMock()

        # Use a real class so load_middleware can be patched
        class FakeBaseHandler:
            def load_middleware(self, *args, **kwargs):
                pass

        mock_django_handler = MagicMock()
        mock_django_handler.BaseHandler = FakeBaseHandler

        with patch.dict("sys.modules", {"django.core.handlers.base": mock_django_handler}):
            install_django_hooks(
                endpoint_collector=mock_ec,
                incident_snapshot_collector=mock_isc,
                config=mock_cfg,
            )

        self.assertIs(django_mod._endpoint_collector, mock_ec)
        self.assertIs(django_mod._incident_snapshot_collector, mock_isc)
        self.assertIs(django_mod._serviceevents_config, mock_cfg)

    def test_install_django_hooks_patches_load_middleware(self):
        """BaseHandler.load_middleware is replaced with instrumented version."""

        class FakeBaseHandler:
            def load_middleware(self, *args, **kwargs):
                pass

        original_load = FakeBaseHandler.load_middleware

        mock_django_handler = MagicMock()
        mock_django_handler.BaseHandler = FakeBaseHandler

        with patch.dict("sys.modules", {"django.core.handlers.base": mock_django_handler}):
            install_django_hooks()

        # load_middleware should have been replaced
        self.assertIsNot(FakeBaseHandler.load_middleware, original_load)

    def test_instrumented_load_middleware_injects_for_build_then_restores(self):
        """SE middleware is at the front DURING the build, and settings.MIDDLEWARE is restored after."""
        call_count = {"n": 0}
        middleware_during_build = {"value": None}

        middleware_path = (
            "amazon.opentelemetry.distro.serviceevents.instrumentation."
            "django_instrumentation.ServiceEventsDjangoMiddleware"
        )

        class FakeBaseHandler:
            def load_middleware(self, *args, **kwargs):
                call_count["n"] += 1
                # Snapshot what the stack build sees: SE middleware must be present and
                # outermost so its process_view/process_exception hooks get registered.
                middleware_during_build["value"] = list(fake_settings.MIDDLEWARE)
                assert fake_settings.MIDDLEWARE[0] == middleware_path

        mock_django_handler = MagicMock()
        mock_django_handler.BaseHandler = FakeBaseHandler

        # Fake django.conf with a settings object whose MIDDLEWARE starts without ours.
        fake_settings = MagicMock()
        fake_settings.MIDDLEWARE = ["django.middleware.common.CommonMiddleware"]
        mock_django_conf = MagicMock()
        mock_django_conf.settings = fake_settings

        with patch.dict(
            "sys.modules",
            {"django.core.handlers.base": mock_django_handler, "django.conf": mock_django_conf},
        ):
            install_django_hooks()
            # Drive the instrumented load_middleware on an instance.
            FakeBaseHandler().load_middleware()

        # During the build the stack saw SE middleware prepended (outermost).
        self.assertEqual(middleware_during_build["value"][0], middleware_path)
        # After the build the customer's MIDDLEWARE is restored unchanged — no persistent
        # global side effect (the chain was already materialized during the build).
        self.assertEqual(fake_settings.MIDDLEWARE, ["django.middleware.common.CommonMiddleware"])
        # Original load_middleware called exactly once: SE middleware is injected before the
        # single build, so the stack (and every middleware) is built only once on first init.
        self.assertEqual(call_count["n"], 1)

    def test_instrumented_load_middleware_skips_when_already_present(self):
        """The wrapped load_middleware does not re-inject when its middleware is already listed."""
        call_count = {"n": 0}

        class FakeBaseHandler:
            def load_middleware(self, *args, **kwargs):
                call_count["n"] += 1

        mock_django_handler = MagicMock()
        mock_django_handler.BaseHandler = FakeBaseHandler

        middleware_path = (
            "amazon.opentelemetry.distro.serviceevents.instrumentation."
            "django_instrumentation.ServiceEventsDjangoMiddleware"
        )
        fake_settings = MagicMock()
        fake_settings.MIDDLEWARE = [middleware_path]
        mock_django_conf = MagicMock()
        mock_django_conf.settings = fake_settings

        with patch.dict(
            "sys.modules",
            {"django.core.handlers.base": mock_django_handler, "django.conf": mock_django_conf},
        ):
            install_django_hooks()
            FakeBaseHandler().load_middleware()

        # Already present: no re-injection and original load_middleware called only once.
        self.assertEqual(fake_settings.MIDDLEWARE, [middleware_path])
        self.assertEqual(call_count["n"], 1)

    def test_instrumented_load_middleware_swallows_injection_error(self):
        """An error while injecting middleware is logged and does not propagate."""
        call_count = {"n": 0}

        class FakeBaseHandler:
            def load_middleware(self, *args, **kwargs):
                call_count["n"] += 1

        mock_django_handler = MagicMock()
        mock_django_handler.BaseHandler = FakeBaseHandler

        # Make accessing settings.MIDDLEWARE raise to hit the except branch.
        fake_settings = MagicMock()
        type(fake_settings).MIDDLEWARE = PropertyMock(side_effect=RuntimeError("settings boom"))
        mock_django_conf = MagicMock()
        mock_django_conf.settings = fake_settings

        with patch.dict(
            "sys.modules",
            {"django.core.handlers.base": mock_django_handler, "django.conf": mock_django_conf},
        ):
            install_django_hooks()
            # Must not raise even though injection fails.
            FakeBaseHandler().load_middleware()

        # Original load_middleware still ran once before the failure.
        self.assertEqual(call_count["n"], 1)

    def test_install_django_hooks_import_error(self):
        """Gracefully handles missing Django (ImportError)."""
        import sys

        saved = sys.modules.get("django.core.handlers.base")
        sys.modules["django.core.handlers.base"] = None  # Force ImportError

        try:
            django_mod._endpoint_collector = None
            install_django_hooks(endpoint_collector=MagicMock())
            # Collectors should remain None since Django import failed
            self.assertIsNone(django_mod._endpoint_collector)
        finally:
            if saved is not None:
                sys.modules["django.core.handlers.base"] = saved
            else:
                sys.modules.pop("django.core.handlers.base", None)


class TestServiceEventsDjangoMiddleware(TestCase):
    """Tests for ServiceEventsDjangoMiddleware."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        django_mod._endpoint_collector = None
        django_mod._incident_snapshot_collector = None
        django_mod._serviceevents_config = None

    def _make_request(self, method="GET", path="/api/test", route="api/test", view_name="test_view"):
        """Helper to create a mock Django request object."""
        request = MagicMock()
        request.method = method
        request.path = path
        request.path_info = path
        request.resolver_match = MagicMock()
        request.resolver_match.route = route
        request.resolver_match.view_name = view_name
        request.body = b'{"key": "value"}'
        request.content_type = "application/json"
        request.POST = MagicMock()
        request.POST.dict.return_value = {}
        request.META = {}
        # Ensure _serviceevents attributes are not pre-set
        del request._serviceevents_start_time
        del request._serviceevents_exception
        del request._serviceevents_skip
        return request

    def test_call_wraps_get_response(self):
        """__call__ calls get_response(request) and returns the response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get_response = MagicMock(return_value=mock_response)

        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        result = middleware(request)

        mock_get_response.assert_called_once_with(request)
        self.assertIs(result, mock_response)

    def test_call_records_start_time(self):
        """request._serviceevents_start_time is set during __call__."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get_response = MagicMock(return_value=mock_response)

        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.time"
        ) as mock_time:
            mock_time.perf_counter_ns.return_value = 1000000000
            middleware(request)

        self.assertEqual(request._serviceevents_start_time, 1000000000)

    def test_call_handles_exception(self):
        """When get_response raises, exception is stored and re-raised."""
        exc = RuntimeError("handler error")
        mock_get_response = MagicMock(side_effect=exc)

        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        with self.assertRaises(RuntimeError):
            middleware(request)

        self.assertIs(request._serviceevents_exception, exc)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.set_current_operation")
    def test_process_view_sets_operation_context(self, mock_set_operation):
        """process_view calls set_current_operation with a "METHOD route" string."""
        mock_get_response = MagicMock()
        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        middleware.process_view(request, MagicMock(), [], {})

        mock_set_operation.assert_called_once()
        # The operation should be "METHOD route" (Django route is slash-less, matching App Signals)
        call_args = mock_set_operation.call_args[0][0]
        self.assertEqual(call_args, "GET api/test")

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.set_current_operation")
    def test_process_view_begins_investigation(self, mock_set_operation):
        """process_view calls monitor_state.begin_investigation()."""
        mock_get_response = MagicMock()
        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        with patch.object(_ServiceEventsMonitorState, "get_instance") as mock_get_inst:
            mock_state = MagicMock()
            mock_get_inst.return_value = mock_state
            middleware.process_view(request, MagicMock(), [], {})

        mock_state.begin_investigation.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.set_current_operation")
    def test_process_view_skips_filtered_endpoints(self, mock_set_operation):
        """Sets _serviceevents_skip = True for filtered endpoints."""
        mock_config = MagicMock()
        mock_config.should_track_endpoint.return_value = False
        django_mod._serviceevents_config = mock_config

        mock_get_response = MagicMock()
        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        middleware.process_view(request, MagicMock(), [], {})

        self.assertTrue(request._serviceevents_skip)
        # set_current_operation should NOT have been called
        mock_set_operation.assert_not_called()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.set_current_operation")
    def test_process_view_captures_trace_id(self, mock_set_operation):
        """Captures trace_id and span_id from OTel span context."""
        mock_get_response = MagicMock()
        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        mock_span = MagicMock()
        mock_span_context = MagicMock()
        mock_span_context.is_valid = True
        mock_span_context.trace_id = 12345678
        mock_span_context.span_id = 87654321
        mock_span.get_span_context.return_value = mock_span_context

        with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
            middleware.process_view(request, MagicMock(), [], {})

        self.assertEqual(request._serviceevents_trace_id, 12345678)
        self.assertEqual(request._serviceevents_span_id, 87654321)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.set_current_operation")
    def test_process_view_captures_trace_id_from_meta_fallback(self, mock_set_operation):
        """Falls back to the OTel span stored in request.META when the context API has no span."""
        mock_get_response = MagicMock()
        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        # Span stored in request.META by the OTel Django middleware.
        meta_span = MagicMock()
        meta_span_context = MagicMock()
        meta_span_context.is_valid = True
        meta_span_context.trace_id = 111
        meta_span_context.span_id = 222
        meta_span.get_span_context.return_value = meta_span_context
        request.META = {"opentelemetry-instrumentor-django.span_key": meta_span}

        # PRIMARY context API returns no usable span so the META fallback runs.
        with patch("opentelemetry.trace.get_current_span", return_value=None):
            middleware.process_view(request, MagicMock(), [], {})

        self.assertEqual(request._serviceevents_trace_id, 111)
        self.assertEqual(request._serviceevents_span_id, 222)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.set_current_operation")
    def test_process_view_swallows_trace_capture_failure(self, mock_set_operation):
        """A failure during trace correlation capture is swallowed and does not abort the request."""
        mock_get_response = MagicMock()
        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        # get_current_span raises: the trace-capture try/except must swallow it.
        with patch("opentelemetry.trace.get_current_span", side_effect=RuntimeError("otel boom")):
            result = middleware.process_view(request, MagicMock(), [], {})

        self.assertIsNone(result)
        # Setup block ran before the trace capture failed, so route context was stored.
        self.assertEqual(request._serviceevents_route, "api/test")

    @patch(
        "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.set_current_operation",
        side_effect=RuntimeError("telemetry exploded"),
    )
    def test_process_view_swallows_telemetry_failure(self, _mock_set_op):
        """A telemetry failure in process_view must not abort the request.

        It must return None (let the view run) and mark the request skipped so
        finalize stays a no-op.
        """
        mock_get_response = MagicMock()
        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        # Must not raise.
        result = middleware.process_view(request, MagicMock(), [], {})
        self.assertIsNone(result)
        self.assertTrue(request._serviceevents_skip)

    def test_process_exception_stores_exception(self):
        """Stores exception on request._serviceevents_exception."""
        mock_get_response = MagicMock()
        middleware = ServiceEventsDjangoMiddleware(mock_get_response)
        request = self._make_request()

        exc = ValueError("bad input")
        middleware.process_exception(request, exc)

        self.assertIs(request._serviceevents_exception, exc)


class TestFinalizeRequest(TestCase):
    """Tests for _finalize_request."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        django_mod._endpoint_collector = None
        django_mod._incident_snapshot_collector = None
        django_mod._serviceevents_config = None

    def _make_request(self, method="GET", path="/api/test", route="api/test", view_name="test_view"):
        """Helper to create a mock Django request object with serviceevents attributes."""
        request = MagicMock()
        request.method = method
        request.path = path
        request.path_info = path
        request.resolver_match = MagicMock()
        request.resolver_match.route = route
        request.resolver_match.view_name = view_name
        request.body = b'{"key": "value"}'
        request.content_type = "application/json"
        request.POST = MagicMock()
        request.POST.dict.return_value = {}
        request.META = {}
        request._serviceevents_start_time = 1000000000
        request._serviceevents_skip = False
        # Set serviceevents context attributes (normally set by process_view). The route is
        # stored verbatim (Django routes are slash-less, matching App Signals).
        request._serviceevents_route = route
        request._serviceevents_method = method
        request._serviceevents_path = path
        request._serviceevents_endpoint = view_name
        # Remove attributes that should not exist by default
        del request._serviceevents_exception
        return request

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_records_endpoint_metric(self, mock_clear):
        """Calls endpoint_collector.record_request() with correct parameters."""
        mock_ec = MagicMock()
        django_mod._endpoint_collector = mock_ec

        request = self._make_request()
        response = MagicMock()
        response.status_code = 200

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.time"
        ) as mock_time:
            mock_time.perf_counter_ns.return_value = 1200000000
            _finalize_request(request, response, None)

        mock_ec.record_request.assert_called_once()
        call_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(call_kwargs["route"], "api/test")
        self.assertEqual(call_kwargs["method"], "GET")
        self.assertEqual(call_kwargs["status_code"], 200)
        self.assertIsNone(call_kwargs["error_info"])

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_processes_incident_on_500(self, mock_clear):
        """Calls incident_snapshot_collector.process_potential_incident() on 500 status."""
        mock_ec = MagicMock()
        mock_isc = MagicMock()
        django_mod._endpoint_collector = mock_ec
        django_mod._incident_snapshot_collector = mock_isc

        request = self._make_request(method="POST", path="/api/orders", route="api/orders", view_name="create_order")
        response = MagicMock()
        response.status_code = 500

        exc = RuntimeError("DB connection failed")

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.time"
        ) as mock_time:
            mock_time.perf_counter_ns.return_value = 1050000000
            _finalize_request(request, response, exc)

        mock_isc.process_potential_incident.assert_called_once()
        call_kwargs = mock_isc.process_potential_incident.call_args[1]
        self.assertEqual(call_kwargs["status_code"], 500)
        self.assertIs(call_kwargs["exception"], exc)

        # Error info should be passed to endpoint collector
        ec_kwargs = mock_ec.record_request.call_args[1]
        self.assertIsNotNone(ec_kwargs["error_info"])

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_infers_500_from_exception(self, mock_clear):
        """When no response is provided, status 500 is inferred from exception."""
        mock_ec = MagicMock()
        django_mod._endpoint_collector = mock_ec

        request = self._make_request()
        exc = ValueError("bad input")

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.time"
        ) as mock_time:
            mock_time.perf_counter_ns.return_value = 1050000000
            _finalize_request(request, None, exc)

        ec_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(ec_kwargs["status_code"], 500)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_always_clears_operation(self, mock_clear):
        """clear_current_operation() is called even when endpoint_collector raises."""
        mock_ec = MagicMock()
        mock_ec.record_request.side_effect = Exception("collector error")
        django_mod._endpoint_collector = mock_ec

        request = self._make_request()
        response = MagicMock()
        response.status_code = 200

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.time"
        ) as mock_time:
            mock_time.perf_counter_ns.return_value = 1200000000
            # Should not raise, error is logged internally
            _finalize_request(request, response, None)

        mock_clear.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_skips_when_serviceevents_skip_true(self, mock_clear):
        """Does nothing when _serviceevents_skip is True."""
        mock_ec = MagicMock()
        django_mod._endpoint_collector = mock_ec

        request = self._make_request()
        request._serviceevents_skip = True
        response = MagicMock()
        response.status_code = 200

        _finalize_request(request, response, None)

        # record_request should NOT have been called
        mock_ec.record_request.assert_not_called()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_skips_when_no_start_time(self, mock_clear):
        """Does nothing when _serviceevents_start_time is not set."""
        mock_ec = MagicMock()
        django_mod._endpoint_collector = mock_ec

        request = self._make_request()
        del request._serviceevents_start_time
        response = MagicMock()
        response.status_code = 200

        _finalize_request(request, response, None)

        # record_request should NOT have been called
        mock_ec.record_request.assert_not_called()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_unmatched_route_collapses_to_first_segment(self, mock_clear):
        """An unmatched-route 404 (process_view never ran) records the first path segment,
        not the full raw path — matching Application Signals."""
        mock_ec = MagicMock()
        django_mod._endpoint_collector = mock_ec

        # Simulate an unmatched 404: __call__ ran (start_time, skip=False) but process_view
        # never did, so none of the _serviceevents_route/method/path attrs were stored.
        request = MagicMock()
        request.method = "GET"
        request.path = "/wp-admin/setup-config.php"
        request.path_info = "/wp-admin/setup-config.php"
        request.resolver_match = None  # no URL match
        request.META = {}
        request.headers = {}
        request.GET = MagicMock()
        request.GET.dict.return_value = {}
        request._serviceevents_start_time = 1000000000
        request._serviceevents_skip = False
        del request._serviceevents_exception
        del request._serviceevents_route
        del request._serviceevents_method
        del request._serviceevents_path
        del request._serviceevents_endpoint

        response = MagicMock()
        response.status_code = 404

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.time"
        ) as mock_time:
            mock_time.perf_counter_ns.return_value = 1200000000
            _finalize_request(request, response, None)

        mock_ec.record_request.assert_called_once()
        call_kwargs = mock_ec.record_request.call_args[1]
        # Must be the collapsed first segment, NOT the full raw probed path.
        self.assertEqual(call_kwargs["route"], "/wp-admin")
        self.assertNotIn("setup-config.php", call_kwargs["route"])
        self.assertEqual(call_kwargs["status_code"], 404)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_matched_route_unaffected_by_sentinel(self, mock_clear):
        """A normally-resolved request still records its real route, not the sentinel."""
        mock_ec = MagicMock()
        django_mod._endpoint_collector = mock_ec

        request = self._make_request(method="GET", path="/users/42", route="users/<int:id>", view_name="user_detail")
        response = MagicMock()
        response.status_code = 200

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.time"
        ) as mock_time:
            mock_time.perf_counter_ns.return_value = 1200000000
            _finalize_request(request, response, None)

        call_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(call_kwargs["route"], "users/<int:id>")

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_passes_correct_request_data(self, mock_clear):
        """Verifies that request_data dict structure is correct for incident processing."""
        mock_ec = MagicMock()
        mock_isc = MagicMock()
        django_mod._endpoint_collector = mock_ec
        django_mod._incident_snapshot_collector = mock_isc

        request = self._make_request(method="POST", path="/api/items", route="api/items", view_name="create_item")
        request._serviceevents_trace_id = 99999
        request._serviceevents_span_id = 88888
        response = MagicMock()
        response.status_code = 500

        exc = RuntimeError("server error")

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.time"
        ) as mock_time:
            mock_time.perf_counter_ns.return_value = 1100000000
            _finalize_request(request, response, exc)

        call_kwargs = mock_isc.process_potential_incident.call_args[1]
        req_data = call_kwargs["request_data"]

        self.assertEqual(req_data["path"], "/api/items")
        self.assertEqual(req_data["endpoint"], "create_item")
        self.assertEqual(req_data["method"], "POST")
        self.assertEqual(req_data["trace_id"], 99999)
        self.assertEqual(req_data["span_id"], 88888)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_defaults_status_200_when_no_response_or_exception(self, mock_clear):
        """Status defaults to 200 when neither a response nor an exception is present."""
        mock_ec = MagicMock()
        django_mod._endpoint_collector = mock_ec

        request = self._make_request()

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.time"
        ) as mock_time:
            mock_time.perf_counter_ns.return_value = 1200000000
            _finalize_request(request, None, None)

        ec_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(ec_kwargs["status_code"], 200)
        self.assertIsNone(ec_kwargs["error_info"])

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.clear_current_operation")
    def test_incident_snapshot_error_is_swallowed(self, mock_clear):
        """An error from the incident snapshot collector is logged and does not propagate."""
        mock_ec = MagicMock()
        mock_isc = MagicMock()
        mock_isc.process_potential_incident.side_effect = Exception("snapshot boom")
        django_mod._endpoint_collector = mock_ec
        django_mod._incident_snapshot_collector = mock_isc

        request = self._make_request(method="POST", path="/api/orders", route="api/orders", view_name="create_order")
        response = MagicMock()
        response.status_code = 500

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.django_instrumentation.time"
        ) as mock_time:
            mock_time.perf_counter_ns.return_value = 1050000000
            # Must not raise despite the collector error.
            _finalize_request(request, response, RuntimeError("db down"))

        mock_isc.process_potential_incident.assert_called_once()
        # Operation context is still cleared.
        mock_clear.assert_called_once()
