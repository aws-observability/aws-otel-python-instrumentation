# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation as fastapi_mod
from amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation import (
    ServiceEventsFastAPIMiddleware,
    _get_request_body,
    _get_route_pattern,
    _parse_query_string,
    _resolve_route_template,
    install_fastapi_hooks,
)
from amazon.opentelemetry.distro.serviceevents.python_monitor import _ServiceEventsMonitorState


class TestGetRoutePattern(TestCase):
    """Tests for _get_route_pattern."""

    def test_get_route_pattern_from_scope(self):
        """Route pattern is extracted from scope route object's path attribute."""
        route_obj = MagicMock()
        route_obj.path = "/api/users/{id}"
        scope = {"route": route_obj, "path": "/api/users/42"}
        result = _get_route_pattern(scope)
        self.assertEqual(result, "/api/users/{id}")

    def test_get_route_pattern_unmatched_uses_first_segment(self):
        """When no route object is set and the template can't be pre-resolved (no app
        routes), collapse to the first path segment rather than the raw path, to bound
        metric cardinality for scanner/bot traffic. Parity with Flask/Django and
        Application Signals."""
        scope = {"path": "/api/users/42"}
        result = _get_route_pattern(scope)
        self.assertEqual(result, "/api")

    def test_get_route_pattern_empty_scope_uses_root(self):
        """An empty scope (no route, no path, no app) yields the root "/" label."""
        scope = {}
        result = _get_route_pattern(scope)
        self.assertEqual(result, "/")

    def test_get_route_pattern_route_without_path_attr_uses_first_segment(self):
        """A route object without a .path attribute (and no resolvable template) falls
        through to the first-segment unmatched label."""
        route_obj = "not-a-route-object"  # has no .path attribute
        scope = {"route": route_obj, "path": "/fallback"}
        result = _get_route_pattern(scope)
        self.assertEqual(result, "/fallback")

    def test_get_route_pattern_preresolves_template_before_routing(self):
        """When scope['route'] is unset, the template is resolved from the app's routes
        so the per-request operation keys on /users/{id}, not the raw /users/42."""
        from starlette.applications import Starlette  # pylint: disable=import-outside-toplevel
        from starlette.responses import PlainTextResponse  # pylint: disable=import-outside-toplevel
        from starlette.routing import Route  # pylint: disable=import-outside-toplevel

        async def handler(_request):
            return PlainTextResponse("ok")

        app = Starlette(routes=[Route("/users/{id}", handler)])
        # Scope as seen at middleware entry: app set, route NOT yet populated.
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/users/42",
            "app": app,
            "headers": [],
        }
        self.assertEqual(_get_route_pattern(scope), "/users/{id}")

    def test_resolve_route_template_returns_none_without_app(self):
        """No app in scope → no pre-resolution (graceful)."""
        self.assertIsNone(_resolve_route_template({"type": "http", "path": "/x"}))

    def test_resolve_route_template_returns_none_on_no_match(self):
        """A path that matches no route resolves to None (caller then uses the unmatched label)."""
        from starlette.applications import Starlette  # pylint: disable=import-outside-toplevel
        from starlette.responses import PlainTextResponse  # pylint: disable=import-outside-toplevel
        from starlette.routing import Route  # pylint: disable=import-outside-toplevel

        async def handler(_request):
            return PlainTextResponse("ok")

        app = Starlette(routes=[Route("/users/{id}", handler)])
        scope = {"type": "http", "method": "GET", "path": "/nope/here", "app": app, "headers": []}
        self.assertIsNone(_resolve_route_template(scope))

    def test_resolve_route_template_skips_route_missing_matches(self):
        """Routes without a matches() callable or path attribute are skipped."""
        bad_route = MagicMock()
        bad_route.matches = None
        bad_route.path = None
        app = MagicMock()
        app.routes = [bad_route]
        scope = {"type": "http", "method": "GET", "path": "/x", "app": app}
        self.assertIsNone(_resolve_route_template(scope))

    def test_resolve_route_template_returns_none_on_exception(self):
        """A route whose matches() raises is swallowed and resolves to None."""
        boom_route = MagicMock()
        boom_route.path = "/x/{id}"
        boom_route.matches.side_effect = RuntimeError("routing exploded")
        app = MagicMock()
        app.routes = [boom_route]
        scope = {"type": "http", "method": "GET", "path": "/x/1", "app": app}
        self.assertIsNone(_resolve_route_template(scope))


class TestParseQueryString(TestCase):
    """Tests for _parse_query_string."""

    def test_parse_query_string(self):
        """Single key-value pair is parsed correctly."""
        result = _parse_query_string(b"key=val")
        self.assertEqual(result, {"key": "val"})

    def test_parse_query_string_multi(self):
        """Multiple values for same key become a list."""
        result = _parse_query_string(b"k=a&k=b")
        self.assertEqual(result, {"k": ["a", "b"]})

    def test_parse_query_string_empty(self):
        """Empty bytes returns empty dict."""
        result = _parse_query_string(b"")
        self.assertEqual(result, {})

    def test_parse_query_string_none(self):
        """None input returns empty dict."""
        result = _parse_query_string(None)
        self.assertEqual(result, {})

    def test_parse_query_string_multiple_params(self):
        """Multiple different keys are parsed."""
        result = _parse_query_string(b"page=1&limit=20&sort=name")
        self.assertEqual(result, {"page": "1", "limit": "20", "sort": "name"})

    def test_parse_query_string_decode_error_returns_empty(self):
        """A query string whose decode raises is swallowed and yields an empty dict."""
        bad = MagicMock()
        bad.__bool__ = lambda self: True
        bad.decode.side_effect = UnicodeDecodeError("utf-8", b"", 0, 1, "bad")
        result = _parse_query_string(bad)
        self.assertEqual(result, {})


class TestGetRequestBody(unittest.IsolatedAsyncioTestCase):
    """Tests for the async _get_request_body fallback chain."""

    async def test_returns_json_body(self):
        """JSON body is returned when request.json() succeeds."""
        request = MagicMock()
        request.json = AsyncMock(return_value={"name": "test"})
        result = await _get_request_body(request)
        self.assertEqual(result, {"name": "test"})

    async def test_falls_back_to_form_data(self):
        """When JSON parsing fails, form data is returned as a dict."""
        request = MagicMock()
        request.json = AsyncMock(side_effect=ValueError("not json"))
        request.form = AsyncMock(return_value={"field": "value"})
        result = await _get_request_body(request)
        self.assertEqual(result, {"field": "value"})

    async def test_falls_back_to_raw_body(self):
        """When JSON and form fail, raw bytes are decoded to a string."""
        request = MagicMock()
        request.json = AsyncMock(side_effect=ValueError("not json"))
        request.form = AsyncMock(return_value={})
        request.body = AsyncMock(return_value=b"raw payload")
        result = await _get_request_body(request)
        self.assertEqual(result, "raw payload")

    async def test_raw_body_too_large(self):
        """Raw bodies over the 10KB limit are summarized, not decoded."""
        request = MagicMock()
        request.json = AsyncMock(side_effect=ValueError("not json"))
        request.form = AsyncMock(return_value={})
        big = b"x" * 10241
        request.body = AsyncMock(return_value=big)
        result = await _get_request_body(request)
        self.assertEqual(result, "<payload too large: 10241 bytes>")

    async def test_form_error_falls_through_to_raw_body(self):
        """A raised form() is swallowed and extraction continues to the raw body."""
        request = MagicMock()
        request.json = AsyncMock(side_effect=ValueError("not json"))
        request.form = AsyncMock(side_effect=RuntimeError("form exploded"))
        request.body = AsyncMock(return_value=b"raw")
        result = await _get_request_body(request)
        self.assertEqual(result, "raw")

    async def test_raw_body_error_returns_none(self):
        """A raised body() is swallowed and None is returned."""
        request = MagicMock()
        request.json = AsyncMock(side_effect=ValueError("not json"))
        request.form = AsyncMock(return_value={})
        request.body = AsyncMock(side_effect=RuntimeError("body exploded"))
        result = await _get_request_body(request)
        self.assertIsNone(result)

    async def test_returns_none_when_all_fail(self):
        """When every extraction path fails or is empty, None is returned."""
        request = MagicMock()
        request.json = AsyncMock(side_effect=ValueError("not json"))
        request.form = AsyncMock(return_value={})
        request.body = AsyncMock(return_value=b"")
        result = await _get_request_body(request)
        self.assertIsNone(result)


class TestInstallFastAPIHooks(TestCase):
    """Tests for install_fastapi_hooks."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        fastapi_mod._endpoint_collector = None
        fastapi_mod._incident_snapshot_collector = None
        fastapi_mod._serviceevents_config = None

    def test_install_stores_collectors(self):
        """Module globals are set after install_fastapi_hooks call."""
        mock_ec = MagicMock()
        mock_isc = MagicMock()
        mock_cfg = MagicMock()

        # Use a real class so __init__ can be patched
        class FakeFastAPI:
            def __init__(self, *args, **kwargs):
                pass

        mock_fastapi_module = MagicMock()
        mock_fastapi_module.FastAPI = FakeFastAPI

        with patch.dict("sys.modules", {"fastapi": mock_fastapi_module}):
            install_fastapi_hooks(
                endpoint_collector=mock_ec,
                incident_snapshot_collector=mock_isc,
                config=mock_cfg,
            )

        self.assertIs(fastapi_mod._endpoint_collector, mock_ec)
        self.assertIs(fastapi_mod._incident_snapshot_collector, mock_isc)
        self.assertIs(fastapi_mod._serviceevents_config, mock_cfg)

    def test_install_import_error(self):
        """Gracefully handles missing FastAPI."""
        import sys

        saved = sys.modules.get("fastapi")
        sys.modules["fastapi"] = None  # Force ImportError

        try:
            fastapi_mod._endpoint_collector = None
            install_fastapi_hooks(endpoint_collector=MagicMock())
            # Collectors should remain None since FastAPI import failed
            self.assertIsNone(fastapi_mod._endpoint_collector)
        finally:
            if saved is not None:
                sys.modules["fastapi"] = saved
            else:
                sys.modules.pop("fastapi", None)

    def test_install_patches_fastapi_init(self):
        """FastAPI.__init__ is replaced with instrumented version."""

        class FakeFastAPI:
            def __init__(self, *args, **kwargs):
                pass

        original_init = FakeFastAPI.__init__

        mock_fastapi_module = MagicMock()
        mock_fastapi_module.FastAPI = FakeFastAPI

        with patch.dict("sys.modules", {"fastapi": mock_fastapi_module}):
            install_fastapi_hooks()

        self.assertIsNot(FakeFastAPI.__init__, original_init)

    def test_instrumented_init_adds_middleware(self):
        """Instantiating a patched app calls original init and adds the middleware."""
        original_init_called = {}

        class FakeFastAPI:
            def __init__(self, *args, **kwargs):
                original_init_called["called"] = True
                self.title = "test-app"
                self.add_middleware = MagicMock()

        mock_fastapi_module = MagicMock()
        mock_fastapi_module.FastAPI = FakeFastAPI

        with patch.dict("sys.modules", {"fastapi": mock_fastapi_module}):
            install_fastapi_hooks()
            app = FakeFastAPI()

        self.assertTrue(original_init_called["called"])
        app.add_middleware.assert_called_once_with(ServiceEventsFastAPIMiddleware)

    def test_instrumented_init_swallows_middleware_install_failure(self):
        """Crash-safety: if add_middleware raises, app construction must still succeed.

        Telemetry must never break the host app. The original FastAPI __init__ must run
        and the constructed instance must be usable even when installing our middleware
        fails. The failure is swallowed (logged at debug), not propagated.
        """
        original_init_called = {}

        class FakeFastAPI:
            def __init__(self, *args, **kwargs):
                original_init_called["called"] = True
                self.title = "test-app"
                # add_middleware blows up the way a framework-internal failure would.
                self.add_middleware = MagicMock(side_effect=RuntimeError("middleware exploded"))

        mock_fastapi_module = MagicMock()
        mock_fastapi_module.FastAPI = FakeFastAPI

        with patch.dict("sys.modules", {"fastapi": mock_fastapi_module}):
            install_fastapi_hooks()
            # Must NOT raise even though add_middleware throws inside instrumented_init.
            app = FakeFastAPI()

        # Original init ran and the app was constructed despite the telemetry failure.
        self.assertTrue(original_init_called["called"])
        self.assertEqual(app.title, "test-app")
        app.add_middleware.assert_called_once_with(ServiceEventsFastAPIMiddleware)


class TestFastAPIMiddleware(unittest.IsolatedAsyncioTestCase):
    """Async tests for ServiceEventsFastAPIMiddleware."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        fastapi_mod._endpoint_collector = None
        fastapi_mod._incident_snapshot_collector = None
        fastapi_mod._serviceevents_config = None

    async def test_middleware_skips_non_http(self):
        """Websocket scope passes through to app without instrumentation."""
        app_called = False

        async def mock_app(scope, receive, send):
            nonlocal app_called
            app_called = True

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {"type": "websocket", "path": "/ws"}

        await middleware(scope, AsyncMock(), AsyncMock())
        self.assertTrue(app_called)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_middleware_records_endpoint_metric(self, mock_set_op, mock_clear_op):
        """Endpoint collector record_request is called with correct args."""
        mock_ec = MagicMock()
        fastapi_mod._endpoint_collector = mock_ec

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        # A genuinely-matched request carries a resolved route object, so the recorded
        # route is the template. (Unmatched requests now collapse to the first path
        # segment — covered by the _get_route_pattern tests.)
        route_obj = MagicMock()
        route_obj.path = "/api/users"
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/users",
            "route": route_obj,
            "headers": [],
            "query_string": b"",
        }

        sent_messages = []

        async def mock_send(msg):
            sent_messages.append(msg)

        await middleware(scope, AsyncMock(), mock_send)

        mock_ec.record_request.assert_called_once()
        call_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(call_kwargs["route"], "/api/users")
        self.assertEqual(call_kwargs["method"], "GET")
        self.assertEqual(call_kwargs["status_code"], 200)
        self.assertIsNone(call_kwargs["error_info"])
        mock_clear_op.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_middleware_processes_incident_on_500(self, mock_set_op, mock_clear_op):
        """Status 500 triggers incident snapshot processing."""
        mock_ec = MagicMock()
        mock_isc = MagicMock()
        fastapi_mod._endpoint_collector = mock_ec
        fastapi_mod._incident_snapshot_collector = mock_isc

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 500})
            await send({"type": "http.response.body", "body": b"error"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/api/orders",
            "headers": [],
            "query_string": b"",
        }

        await middleware(scope, AsyncMock(), AsyncMock())

        mock_isc.process_potential_incident.assert_called_once()
        call_kwargs = mock_isc.process_potential_incident.call_args[1]
        self.assertEqual(call_kwargs["status_code"], 500)

        # Error info should be passed to endpoint collector
        ec_kwargs = mock_ec.record_request.call_args[1]
        self.assertIsNotNone(ec_kwargs["error_info"])

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_middleware_handles_app_exception(self, mock_set_op, mock_clear_op):
        """App exception is caught, 500 is sent, and exception is re-raised."""
        mock_ec = MagicMock()
        fastapi_mod._endpoint_collector = mock_ec

        async def mock_app(scope, receive, send):
            raise RuntimeError("app crashed")

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/broken",
            "headers": [],
            "query_string": b"",
        }

        sent_messages = []

        async def mock_send(msg):
            sent_messages.append(msg)

        with self.assertRaises(RuntimeError):
            await middleware(scope, AsyncMock(), mock_send)

        # Should have sent 500 error response
        self.assertTrue(any(m.get("status") == 500 for m in sent_messages))

        # Endpoint collector should record 500
        ec_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(ec_kwargs["status_code"], 500)

        # Operation should still be cleared
        mock_clear_op.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_middleware_send_failure_does_not_mask_app_exception(self, mock_set_op, mock_clear_op):
        """If the error-response send() raises, the ORIGINAL app exception must still propagate.

        Crash-safety: a client disconnect / broken pipe during our best-effort 500 send must
        not replace the application's own exception with the send error — telemetry must never
        alter which exception the host application sees.
        """
        fastapi_mod._endpoint_collector = MagicMock()

        async def mock_app(scope, receive, send):
            raise RuntimeError("original app error")

        async def mock_send(msg):
            # Simulate the client connection dropping while we try to emit the 500.
            raise OSError("broken pipe")

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/broken",
            "headers": [],
            "query_string": b"",
        }

        # The original RuntimeError must surface, NOT the OSError from send().
        with self.assertRaises(RuntimeError) as ctx:
            await middleware(scope, AsyncMock(), mock_send)
        self.assertEqual(str(ctx.exception), "original app error")

        # Operation context is still cleared despite the send failure.
        mock_clear_op.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_middleware_clears_operation(self, mock_set_op, mock_clear_op):
        """clear_current_operation is always called in the finally block."""

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/test",
            "headers": [],
            "query_string": b"",
        }

        await middleware(scope, AsyncMock(), AsyncMock())

        mock_set_op.assert_called_once()
        mock_clear_op.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_send_wrapper_captures_status_code(self, mock_set_op, mock_clear_op):
        """Status code is extracted from http.response.start message."""
        mock_ec = MagicMock()
        fastapi_mod._endpoint_collector = mock_ec

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 201})
            await send({"type": "http.response.body", "body": b"created"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/api/items",
            "headers": [],
            "query_string": b"",
        }

        await middleware(scope, AsyncMock(), AsyncMock())

        ec_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(ec_kwargs["status_code"], 201)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_middleware_skips_filtered_endpoint(self, mock_set_op, mock_clear_op):
        """Middleware passes through when config filters out the endpoint."""
        mock_config = MagicMock()
        mock_config.should_track_endpoint.return_value = False
        fastapi_mod._serviceevents_config = mock_config

        app_called = False

        async def mock_app(scope, receive, send):
            nonlocal app_called
            app_called = True
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/health",
            "headers": [],
            "query_string": b"",
        }

        await middleware(scope, AsyncMock(), AsyncMock())

        self.assertTrue(app_called)
        # set_current_operation should NOT have been called
        mock_set_op.assert_not_called()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_middleware_parses_headers_and_query(self, mock_set_op, mock_clear_op):
        """Incident snapshot receives parsed headers and query params."""
        mock_isc = MagicMock()
        fastapi_mod._incident_snapshot_collector = mock_isc

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 500})
            await send({"type": "http.response.body", "body": b"error"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/search",
            "headers": [(b"content-type", b"application/json"), (b"x-request-id", b"abc123")],
            "query_string": b"q=test&page=1",
            "path_params": {"id": "42"},
        }

        await middleware(scope, AsyncMock(), AsyncMock())

        call_kwargs = mock_isc.process_potential_incident.call_args[1]
        req_data = call_kwargs["request_data"]
        self.assertEqual(req_data["headers"]["content-type"], "application/json")
        self.assertEqual(req_data["args"]["q"], "test")
        self.assertEqual(req_data["args"]["page"], "1")

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_middleware_caches_request_body(self, mock_set_op, mock_clear_op):
        """Request body is captured via receive_wrapper and passed to incident collector."""
        mock_isc = MagicMock()
        fastapi_mod._incident_snapshot_collector = mock_isc

        body_content = b'{"name": "test"}'

        async def mock_app(scope, receive, send):
            # App reads the body
            await receive()
            await send({"type": "http.response.start", "status": 500})
            await send({"type": "http.response.body", "body": b"error"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/api/items",
            "headers": [],
            "query_string": b"",
        }

        call_count = 0

        async def mock_receive():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"type": "http.request", "body": body_content, "more_body": False}
            return {"type": "http.disconnect"}

        await middleware(scope, mock_receive, AsyncMock())

        call_kwargs = mock_isc.process_potential_incident.call_args[1]
        req_data = call_kwargs["request_data"]
        self.assertIn("cached_body", req_data)
        self.assertIn("test", req_data["cached_body"])


class TestFastAPIMiddlewareRouteResolution(unittest.IsolatedAsyncioTestCase):
    """The exported endpoint telemetry must use the route template, not the raw path.

    At middleware entry scope["route"] is unset, so the entry-time value is the raw
    URL. Starlette/FastAPI routing populates scope["route"] in place during dispatch,
    so the middleware re-resolves it in the finally block before recording metrics.
    """

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        fastapi_mod._endpoint_collector = None
        fastapi_mod._incident_snapshot_collector = None
        fastapi_mod._serviceevents_config = None

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_route_template_used_after_routing(self, mock_set_op, mock_clear_op):
        """record_request gets the template (/users/{id}) once routing populates scope."""
        mock_ec = MagicMock()
        fastapi_mod._endpoint_collector = mock_ec

        route_obj = MagicMock()
        route_obj.path = "/users/{id}"

        async def mock_app(scope, receive, send):
            # Simulate FastAPI routing mutating the scope dict in place during dispatch.
            scope["route"] = route_obj
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        # Raw path as the client hit it — no "route" key yet at entry.
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/users/42",
            "headers": [],
            "query_string": b"",
        }

        await middleware(scope, AsyncMock(), AsyncMock())

        call_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(call_kwargs["route"], "/users/{id}")

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_route_template_used_on_exception_path(self, mock_set_op, mock_clear_op):
        """Even when the view raises, the re-resolved template is used for telemetry."""
        mock_ec = MagicMock()
        fastapi_mod._endpoint_collector = mock_ec

        route_obj = MagicMock()
        route_obj.path = "/boom/{id}"

        async def mock_app(scope, receive, send):
            scope["route"] = route_obj  # routing resolved before the handler raised
            raise RuntimeError("app crashed")

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/boom/7",
            "headers": [],
            "query_string": b"",
        }

        with self.assertRaises(RuntimeError):
            await middleware(scope, AsyncMock(), AsyncMock())

        call_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(call_kwargs["route"], "/boom/{id}")
        self.assertEqual(call_kwargs["status_code"], 500)


class TestFastAPIMiddlewareTraceFallback(unittest.IsolatedAsyncioTestCase):
    """On the exception path, http.response.start never fires, so send_wrapper never
    captures trace correlation. The finally recovers it from the active OTel span so
    IncidentSnapshot stays joinable on the error path.
    """

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        fastapi_mod._endpoint_collector = None
        fastapi_mod._incident_snapshot_collector = None
        fastapi_mod._serviceevents_config = None

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_trace_recovered_from_active_span_on_exception(self, mock_set_op, mock_clear_op):
        """When the app raises before responding, trace_id/span_id come from the live span."""
        mock_isc = MagicMock()
        fastapi_mod._incident_snapshot_collector = mock_isc

        async def mock_app(scope, receive, send):
            # No http.response.start — exception before any response is sent.
            raise RuntimeError("app crashed")

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/broken",
            "headers": [],
            "query_string": b"",
        }

        # A valid span still active in the finally block (OTel-outer nesting).
        mock_span = MagicMock()
        span_ctx = MagicMock()
        span_ctx.is_valid = True
        span_ctx.trace_id = 0x0123456789ABCDEF0123456789ABCDEF
        span_ctx.span_id = 0x0123456789ABCDEF
        mock_span.get_span_context.return_value = span_ctx

        with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
            with self.assertRaises(RuntimeError):
                await middleware(scope, AsyncMock(), AsyncMock())

        call_kwargs = mock_isc.process_potential_incident.call_args[1]
        req_data = call_kwargs["request_data"]
        self.assertEqual(req_data["trace_id"], 0x0123456789ABCDEF0123456789ABCDEF)
        self.assertEqual(req_data["span_id"], 0x0123456789ABCDEF)


class TestFastAPIMiddlewareCrashSafety(unittest.IsolatedAsyncioTestCase):
    """The middleware must never stop a request when telemetry setup fails."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        fastapi_mod._endpoint_collector = None
        fastapi_mod._incident_snapshot_collector = None
        fastapi_mod._serviceevents_config = None

    @patch(
        "amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation",
        side_effect=RuntimeError("telemetry exploded"),
    )
    async def test_pre_await_failure_still_dispatches_app(self, _mock_set_op):
        """If pre-await telemetry setup raises, the app must still be invoked and served."""
        app_called = False

        async def mock_app(scope, receive, send):
            nonlocal app_called
            app_called = True
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/users",
            "headers": [],
            "query_string": b"",
        }

        sent = []

        async def mock_send(msg):
            sent.append(msg)

        # Must not raise; the app must still run.
        await middleware(scope, AsyncMock(), mock_send)
        self.assertTrue(app_called)
        self.assertEqual(sent[0]["status"], 200)


class TestFastAPIMiddlewareErrorBranches(unittest.IsolatedAsyncioTestCase):
    """Telemetry failures inside the request lifecycle must be swallowed, never raised."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        fastapi_mod._endpoint_collector = None
        fastapi_mod._incident_snapshot_collector = None
        fastapi_mod._serviceevents_config = None

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_receive_wrapper_swallows_caching_error(self, mock_set_op, mock_clear_op):
        """A malformed receive message must still be returned despite a caching failure."""
        mock_isc = MagicMock()
        fastapi_mod._incident_snapshot_collector = mock_isc

        # An object whose __getitem__ raises, so message["type"] inside the try blows up.
        class BadMessage:
            def __getitem__(self, key):
                raise KeyError("no type")

        bad_message = BadMessage()

        received = []

        async def mock_app(scope, receive, send):
            received.append(await receive())
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})

        async def mock_receive():
            return bad_message

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/users",
            "headers": [],
            "query_string": b"",
        }

        await middleware(scope, mock_receive, AsyncMock())

        # The original message is returned to the app unchanged.
        self.assertIs(received[0], bad_message)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_send_wrapper_captures_trace_on_success(self, mock_set_op, mock_clear_op):
        """A valid active span at http.response.start populates trace correlation."""
        mock_isc = MagicMock()
        fastapi_mod._incident_snapshot_collector = mock_isc

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 500})
            await send({"type": "http.response.body", "body": b"err"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/users",
            "headers": [],
            "query_string": b"",
        }

        mock_span = MagicMock()
        span_ctx = MagicMock()
        span_ctx.is_valid = True
        span_ctx.trace_id = 0xAABBCCDDEEFF00112233445566778899
        span_ctx.span_id = 0xAABBCCDDEEFF0011
        mock_span.get_span_context.return_value = span_ctx

        with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
            await middleware(scope, AsyncMock(), AsyncMock())

        req_data = mock_isc.process_potential_incident.call_args[1]["request_data"]
        self.assertEqual(req_data["trace_id"], 0xAABBCCDDEEFF00112233445566778899)
        self.assertEqual(req_data["span_id"], 0xAABBCCDDEEFF0011)

    @patch.object(fastapi_mod, "_extract_error_from_call_path")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_error_info_extraction_failure_swallowed(self, mock_set_op, mock_clear_op, mock_extract):
        """A failure extracting error info on a 4xx/5xx must not break the request."""
        mock_extract.side_effect = RuntimeError("extract exploded")
        mock_ec = MagicMock()
        fastapi_mod._endpoint_collector = mock_ec

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 500})
            await send({"type": "http.response.body", "body": b"err"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/fail",
            "headers": [],
            "query_string": b"",
        }

        await middleware(scope, AsyncMock(), AsyncMock())

        # error_info stays None because extraction failed, but the metric is still recorded.
        ec_kwargs = mock_ec.record_request.call_args[1]
        self.assertIsNone(ec_kwargs["error_info"])

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_record_request_failure_swallowed(self, mock_set_op, mock_clear_op):
        """An exception from record_request is logged, not propagated."""
        mock_ec = MagicMock()
        mock_ec.record_request.side_effect = RuntimeError("collector exploded")
        fastapi_mod._endpoint_collector = mock_ec

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"ok"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/users",
            "headers": [],
            "query_string": b"",
        }

        # Must not raise.
        await middleware(scope, AsyncMock(), AsyncMock())
        mock_ec.record_request.assert_called_once()
        mock_clear_op.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.clear_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.fastapi_instrumentation.set_current_operation")
    async def test_process_incident_failure_swallowed(self, mock_set_op, mock_clear_op):
        """An exception from process_potential_incident is logged, not propagated."""
        mock_isc = MagicMock()
        mock_isc.process_potential_incident.side_effect = RuntimeError("incident exploded")
        fastapi_mod._incident_snapshot_collector = mock_isc

        async def mock_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 500})
            await send({"type": "http.response.body", "body": b"err"})

        middleware = ServiceEventsFastAPIMiddleware(mock_app)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/users",
            "headers": [],
            "query_string": b"",
        }

        # Must not raise.
        await middleware(scope, AsyncMock(), AsyncMock())
        mock_isc.process_potential_incident.assert_called_once()
        mock_clear_op.assert_called_once()
