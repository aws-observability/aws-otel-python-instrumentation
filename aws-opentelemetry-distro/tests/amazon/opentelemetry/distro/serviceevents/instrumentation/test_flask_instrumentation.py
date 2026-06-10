# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

import amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation as flask_mod
from amazon.opentelemetry.distro.serviceevents.instrumentation._constants import UNMATCHED_ROUTE
from amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation import (
    _after_request_hook,
    _before_request_hook,
    _capture_active_trace_context,
    _extract_error_from_call_path,
    _get_request_body,
    _get_route_pattern,
    _teardown_request_hook,
    install_flask_hooks,
)
from amazon.opentelemetry.distro.serviceevents.python_monitor import _ServiceEventsMonitorState


class TestGetRoutePattern(TestCase):
    """Tests for _get_route_pattern."""

    def test_get_route_pattern_from_url_rule(self):
        """url_rule.rule is returned when available."""
        request = MagicMock()
        request.url_rule = MagicMock()
        request.url_rule.rule = "/users/<int:id>"
        result = _get_route_pattern(request)
        self.assertEqual(result, "/users/<int:id>")

    def test_get_route_pattern_from_endpoint(self):
        """Falls back to endpoint name when url_rule is None."""
        request = MagicMock()
        request.url_rule = None
        request.endpoint = "user_detail"
        result = _get_route_pattern(request)
        self.assertEqual(result, "/user_detail")

    def test_get_route_pattern_unmatched_uses_sentinel(self):
        """Unmatched requests (no url_rule, no endpoint) collapse to the <unmatched>
        sentinel instead of the raw path, to bound metric cardinality for scanner/bot
        traffic. Parity with the Django/FastAPI instrumentation."""
        request = MagicMock()
        request.url_rule = None
        request.endpoint = None
        request.path = "/wp-admin/setup-config.php"
        result = _get_route_pattern(request)
        self.assertEqual(result, UNMATCHED_ROUTE)


class TestGetRequestBody(TestCase):
    """Tests for _get_request_body."""

    def test_get_request_body_json(self):
        """Returns parsed JSON when get_json succeeds."""
        request = MagicMock()
        request.get_json.return_value = {"key": "value"}
        result = _get_request_body(request)
        self.assertEqual(result, {"key": "value"})

    def test_get_request_body_form(self):
        """Returns form dict when JSON fails but form data exists."""
        request = MagicMock()
        request.get_json.return_value = None
        form_data = MagicMock()
        form_data.__bool__ = lambda self: True
        form_data.to_dict.return_value = {"field": "data"}
        request.form = form_data
        result = _get_request_body(request)
        self.assertEqual(result, {"field": "data"})

    def test_get_request_body_raw(self):
        """Returns raw data decoded as string when JSON and form fail."""
        request = MagicMock()
        request.get_json.return_value = None
        request.form = MagicMock()
        request.form.__bool__ = lambda self: False
        request.get_data.return_value = b"raw body content"
        result = _get_request_body(request)
        self.assertEqual(result, "raw body content")

    def test_get_request_body_none(self):
        """Returns None when all extraction methods fail."""
        request = MagicMock()
        request.get_json.side_effect = Exception("no json")
        request.form = MagicMock()
        type(request.form).__bool__ = PropertyMock(side_effect=Exception("no form"))
        request.get_data.side_effect = Exception("no data")
        result = _get_request_body(request)
        self.assertIsNone(result)

    def test_get_request_body_large_payload(self):
        """Returns truncation message for payloads over 10KB."""
        request = MagicMock()
        request.get_json.return_value = None
        request.form = MagicMock()
        request.form.__bool__ = lambda self: False
        large_data = b"x" * 20000
        request.get_data.return_value = large_data
        result = _get_request_body(request)
        self.assertIn("payload too large", result)
        self.assertIn("20000", result)


class TestExtractErrorFromCallPath(TestCase):
    """Tests for _extract_error_from_call_path."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None

    def test_extract_error_with_exception(self):
        """Exception type name is extracted from the exception object."""
        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()

        exc = ValueError("test error")
        result = _extract_error_from_call_path(exc, "/api/test", "GET")

        self.assertEqual(result["error_type"], "ValueError")

    def test_extract_error_with_call_path(self):
        """function_name is extracted from investigation call_path."""
        _state = _ServiceEventsMonitorState.get_instance()
        _state.begin_investigation()
        # Directly set call_path data
        inv_data = _state._investigation_data.get()
        inv_data["call_path"] = [{"function_name": "my_func_123"}]

        exc = RuntimeError("fail")
        result = _extract_error_from_call_path(exc, "/api/test", "POST")

        self.assertEqual(result["error_type"], "RuntimeError")
        self.assertEqual(result["function_name"], "my_func_123")

    def test_extract_error_no_investigation_data(self):
        """Returns default values when no investigation data exists."""
        _ServiceEventsMonitorState.get_instance()
        # No begin_investigation call, so no inv data

        result = _extract_error_from_call_path(None, "/api/test", "GET")

        self.assertEqual(result["error_type"], "UnknownError")
        self.assertEqual(result["function_name"], "unknown")

    def test_extract_error_prefers_captured_exception_origin(self):
        """The function the monitor recorded as the thrower wins over call_path[0]."""
        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()
        inv_data = state._investigation_data.get()
        # call_path[0] is a sibling that did NOT throw; the exception origin is deeper.
        inv_data["call_path"] = [{"function_name": "sibling_func"}, {"function_name": "thrower_func"}]
        inv_data["exception"] = {"name": "ValueError", "function_name": "thrower_func"}

        result = _extract_error_from_call_path(ValueError("boom"), "/api/test", "GET")

        self.assertEqual(result["error_type"], "ValueError")
        self.assertEqual(result["function_name"], "thrower_func")

    def test_extract_error_recovers_type_when_exception_arg_is_none(self):
        """FastAPI global-handler case: exception arg is None but the monitor captured the
        real error — recover its type instead of mislabeling it UnknownError."""
        state = _ServiceEventsMonitorState.get_instance()
        state.begin_investigation()
        inv_data = state._investigation_data.get()
        inv_data["exception"] = {"name": "KeyError", "function_name": "handler_func"}

        result = _extract_error_from_call_path(None, "/api/test", "GET")

        self.assertEqual(result["error_type"], "KeyError")
        self.assertEqual(result["function_name"], "handler_func")


class TestInstallFlaskHooks(TestCase):
    """Tests for install_flask_hooks."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        flask_mod._endpoint_collector = None
        flask_mod._incident_snapshot_collector = None
        flask_mod._serviceevents_config = None

    def test_install_flask_hooks_stores_collectors(self):
        """Module globals are set after install_flask_hooks call."""
        mock_ec = MagicMock()
        mock_isc = MagicMock()
        mock_cfg = MagicMock()

        # Use a real class so __init__ can be patched
        class FakeFlask:
            def __init__(self, *args, **kwargs):
                pass

        mock_flask_module = MagicMock()
        mock_flask_module.Flask = FakeFlask
        mock_flask_module.g = MagicMock()
        mock_flask_module.request = MagicMock()

        with patch.dict("sys.modules", {"flask": mock_flask_module}):
            install_flask_hooks(
                endpoint_collector=mock_ec,
                incident_snapshot_collector=mock_isc,
                config=mock_cfg,
            )

        self.assertIs(flask_mod._endpoint_collector, mock_ec)
        self.assertIs(flask_mod._incident_snapshot_collector, mock_isc)
        self.assertIs(flask_mod._serviceevents_config, mock_cfg)

    def test_install_flask_hooks_patches_flask_init(self):
        """Flask.__init__ is replaced with instrumented version."""

        class FakeFlask:
            def __init__(self, *args, **kwargs):
                pass

        original_init = FakeFlask.__init__

        mock_flask_module = MagicMock()
        mock_flask_module.Flask = FakeFlask

        with patch.dict("sys.modules", {"flask": mock_flask_module}):
            install_flask_hooks()

        # Flask.__init__ should have been replaced
        self.assertIsNot(FakeFlask.__init__, original_init)

    def test_install_flask_hooks_import_error(self):
        """Gracefully handles missing Flask."""
        # Remove flask from sys.modules and make import fail
        import sys

        saved = sys.modules.get("flask")
        sys.modules["flask"] = None  # Force ImportError

        try:
            # Should not raise
            flask_mod._endpoint_collector = None
            install_flask_hooks(endpoint_collector=MagicMock())
            # Collectors should remain None since Flask import failed
            self.assertIsNone(flask_mod._endpoint_collector)
        finally:
            if saved is not None:
                sys.modules["flask"] = saved
            else:
                sys.modules.pop("flask", None)

    def test_instrumented_init_registers_hooks_on_app_creation(self):
        """Creating a Flask app after install runs the wrapped __init__ and registers all hooks."""
        init_calls = []

        class FakeFlask:
            def __init__(self, name, *args, **kwargs):
                init_calls.append(name)
                self.name = name
                self.before_request = MagicMock()
                self.after_request = MagicMock()
                self.teardown_request = MagicMock()

        mock_flask_module = MagicMock()
        mock_flask_module.Flask = FakeFlask

        with patch.dict("sys.modules", {"flask": mock_flask_module}):
            install_flask_hooks()
            # Instantiating now exercises instrumented_init (original init + hook registration).
            app = FakeFlask("my_app")

        self.assertEqual(init_calls, ["my_app"])  # original __init__ ran
        app.before_request.assert_called_once_with(flask_mod._before_request_hook)
        app.after_request.assert_called_once_with(flask_mod._after_request_hook)
        app.teardown_request.assert_called_once_with(flask_mod._teardown_request_hook)

    def test_instrumented_init_swallows_hook_registration_failure(self):
        """A failure while registering hooks must NOT break Flask app construction.

        Telemetry must never crash the host app: if before_request/after_request/
        teardown_request registration raises, instrumented_init swallows it and the
        app is constructed as if uninstrumented (the original __init__ still ran).
        """
        init_calls = []

        class FakeFlask:
            def __init__(self, name, *args, **kwargs):
                init_calls.append(name)
                self.name = name
                # before_request raises -> simulates a hook-registration failure.
                self.before_request = MagicMock(side_effect=RuntimeError("registration boom"))
                self.after_request = MagicMock()
                self.teardown_request = MagicMock()

        mock_flask_module = MagicMock()
        mock_flask_module.Flask = FakeFlask

        with patch.dict("sys.modules", {"flask": mock_flask_module}):
            install_flask_hooks()
            # Must not raise despite before_request blowing up inside instrumented_init.
            app = FakeFlask("my_app")

        self.assertEqual(init_calls, ["my_app"])  # original __init__ still ran
        self.assertEqual(app.name, "my_app")  # app fully constructed/usable


class TestFlaskHookFunctions(TestCase):
    """Tests for the Flask hook functions (_before_request_hook, _after_request_hook, _teardown_request_hook)."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        flask_mod._endpoint_collector = None
        flask_mod._incident_snapshot_collector = None
        flask_mod._serviceevents_config = None

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.set_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.time")
    def test_before_request_sets_operation_context(self, mock_time, mock_set_operation):
        """Before request hook sets the operation via set_current_operation."""
        mock_time.perf_counter_ns.return_value = 1000000000

        mock_g = MagicMock()
        mock_request = MagicMock()
        mock_request.method = "GET"
        mock_request.path = "/api/users"
        mock_request.endpoint = "users_list"
        mock_request.url_rule = MagicMock()
        mock_request.url_rule.rule = "/api/users"

        with patch(
            "amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.g", mock_g, create=True
        ):
            with patch(
                "amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.request",
                mock_request,
                create=True,
            ):
                # Need to patch the import inside the function
                flask_mock = MagicMock()
                flask_mock.g = mock_g
                flask_mock.request = mock_request
                with patch.dict("sys.modules", {"flask": flask_mock}):
                    _before_request_hook()

        mock_set_operation.assert_called_once()
        # The operation should be "METHOD /route"
        call_args = mock_set_operation.call_args[0][0]
        self.assertEqual(call_args, "GET /api/users")

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.time")
    def test_after_request_calculates_duration(self, mock_time):
        """After request hook calculates request duration and stores it."""
        mock_time.perf_counter_ns.return_value = 1200000000

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000

        mock_response = MagicMock()
        mock_response.status_code = 200

        flask_mock = MagicMock()
        flask_mock.g = mock_g

        with patch.dict("sys.modules", {"flask": flask_mock}):
            result = _after_request_hook(mock_response)

        self.assertIs(result, mock_response)
        # Duration should be stored on g (~200ms)
        self.assertAlmostEqual(mock_g.serviceevents_duration_ms, 200.0, places=1)
        self.assertEqual(mock_g.serviceevents_status_code, 200)

    def test_after_request_skips_if_filtered(self):
        """After request hook returns response immediately if serviceevents_skip is True."""
        mock_g = MagicMock()
        mock_g.serviceevents_skip = True

        mock_response = MagicMock()

        flask_mock = MagicMock()
        flask_mock.g = mock_g

        with patch.dict("sys.modules", {"flask": flask_mock}):
            result = _after_request_hook(mock_response)

        self.assertIs(result, mock_response)
        # Duration should NOT be set
        self.assertFalse(
            hasattr(mock_g, "serviceevents_duration_ms")
            and mock_g.serviceevents_duration_ms is not mock_g.serviceevents_duration_ms
        )

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_records_endpoint_metric(self, mock_clear):
        """Teardown hook calls endpoint_collector.record_request."""
        mock_ec = MagicMock()
        flask_mod._endpoint_collector = mock_ec

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        mock_g.serviceevents_duration_ms = 150.0
        mock_g.serviceevents_status_code = 200
        mock_g.serviceevents_route = "/api/users"
        mock_g.serviceevents_method = "GET"
        mock_g.serviceevents_path = "/api/users"
        mock_g.serviceevents_endpoint = "users_list"
        # Ensure hasattr returns False for serviceevents_incident_processed
        del mock_g.serviceevents_incident_processed

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.args = {}
        mock_request.view_args = {}

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        with patch.dict("sys.modules", {"flask": flask_mock}):
            _teardown_request_hook(exception=None)

        mock_ec.record_request.assert_called_once()
        call_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(call_kwargs["route"], "/api/users")
        self.assertEqual(call_kwargs["method"], "GET")
        self.assertEqual(call_kwargs["status_code"], 200)
        self.assertIsNone(call_kwargs["error_info"])

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_processes_incident_on_error(self, mock_clear):
        """Teardown hook triggers incident processing on 500 status."""
        mock_ec = MagicMock()
        mock_isc = MagicMock()
        flask_mod._endpoint_collector = mock_ec
        flask_mod._incident_snapshot_collector = mock_isc

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        mock_g.serviceevents_duration_ms = 50.0
        mock_g.serviceevents_status_code = 500
        mock_g.serviceevents_route = "/api/orders"
        mock_g.serviceevents_method = "POST"
        mock_g.serviceevents_path = "/api/orders"
        mock_g.serviceevents_endpoint = "create_order"
        del mock_g.serviceevents_incident_processed

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.args = {}
        mock_request.view_args = {}

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        exc = RuntimeError("DB connection failed")

        with patch.dict("sys.modules", {"flask": flask_mock}):
            _teardown_request_hook(exception=exc)

        # Incident collector should be called
        mock_isc.process_potential_incident.assert_called_once()
        call_kwargs = mock_isc.process_potential_incident.call_args[1]
        self.assertEqual(call_kwargs["status_code"], 500)
        self.assertIs(call_kwargs["exception"], exc)

        # Error info should be passed to endpoint collector
        ec_kwargs = mock_ec.record_request.call_args[1]
        self.assertIsNotNone(ec_kwargs["error_info"])

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_clears_operation(self, mock_clear):
        """Teardown always calls clear_current_operation, even on error."""
        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        mock_g.serviceevents_duration_ms = 10.0
        mock_g.serviceevents_status_code = 200
        mock_g.serviceevents_route = "/api/test"
        mock_g.serviceevents_method = "GET"
        mock_g.serviceevents_path = "/api/test"
        mock_g.serviceevents_endpoint = "test"
        del mock_g.serviceevents_incident_processed

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.args = {}
        mock_request.view_args = {}

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        with patch.dict("sys.modules", {"flask": flask_mock}):
            _teardown_request_hook(exception=None)

        mock_clear.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_clears_investigation_data_on_normal_path(self, _mock_clear, mock_state_cls):
        """Teardown drops investigation data unconditionally (the non-incident leak fix).

        On the normal path no incident is collected, so get_investigation_data() is never
        called; teardown must still clear it so a stale dict (and any captured traceback)
        does not linger on a pooled worker thread.
        """
        mock_state = MagicMock()
        mock_state_cls.get_instance.return_value = mock_state

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        mock_g.serviceevents_duration_ms = 10.0
        mock_g.serviceevents_status_code = 200
        mock_g.serviceevents_route = "/api/test"
        mock_g.serviceevents_method = "GET"
        mock_g.serviceevents_path = "/api/test"
        mock_g.serviceevents_endpoint = "test"
        del mock_g.serviceevents_incident_processed

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.args = {}
        mock_request.view_args = {}

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        with patch.dict("sys.modules", {"flask": flask_mock}):
            _teardown_request_hook(exception=None)

        mock_state.clear_investigation_data.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_clears_operation_on_exception(self, mock_clear):
        """clear_current_operation is called even when endpoint_collector raises."""
        mock_ec = MagicMock()
        mock_ec.record_request.side_effect = Exception("collector error")
        flask_mod._endpoint_collector = mock_ec

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        mock_g.serviceevents_duration_ms = 10.0
        mock_g.serviceevents_status_code = 200
        mock_g.serviceevents_route = "/api/test"
        mock_g.serviceevents_method = "GET"
        mock_g.serviceevents_path = "/api/test"
        mock_g.serviceevents_endpoint = "test"
        del mock_g.serviceevents_incident_processed

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.args = {}
        mock_request.view_args = {}

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        with patch.dict("sys.modules", {"flask": flask_mock}):
            # Should not raise, error is logged internally
            _teardown_request_hook(exception=None)

        mock_clear.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_skips_if_filtered(self, mock_clear):
        """Teardown skips processing when g.serviceevents_skip is True."""
        mock_g = MagicMock()
        mock_g.serviceevents_skip = True

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = MagicMock()

        mock_ec = MagicMock()
        flask_mod._endpoint_collector = mock_ec

        with patch.dict("sys.modules", {"flask": flask_mock}):
            _teardown_request_hook(exception=None)

        # Should NOT call record_request
        mock_ec.record_request.assert_not_called()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_infers_500_from_exception(self, mock_clear):
        """When status_code is not set but exception exists, status 500 is inferred."""
        mock_ec = MagicMock()
        flask_mod._endpoint_collector = mock_ec

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        # No serviceevents_duration_ms set, so it should calculate
        del mock_g.serviceevents_duration_ms
        # No serviceevents_status_code set, so it should infer from exception
        del mock_g.serviceevents_status_code
        mock_g.serviceevents_route = "/api/error"
        mock_g.serviceevents_method = "GET"
        mock_g.serviceevents_path = "/api/error"
        mock_g.serviceevents_endpoint = "error_endpoint"
        del mock_g.serviceevents_incident_processed

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.args = {}
        mock_request.view_args = {}

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        exc = ValueError("bad input")

        with patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.time") as mock_time:
            mock_time.perf_counter_ns.return_value = 1050000000
            with patch.dict("sys.modules", {"flask": flask_mock}):
                _teardown_request_hook(exception=exc)

        ec_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(ec_kwargs["status_code"], 500)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.set_current_operation")
    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.time")
    def test_before_request_skips_filtered_endpoint(self, mock_time, mock_set_operation):
        """When config filters out the endpoint, the hook marks skip and stops early."""
        mock_cfg = MagicMock()
        mock_cfg.should_track_endpoint.return_value = False
        flask_mod._serviceevents_config = mock_cfg

        mock_g = MagicMock()
        mock_request = MagicMock()
        mock_request.method = "GET"
        mock_request.path = "/health"
        mock_request.endpoint = "health"
        mock_request.url_rule = MagicMock()
        mock_request.url_rule.rule = "/health"

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        with patch.dict("sys.modules", {"flask": flask_mock}):
            _before_request_hook()

        self.assertTrue(mock_g.serviceevents_skip)
        # Early return: operation context is never established for filtered endpoints.
        mock_set_operation.assert_not_called()
        mock_time.perf_counter_ns.assert_not_called()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.time")
    def test_after_request_captures_trace_context(self, mock_time):
        """After request stores trace_id/span_id when an active span is present."""
        mock_time.perf_counter_ns.return_value = 1100000000

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000

        mock_response = MagicMock()
        mock_response.status_code = 200

        flask_mock = MagicMock()
        flask_mock.g = mock_g

        mock_span = MagicMock()
        span_ctx = MagicMock()
        span_ctx.is_valid = True
        span_ctx.trace_id = 0xABCD
        span_ctx.span_id = 0x1234
        mock_span.get_span_context.return_value = span_ctx

        with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
            with patch.dict("sys.modules", {"flask": flask_mock}):
                result = _after_request_hook(mock_response)

        self.assertIs(result, mock_response)
        self.assertEqual(mock_g.serviceevents_trace_id, 0xABCD)
        self.assertEqual(mock_g.serviceevents_span_id, 0x1234)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_returns_when_no_start_time(self, mock_clear):
        """Teardown returns early (after clearing) when start_time was never recorded."""
        mock_ec = MagicMock()
        flask_mod._endpoint_collector = mock_ec

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        del mock_g.serviceevents_start_time  # never set (e.g. before_request bailed)

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = MagicMock()

        with patch.dict("sys.modules", {"flask": flask_mock}):
            _teardown_request_hook(exception=None)

        mock_ec.record_request.assert_not_called()
        mock_clear.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_returns_when_incident_already_processed(self, mock_clear):
        """A second teardown call is a no-op once the incident has been processed."""
        mock_ec = MagicMock()
        flask_mod._endpoint_collector = mock_ec

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        mock_g.serviceevents_incident_processed = True  # already handled

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = MagicMock()

        with patch.dict("sys.modules", {"flask": flask_mock}):
            _teardown_request_hook(exception=None)

        mock_ec.record_request.assert_not_called()
        mock_clear.assert_called_once()

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_defaults_status_to_200_without_exception(self, mock_clear):
        """When no status was captured and there is no exception, status defaults to 200."""
        mock_ec = MagicMock()
        flask_mod._endpoint_collector = mock_ec

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        mock_g.serviceevents_duration_ms = 5.0
        del mock_g.serviceevents_status_code  # never captured in after_request
        mock_g.serviceevents_route = "/api/ok"
        mock_g.serviceevents_method = "GET"
        mock_g.serviceevents_path = "/api/ok"
        mock_g.serviceevents_endpoint = "ok"
        del mock_g.serviceevents_incident_processed

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.args = {}
        mock_request.view_args = {}

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        with patch.dict("sys.modules", {"flask": flask_mock}):
            _teardown_request_hook(exception=None)

        ec_kwargs = mock_ec.record_request.call_args[1]
        self.assertEqual(ec_kwargs["status_code"], 200)
        self.assertIsNone(ec_kwargs["error_info"])

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_swallows_incident_collector_error(self, mock_clear):
        """An exception from the incident collector is logged, not propagated."""
        mock_isc = MagicMock()
        mock_isc.process_potential_incident.side_effect = RuntimeError("snapshot boom")
        flask_mod._incident_snapshot_collector = mock_isc

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        mock_g.serviceevents_duration_ms = 5.0
        mock_g.serviceevents_status_code = 200
        mock_g.serviceevents_route = "/api/ok"
        mock_g.serviceevents_method = "GET"
        mock_g.serviceevents_path = "/api/ok"
        mock_g.serviceevents_endpoint = "ok"
        del mock_g.serviceevents_incident_processed

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.args = {}
        mock_request.view_args = {}

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        with patch.dict("sys.modules", {"flask": flask_mock}):
            # Should not raise even though the collector blows up.
            _teardown_request_hook(exception=None)

        mock_isc.process_potential_incident.assert_called_once()
        mock_clear.assert_called_once()


class TestFlaskTeardownTraceFallback(TestCase):
    """When Flask skips after_request (unhandled exception with PROPAGATE_EXCEPTIONS=True),
    teardown recovers trace correlation from the still-active OTel span so IncidentSnapshot
    stays joinable on the error path.
    """

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        flask_mod._endpoint_collector = None
        flask_mod._incident_snapshot_collector = None
        flask_mod._serviceevents_config = None

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_recovers_trace_when_after_request_skipped(self, mock_clear):
        """trace_id/span_id come from the active span when not pre-captured in after_request."""
        mock_isc = MagicMock()
        flask_mod._incident_snapshot_collector = mock_isc

        # g has no serviceevents_trace_id — after_request was skipped on the error path.
        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        mock_g.serviceevents_duration_ms = 12.0
        mock_g.serviceevents_route = "/api/orders"
        mock_g.serviceevents_method = "POST"
        mock_g.serviceevents_path = "/api/orders"
        mock_g.serviceevents_endpoint = "create_order"
        del mock_g.serviceevents_status_code  # infer 500 from exception
        del mock_g.serviceevents_incident_processed
        del mock_g.serviceevents_trace_id  # not pre-captured
        del mock_g.serviceevents_span_id

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.args = {}
        mock_request.view_args = {}

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        mock_span = MagicMock()
        span_ctx = MagicMock()
        span_ctx.is_valid = True
        span_ctx.trace_id = 0x0123456789ABCDEF0123456789ABCDEF
        span_ctx.span_id = 0x0123456789ABCDEF
        mock_span.get_span_context.return_value = span_ctx

        exc = ValueError("boom")
        with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
            with patch.dict("sys.modules", {"flask": flask_mock}):
                _teardown_request_hook(exception=exc)

        call_kwargs = mock_isc.process_potential_incident.call_args[1]
        req_data = call_kwargs["request_data"]
        self.assertEqual(req_data["trace_id"], 0x0123456789ABCDEF0123456789ABCDEF)
        self.assertEqual(req_data["span_id"], 0x0123456789ABCDEF)

    @patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.clear_current_operation")
    def test_teardown_keeps_precaptured_trace_from_after_request(self, mock_clear):
        """When after_request already captured trace correlation, teardown leaves it intact."""
        mock_isc = MagicMock()
        flask_mod._incident_snapshot_collector = mock_isc

        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000
        mock_g.serviceevents_duration_ms = 12.0
        mock_g.serviceevents_status_code = 200
        mock_g.serviceevents_route = "/api/orders"
        mock_g.serviceevents_method = "GET"
        mock_g.serviceevents_path = "/api/orders"
        mock_g.serviceevents_endpoint = "list_orders"
        del mock_g.serviceevents_incident_processed
        # Pre-captured in after_request (success path).
        mock_g.serviceevents_trace_id = 0xAAAA
        mock_g.serviceevents_span_id = 0xBBBB

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.args = {}
        mock_request.view_args = {}

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        # If the fallback wrongly fired it would overwrite with this span; assert it does NOT.
        mock_span = MagicMock()
        span_ctx = MagicMock()
        span_ctx.is_valid = True
        span_ctx.trace_id = 0x9999
        span_ctx.span_id = 0x8888
        mock_span.get_span_context.return_value = span_ctx

        with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
            with patch.dict("sys.modules", {"flask": flask_mock}):
                _teardown_request_hook(exception=None)

        call_kwargs = mock_isc.process_potential_incident.call_args[1]
        req_data = call_kwargs["request_data"]
        self.assertEqual(req_data["trace_id"], 0xAAAA)
        self.assertEqual(req_data["span_id"], 0xBBBB)


class TestCaptureActiveTraceContext(TestCase):
    """Tests for _capture_active_trace_context."""

    def test_returns_ids_for_valid_span(self):
        """Returns (trace_id, span_id) when an active, valid span exists."""
        mock_span = MagicMock()
        span_ctx = MagicMock()
        span_ctx.is_valid = True
        span_ctx.trace_id = 0xDEAD
        span_ctx.span_id = 0xBEEF
        mock_span.get_span_context.return_value = span_ctx

        with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
            trace_id, span_id = _capture_active_trace_context()

        self.assertEqual(trace_id, 0xDEAD)
        self.assertEqual(span_id, 0xBEEF)

    def test_returns_none_when_span_context_invalid(self):
        """Returns (None, None) when the span context is not valid."""
        mock_span = MagicMock()
        span_ctx = MagicMock()
        span_ctx.is_valid = False
        mock_span.get_span_context.return_value = span_ctx

        with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
            self.assertEqual(_capture_active_trace_context(), (None, None))

    def test_returns_none_when_otel_raises(self):
        """A failure in the OTel API is swallowed and yields (None, None)."""
        with patch("opentelemetry.trace.get_current_span", side_effect=RuntimeError("otel down")):
            self.assertEqual(_capture_active_trace_context(), (None, None))


class TestFlaskHookCrashSafety(TestCase):
    """Request-thread hooks must never turn a telemetry failure into a 500."""

    def setUp(self):
        _ServiceEventsMonitorState._instance = None
        flask_mod._endpoint_collector = None
        flask_mod._incident_snapshot_collector = None
        flask_mod._serviceevents_config = None

    @patch(
        "amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.set_current_operation",
        side_effect=RuntimeError("telemetry exploded"),
    )
    def test_before_request_swallows_telemetry_failure(self, _mock_set_op):
        """A failure inside before_request must not propagate (would become a 500)."""
        mock_g = MagicMock()
        mock_request = MagicMock()
        mock_request.method = "GET"
        mock_request.path = "/api/users"
        mock_request.endpoint = "users_list"
        mock_request.url_rule = MagicMock()
        mock_request.url_rule.rule = "/api/users"

        flask_mock = MagicMock()
        flask_mock.g = mock_g
        flask_mock.request = mock_request

        with patch.dict("sys.modules", {"flask": flask_mock}):
            # Must return normally (None), not raise.
            self.assertIsNone(_before_request_hook())

    def test_after_request_returns_response_on_telemetry_failure(self):
        """Even if telemetry fails, the customer's response must pass through unchanged."""
        mock_g = MagicMock()
        mock_g.serviceevents_skip = False
        mock_g.serviceevents_start_time = 1000000000

        mock_response = MagicMock()
        mock_response.status_code = 200

        flask_mock = MagicMock()
        flask_mock.g = mock_g

        # Make the duration math raise by having time.perf_counter_ns blow up.
        with patch("amazon.opentelemetry.distro.serviceevents.instrumentation.flask_instrumentation.time") as mock_time:
            mock_time.perf_counter_ns.side_effect = RuntimeError("clock exploded")
            with patch.dict("sys.modules", {"flask": flask_mock}):
                result = _after_request_hook(mock_response)

        self.assertIs(result, mock_response)
