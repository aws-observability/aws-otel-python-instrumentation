# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for DebuggerClient and ConfigurationPoller logic.

These exercise the request/response handling, configuration parsing, attribute
filtering, staleness detection, and degraded-mode logging without starting real
polling threads or making real network calls (the HTTP session is mocked).
"""

import logging
import time
import unittest
from unittest import mock

from amazon.opentelemetry.distro.debugger import _debugger_client as dc_module
from amazon.opentelemetry.distro.debugger._debugger_client import (
    DEFAULT_API_URL,
    DEGRADED_POLL_INTERVAL,
    MAX_BACKOFF_ATTEMPTS,
    ConfigurationPoller,
    DebuggerClient,
)


def _make_client(**overrides):
    """Build a DebuggerClient with an explicit service/environment and a mocked session."""
    kwargs = {
        "probe_poll_interval": 60,
        "breakpoint_poll_interval": 30,
        "service_name": "my-service",
        "api_url": "http://localhost:2000",
    }
    kwargs.update(overrides)
    client = DebuggerClient(**kwargs)
    client._cached_environment = "prod"  # avoid resource lookups in tests
    client._session = mock.MagicMock()
    return client


def _response(status_code=200, json_body=None, text=""):
    resp = mock.MagicMock()
    resp.status_code = status_code
    resp.text = text
    if json_body is not None:
        resp.json.return_value = json_body
    return resp


class TestDebuggerClientConfig(unittest.TestCase):
    def test_get_api_url_from_env(self):
        with mock.patch.dict("os.environ", {"OTEL_AWS_DYNAMIC_INSTRUMENTATION_API_URL": "http://proxy:9999"}):
            self.assertEqual(DebuggerClient._get_api_url(), "http://proxy:9999")

    def test_get_api_url_default_when_unset(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(DebuggerClient._get_api_url(), DEFAULT_API_URL)

    def test_api_url_override_takes_precedence(self):
        client = _make_client(api_url="http://explicit:1234")
        self.assertEqual(client.proxy_url, "http://explicit:1234")

    def test_service_name_override(self):
        client = _make_client(service_name="override-svc")
        self.assertEqual(client.service_name, "override-svc")

    def test_service_name_from_env_when_no_override(self):
        client = _make_client(service_name=None)
        with mock.patch.object(dc_module.trace, "get_tracer_provider") as get_tp, mock.patch.dict(
            "os.environ", {"OTEL_SERVICE_NAME": "env-svc"}
        ):
            # Resource returns an unknown_service prefix so we fall through to the env var.
            get_tp.return_value.resource.attributes.get.return_value = "unknown_service:python"
            self.assertEqual(client.service_name, "env-svc")

    def test_service_name_resolved_from_resource_is_cached(self):
        client = _make_client(service_name=None)
        with mock.patch.object(dc_module.trace, "get_tracer_provider") as get_tp:
            get_tp.return_value.resource.attributes.get.return_value = "resolved-svc"
            self.assertEqual(client.service_name, "resolved-svc")
            # Second call should use the cache, not re-query the provider.
            get_tp.reset_mock()
            self.assertEqual(client.service_name, "resolved-svc")
            get_tp.assert_not_called()

    def test_missing_requests_raises(self):
        with mock.patch.object(dc_module, "REQUESTS_AVAILABLE", False):
            with self.assertRaises(ImportError):
                DebuggerClient(probe_poll_interval=60, breakpoint_poll_interval=30)


class TestFetchConfigurationByType(unittest.TestCase):
    def test_200_single_page_builds_config(self):
        client = _make_client()
        body = {
            "Changed": True,
            "SyncedAt": 123.0,
            "SyncInterval": 90,
            "LatestConfigurations": [
                {"LocationHash": "h1", "ConfigurationData": '{"foo": 1}', "AttributeFilters": None},
            ],
        }
        client._session.post.return_value = _response(200, body)

        result = client.fetch_configuration_by_type("PROBE")

        self.assertIsNotNone(result)
        self.assertTrue(result["Changed"])
        self.assertEqual(result["SyncedAt"], 123.0)
        self.assertEqual(result["SyncInterval"], 90)
        self.assertEqual(len(result["LatestConfigurations"]), 1)
        item = result["LatestConfigurations"][0]
        # ConfigurationData JSON string is deserialized; AttributeFilters defaulted to [].
        self.assertEqual(item["ConfigurationData"], {"foo": 1})
        self.assertEqual(item["AttributeFilters"], [])

    def test_non_string_configuration_data_skips_item_without_aborting_page(self):
        client = _make_client()
        body = {
            "LatestConfigurations": [
                {"LocationHash": "bad", "ConfigurationData": {"already": "dict"}, "AttributeFilters": None},
                {"LocationHash": "ok", "ConfigurationData": '{"foo": 1}', "AttributeFilters": None},
            ],
        }
        client._session.post.return_value = _response(200, body)
        result = client.fetch_configuration_by_type("PROBE")
        self.assertEqual(len(result["LatestConfigurations"]), 1)
        self.assertEqual(result["LatestConfigurations"][0]["LocationHash"], "ok")
        self.assertEqual(result["LatestConfigurations"][0]["ConfigurationData"], {"foo": 1})

    def test_invalid_json_in_configuration_data_skips_item_without_aborting_page(self):
        client = _make_client()
        body = {
            "LatestConfigurations": [
                {"LocationHash": "bad", "ConfigurationData": "{not valid json", "AttributeFilters": None},
                {"LocationHash": "ok", "ConfigurationData": '{"foo": 1}', "AttributeFilters": None},
            ],
        }
        client._session.post.return_value = _response(200, body)
        result = client.fetch_configuration_by_type("PROBE")
        self.assertEqual(len(result["LatestConfigurations"]), 1)
        self.assertEqual(result["LatestConfigurations"][0]["LocationHash"], "ok")

    def test_payload_includes_service_environment_and_type(self):
        client = _make_client()
        client._session.post.return_value = _response(200, {"LatestConfigurations": []})
        client.fetch_configuration_by_type("BREAKPOINT", last_sync_time=55.0)
        _, kwargs = client._session.post.call_args
        payload = kwargs["json"]
        self.assertEqual(payload["Service"], "my-service")
        self.assertEqual(payload["Environment"], "prod")
        self.assertEqual(payload["InstrumentationType"], "BREAKPOINT")
        self.assertEqual(payload["SyncedAt"], 55.0)

    def test_pagination_follows_next_token(self):
        client = _make_client()
        page1 = {
            "Changed": True,
            "SyncedAt": 1.0,
            "LatestConfigurations": [{"LocationHash": "a", "AttributeFilters": []}],
            "NextToken": "tok",
        }
        page2 = {
            "LatestConfigurations": [{"LocationHash": "b", "AttributeFilters": []}],
        }
        client._session.post.side_effect = [_response(200, page1), _response(200, page2)]
        result = client.fetch_configuration_by_type("PROBE")
        self.assertEqual(len(result["LatestConfigurations"]), 2)
        self.assertEqual(client._session.post.call_count, 2)

    def test_404_returns_empty_unchanged(self):
        client = _make_client()
        client._session.post.return_value = _response(404, text="not found")
        result = client.fetch_configuration_by_type("PROBE")
        self.assertEqual(result, {"Changed": False, "LatestConfigurations": []})

    def test_400_returns_none(self):
        client = _make_client()
        client._session.post.return_value = _response(400, text="bad")
        self.assertIsNone(client.fetch_configuration_by_type("PROBE"))

    def test_500_returns_none(self):
        client = _make_client()
        client._session.post.return_value = _response(503, text="server error")
        self.assertIsNone(client.fetch_configuration_by_type("PROBE"))

    def test_unexpected_status_returns_none(self):
        client = _make_client()
        client._session.post.return_value = _response(302, text="redirect")
        self.assertIsNone(client.fetch_configuration_by_type("PROBE"))

    def test_invalid_json_returns_none(self):
        import json as json_module

        client = _make_client()
        resp = _response(200)
        resp.json.side_effect = json_module.JSONDecodeError("boom", "", 0)
        client._session.post.return_value = resp
        self.assertIsNone(client.fetch_configuration_by_type("PROBE"))

    def test_timeout_returns_none(self):
        client = _make_client()
        client._session.post.side_effect = dc_module.requests.exceptions.Timeout()
        self.assertIsNone(client.fetch_configuration_by_type("PROBE"))

    def test_connection_error_returns_none(self):
        client = _make_client()
        client._session.post.side_effect = dc_module.requests.exceptions.ConnectionError()
        self.assertIsNone(client.fetch_configuration_by_type("PROBE"))

    def test_generic_request_exception_returns_none(self):
        client = _make_client()
        client._session.post.side_effect = dc_module.requests.exceptions.RequestException()
        self.assertIsNone(client.fetch_configuration_by_type("PROBE"))

    def test_unexpected_exception_returns_none(self):
        client = _make_client()
        client._session.post.side_effect = RuntimeError("unexpected")
        self.assertIsNone(client.fetch_configuration_by_type("PROBE"))

    def test_empty_json_body_is_tolerated(self):
        client = _make_client()
        client._session.post.return_value = _response(200, json_body=None)
        client._session.post.return_value.json.return_value = None
        result = client.fetch_configuration_by_type("PROBE")
        self.assertIsNotNone(result)
        self.assertEqual(result["LatestConfigurations"], [])


class TestConfigurationPollerLogic(unittest.TestCase):
    def setUp(self):
        self.client = _make_client()
        self.poller = ConfigurationPoller(self.client)

    def test_matches_attribute_filters_empty_is_true(self):
        self.assertTrue(ConfigurationPoller._matches_attribute_filters([]))

    def test_matches_attribute_filters_match(self):
        with mock.patch.object(dc_module.trace, "get_tracer_provider") as get_tp:
            attrs = {"service.name": "my-service", "deployment.environment": "prod"}
            get_tp.return_value.resource.attributes.get.side_effect = attrs.get
            self.assertTrue(ConfigurationPoller._matches_attribute_filters([{"service.name": "my-service"}]))

    def test_matches_attribute_filters_no_match(self):
        with mock.patch.object(dc_module.trace, "get_tracer_provider") as get_tp:
            get_tp.return_value.resource.attributes.get.side_effect = {"service.name": "other"}.get
            self.assertFalse(ConfigurationPoller._matches_attribute_filters([{"service.name": "my-service"}]))

    def test_matches_attribute_filters_skips_invalid_objects(self):
        with mock.patch.object(dc_module.trace, "get_tracer_provider") as get_tp:
            get_tp.return_value.resource.attributes.get.side_effect = {}.get
            # A non-dict filter and an empty-key filter are skipped; no match -> False.
            self.assertFalse(ConfigurationPoller._matches_attribute_filters(["not-a-dict", {"": "x"}]))

    def test_matches_attribute_filters_defaults_true_on_error(self):
        with mock.patch.object(dc_module.trace, "get_tracer_provider", side_effect=RuntimeError("boom")):
            self.assertTrue(ConfigurationPoller._matches_attribute_filters([{"service.name": "my-service"}]))

    def test_parse_api_response_filters_and_parses(self):
        api_response = {
            "LatestConfigurations": [
                {"LocationHash": "keep", "AttributeFilters": []},
                {"LocationHash": "drop", "AttributeFilters": [{"service.name": "nope"}]},
            ]
        }
        # First config matches (empty filter), second does not.
        with mock.patch.object(ConfigurationPoller, "_matches_attribute_filters", side_effect=[True, False]):
            with mock.patch.object(dc_module.BreakpointConfiguration, "from_api_config") as from_api:
                from_api.return_value = mock.MagicMock()
                configs = self.poller._parse_api_response_to_configs(api_response)
        self.assertEqual(len(configs), 1)
        from_api.assert_called_once()

    def test_parse_api_response_skips_none_and_errors(self):
        api_response = {
            "LatestConfigurations": [
                {"LocationHash": "a", "AttributeFilters": []},
                {"LocationHash": "b", "AttributeFilters": []},
                {"LocationHash": "c", "AttributeFilters": []},
            ]
        }
        with mock.patch.object(ConfigurationPoller, "_matches_attribute_filters", return_value=True):
            with mock.patch.object(dc_module.BreakpointConfiguration, "from_api_config") as from_api:
                from_api.side_effect = [mock.MagicMock(), None, RuntimeError("parse fail")]
                configs = self.poller._parse_api_response_to_configs(api_response)
        # Only the first (valid) config is kept; None and the raising one are dropped.
        self.assertEqual(len(configs), 1)

    def test_apply_merged_configuration_delegates_to_manager(self):
        manager = mock.MagicMock()
        manager.apply_configuration.return_value = {
            "applied": 2,
            "failed": 0,
            "unchanged": 1,
            "details": {"failed": []},
        }
        with mock.patch.object(dc_module, "get_global_manager", return_value=manager):
            self.poller._apply_merged_configuration(new_probe_configs=["p"], new_breakpoint_configs=None)
        # Probe cache updated; manager applied the merged (probe + breakpoint) list.
        self.assertEqual(self.poller._cached_probe_configs, ["p"])
        manager.apply_configuration.assert_called_once_with(["p"])
        manager.report_initial_status.assert_called_once()

    def test_apply_merged_configuration_no_manager_is_noop(self):
        with mock.patch.object(dc_module, "get_global_manager", return_value=None):
            # Should not raise even though there is no manager.
            self.poller._apply_merged_configuration(new_probe_configs=None, new_breakpoint_configs=["b"])
        self.assertEqual(self.poller._cached_breakpoint_configs, ["b"])

    def test_apply_merged_configuration_swallows_manager_errors(self):
        manager = mock.MagicMock()
        manager.apply_configuration.side_effect = RuntimeError("apply boom")
        with mock.patch.object(dc_module, "get_global_manager", return_value=manager):
            # Exception must be swallowed (SAFETY: polling must never crash the app).
            self.poller._apply_merged_configuration(new_probe_configs=["p"], new_breakpoint_configs=None)

    def test_check_degraded_mode_warns_at_threshold(self):
        with self.assertLogs(dc_module.logger, level=logging.WARNING) as cm:
            self.poller._check_degraded_mode("PROBE", MAX_BACKOFF_ATTEMPTS)
        self.assertTrue(any("degraded polling mode" in m for m in cm.output))

    def test_check_degraded_mode_silent_below_threshold(self):
        logger_obj = logging.getLogger(dc_module.__name__)
        with mock.patch.object(logger_obj, "warning") as warn:
            self.poller._check_degraded_mode("PROBE", MAX_BACKOFF_ATTEMPTS - 1)
            warn.assert_not_called()

    def test_check_staleness_warns_when_probe_stale(self):
        self.poller._probe_last_success_time = time.time() - (ConfigurationPoller.PROBE_STALENESS_THRESHOLD + 10)
        with self.assertLogs(dc_module.logger, level=logging.WARNING) as cm:
            self.poller._check_staleness()
        self.assertTrue(any("[PROBE]" in m and "stale" in m for m in cm.output))

    def test_check_staleness_silent_when_fresh(self):
        self.poller._probe_last_success_time = time.time()
        self.poller._breakpoint_last_success_time = time.time()
        logger_obj = logging.getLogger(dc_module.__name__)
        with mock.patch.object(logger_obj, "warning") as warn:
            self.poller._check_staleness()
            warn.assert_not_called()

    def test_is_running_reflects_state(self):
        self.assertFalse(self.poller.is_running())
        self.poller._running = True
        self.assertTrue(self.poller.is_running())

    def test_degraded_poll_interval_constant_positive(self):
        # Guards against an accidental zero/negative that would busy-loop.
        self.assertGreater(DEGRADED_POLL_INTERVAL, 0)


if __name__ == "__main__":
    unittest.main()
