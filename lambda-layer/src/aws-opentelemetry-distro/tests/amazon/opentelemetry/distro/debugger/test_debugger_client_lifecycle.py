# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for DebuggerClient.start_polling/stop_polling and ConfigurationPoller.start/stop.

These exercise the polling lifecycle (thread creation, the stop event, the
already-running / not-running early returns) WITHOUT starting real threads:
``threading.Thread`` is patched so no background work runs. Service-name and
environment resolution edge cases (present-but-empty resource attributes,
exception fallbacks, the suppress_http_instrumentation ImportError fallback)
are covered here as well.
"""

import logging
import time
import unittest
from unittest import mock

from amazon.opentelemetry.distro.debugger import _debugger_client as dc_module
from amazon.opentelemetry.distro.debugger._debugger_client import ConfigurationPoller, DebuggerClient


def _make_client(**overrides):
    """Build a DebuggerClient with explicit service/environment and a mocked session."""
    kwargs = {
        "probe_poll_interval": 60,
        "breakpoint_poll_interval": 30,
        "service_name": "my-service",
        "api_url": "http://localhost:2000",
    }
    kwargs.update(overrides)
    client = DebuggerClient(**kwargs)
    client._cached_environment = "prod"
    client._session = mock.MagicMock()
    return client


class TestStartStopPolling(unittest.TestCase):
    """Tests for DebuggerClient.start_polling / stop_polling delegation."""

    def test_start_polling_creates_and_starts_poller(self):
        client = _make_client()
        fake_poller = mock.MagicMock()
        with mock.patch.object(dc_module, "ConfigurationPoller", return_value=fake_poller) as poller_cls:
            client.start_polling()
        poller_cls.assert_called_once_with(client)
        fake_poller.start.assert_called_once()
        self.assertIs(client._poller, fake_poller)

    def test_start_polling_when_already_started_is_noop(self):
        client = _make_client()
        existing = mock.MagicMock()
        client._poller = existing
        with mock.patch.object(dc_module, "ConfigurationPoller") as poller_cls:
            client.start_polling()
        # Already started: no new poller created, existing untouched.
        poller_cls.assert_not_called()
        self.assertIs(client._poller, existing)

    def test_stop_polling_stops_and_clears_poller(self):
        client = _make_client()
        fake_poller = mock.MagicMock()
        client._poller = fake_poller
        client.stop_polling()
        fake_poller.stop.assert_called_once()
        self.assertIsNone(client._poller)

    def test_stop_polling_when_not_started_is_noop(self):
        client = _make_client()
        client._poller = None
        # Should not raise.
        client.stop_polling()
        self.assertIsNone(client._poller)


class TestConfigurationPollerStart(unittest.TestCase):
    """Tests for ConfigurationPoller.start with threads mocked out."""

    def setUp(self):
        self.client = _make_client()
        self.poller = ConfigurationPoller(self.client)

    def test_start_creates_two_daemon_threads_and_sets_running(self):
        created_threads = []

        def fake_thread(*args, **kwargs):
            thread = mock.MagicMock()
            thread.name = kwargs.get("name")
            created_threads.append((kwargs, thread))
            return thread

        with mock.patch.object(dc_module.threading, "Thread", side_effect=fake_thread):
            self.poller.start()

        # Two threads created (PROBE + BREAKPOINT), each started as a daemon.
        self.assertEqual(len(created_threads), 2)
        thread_names = {kwargs["name"] for kwargs, _ in created_threads}
        self.assertEqual(thread_names, {"ProbePoller", "BreakpointPoller"})
        for kwargs, thread in created_threads:
            self.assertTrue(kwargs["daemon"])
            thread.start.assert_called_once()
        self.assertTrue(self.poller._running)

    def test_start_clears_stop_event(self):
        self.poller._stop_event.set()
        with mock.patch.object(dc_module.threading, "Thread", return_value=mock.MagicMock()):
            self.poller.start()
        self.assertFalse(self.poller._stop_event.is_set())

    def test_start_when_already_running_is_noop(self):
        self.poller._running = True
        with mock.patch.object(dc_module.threading, "Thread") as thread_cls:
            self.poller.start()
        # Early return: no threads created.
        thread_cls.assert_not_called()

    def test_start_assigns_thread_references(self):
        probe_thread = mock.MagicMock()
        breakpoint_thread = mock.MagicMock()
        with mock.patch.object(dc_module.threading, "Thread", side_effect=[probe_thread, breakpoint_thread]):
            self.poller.start()
        self.assertIs(self.poller._probe_thread, probe_thread)
        self.assertIs(self.poller._breakpoint_thread, breakpoint_thread)


class TestConfigurationPollerStop(unittest.TestCase):
    """Tests for ConfigurationPoller.stop with threads mocked out."""

    def setUp(self):
        self.client = _make_client()
        self.poller = ConfigurationPoller(self.client)

    def _alive_thread(self, alive_after_join=False):
        thread = mock.MagicMock()
        # is_alive() is consulted twice: once to decide to join, once after join.
        thread.is_alive.side_effect = [True, alive_after_join]
        return thread

    def test_stop_sets_event_joins_threads_and_clears_running(self):
        self.poller._running = True
        probe_thread = self._alive_thread()
        breakpoint_thread = self._alive_thread()
        self.poller._probe_thread = probe_thread
        self.poller._breakpoint_thread = breakpoint_thread

        self.poller.stop()

        self.assertTrue(self.poller._stop_event.is_set())
        probe_thread.join.assert_called_once_with(timeout=5.0)
        breakpoint_thread.join.assert_called_once_with(timeout=5.0)
        self.assertFalse(self.poller._running)

    def test_stop_when_not_running_is_noop(self):
        self.poller._running = False
        self.poller._probe_thread = mock.MagicMock()
        self.poller.stop()
        # Early return: stop event not set, thread not joined.
        self.assertFalse(self.poller._stop_event.is_set())
        self.poller._probe_thread.join.assert_not_called()

    def test_stop_warns_when_probe_thread_does_not_stop(self):
        self.poller._running = True
        # PROBE thread stays alive even after join -> PROBE warning path.
        self.poller._probe_thread = self._alive_thread(alive_after_join=True)
        self.poller._breakpoint_thread = None

        with self.assertLogs(dc_module.logger, level=logging.WARNING) as cm:
            self.poller.stop()
        self.assertTrue(any("PROBE thread did not stop within timeout" in m for m in cm.output))
        self.assertFalse(self.poller._running)

    def test_stop_warns_when_breakpoint_thread_does_not_stop(self):
        self.poller._running = True
        # BREAKPOINT thread stays alive even after join -> BREAKPOINT warning path.
        self.poller._probe_thread = None
        self.poller._breakpoint_thread = self._alive_thread(alive_after_join=True)

        with self.assertLogs(dc_module.logger, level=logging.WARNING) as cm:
            self.poller.stop()
        self.assertTrue(any("BREAKPOINT thread did not stop within timeout" in m for m in cm.output))
        self.assertFalse(self.poller._running)

    def test_stop_handles_none_threads(self):
        # start() was never called, so both thread handles are None.
        self.poller._running = True
        self.poller._probe_thread = None
        self.poller._breakpoint_thread = None
        # Should not raise.
        self.poller.stop()
        self.assertFalse(self.poller._running)

    def test_stop_skips_join_for_dead_threads(self):
        self.poller._running = True
        dead_thread = mock.MagicMock()
        dead_thread.is_alive.return_value = False
        self.poller._probe_thread = dead_thread
        self.poller._breakpoint_thread = dead_thread
        self.poller.stop()
        dead_thread.join.assert_not_called()


class TestServiceNameEdgeCases(unittest.TestCase):
    """Tests for the service_name property edge cases (present-but-empty / exception)."""

    def test_empty_resource_service_name_falls_through_to_env(self):
        client = _make_client(service_name=None)
        with mock.patch.object(dc_module.trace, "get_tracer_provider") as get_tp, mock.patch.dict(
            "os.environ", {"OTEL_SERVICE_NAME": "env-svc"}
        ):
            # Resource returns an empty service name -> falsy -> fall through to env var.
            get_tp.return_value.resource.attributes.get.return_value = ""
            self.assertEqual(client.service_name, "env-svc")

    def test_resource_exception_falls_through_to_env(self):
        client = _make_client(service_name=None)
        with mock.patch.object(
            dc_module.trace, "get_tracer_provider", side_effect=RuntimeError("no provider")
        ), mock.patch.dict("os.environ", {"OTEL_SERVICE_NAME": "env-svc"}):
            self.assertEqual(client.service_name, "env-svc")

    def test_no_resource_and_no_env_returns_unknown_service(self):
        client = _make_client(service_name=None)
        with mock.patch.object(
            dc_module.trace, "get_tracer_provider", side_effect=RuntimeError("boom")
        ), mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(client.service_name, dc_module.UNKNOWN_SERVICE)
        # Unknown service is NOT cached (so a later resource population can win).
        self.assertIsNone(client._cached_service_name)


class TestEnvironmentEdgeCases(unittest.TestCase):
    """Tests for the environment property (resolution, caching, exception fallback)."""

    def test_environment_resolved_from_resource_is_cached(self):
        client = _make_client()
        client._cached_environment = None
        with mock.patch.object(dc_module.trace, "get_tracer_provider") as get_tp:
            get_tp.return_value.resource.attributes.get.side_effect = lambda key: (
                "staging" if key == "deployment.environment.name" else None
            )
            self.assertEqual(client.environment, "staging")
            # Second call uses the cache, not the provider.
            get_tp.reset_mock()
            self.assertEqual(client.environment, "staging")
            get_tp.assert_not_called()

    def test_environment_present_but_empty_returns_unknown(self):
        client = _make_client()
        client._cached_environment = None
        with mock.patch.object(dc_module.trace, "get_tracer_provider") as get_tp:
            # Both deployment.environment.name and DEPLOYMENT_ENVIRONMENT are empty/None.
            get_tp.return_value.resource.attributes.get.return_value = None
            self.assertEqual(client.environment, "UnknownEnvironment")
        # Not cached, so a later population can still win.
        self.assertIsNone(client._cached_environment)

    def test_environment_exception_returns_unknown(self):
        client = _make_client()
        client._cached_environment = None
        with mock.patch.object(dc_module.trace, "get_tracer_provider", side_effect=RuntimeError("boom")):
            self.assertEqual(client.environment, "UnknownEnvironment")


class TestSuppressHttpInstrumentationFallback(unittest.TestCase):
    """Tests for the suppress_http_instrumentation ImportError fallback in fetch_configuration_by_type."""

    def test_import_error_falls_back_to_nullcontext(self):
        client = _make_client()
        client._session.post.return_value = mock.MagicMock(
            status_code=200, json=mock.MagicMock(return_value={"LatestConfigurations": []})
        )

        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "opentelemetry.instrumentation.utils":
                raise ImportError("no instrumentation utils")
            return real_import(name, *args, **kwargs)

        # Force the suppress_http_instrumentation import to fail; fetch must still succeed
        # via the contextlib.nullcontext fallback.
        with mock.patch("builtins.__import__", side_effect=fake_import):
            result = client.fetch_configuration_by_type("PROBE")
        self.assertIsNotNone(result)
        self.assertEqual(result["LatestConfigurations"], [])


class TestCheckStalenessBreakpoint(unittest.TestCase):
    """Covers the BREAKPOINT staleness warning branch (line 712)."""

    def setUp(self):
        self.client = _make_client()
        self.poller = ConfigurationPoller(self.client)

    def test_breakpoint_staleness_warns(self):
        self.poller._breakpoint_last_success_time = time.time() - (
            ConfigurationPoller.BREAKPOINT_STALENESS_THRESHOLD + 10
        )
        with self.assertLogs(dc_module.logger, level=logging.WARNING) as cm:
            self.poller._check_staleness()
        self.assertTrue(any("[BREAKPOINT]" in m and "stale" in m for m in cm.output))


class TestApplyMergedConfigurationFailureLogging(unittest.TestCase):
    """Covers the per-failure warning loop (lines 770-771)."""

    def setUp(self):
        self.client = _make_client()
        self.poller = ConfigurationPoller(self.client)

    def test_logs_each_failed_application(self):
        manager = mock.MagicMock()
        manager.apply_configuration.return_value = {
            "applied": 0,
            "failed": 1,
            "unchanged": 0,
            "details": {"failed": [{"function_key": "myapp.broken", "error": "kaboom"}]},
        }
        with mock.patch.object(dc_module, "get_global_manager", return_value=manager):
            with self.assertLogs(dc_module.logger, level=logging.WARNING) as cm:
                self.poller._apply_merged_configuration(new_probe_configs=["p"], new_breakpoint_configs=None)
        self.assertTrue(any("Failed to apply myapp.broken" in m for m in cm.output))


if __name__ == "__main__":
    unittest.main()
