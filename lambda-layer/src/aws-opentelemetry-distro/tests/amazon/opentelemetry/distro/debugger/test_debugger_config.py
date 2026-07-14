# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for debugger.py configuration helpers and client lifecycle.

Fork registration and real thread/network behavior are covered elsewhere
(test_debugger_fork.py). These tests focus on env-var parsing and the
client start/stop/cleanup lifecycle with collaborators mocked out.
"""

import os
import unittest
from unittest import mock

from amazon.opentelemetry.distro.debugger import debugger as debugger_module
from amazon.opentelemetry.distro.debugger._debugger_client import (
    DEFAULT_BREAKPOINT_POLL_INTERVAL,
    DEFAULT_PROBE_POLL_INTERVAL,
)
from amazon.opentelemetry.distro.debugger.debugger import (
    OTEL_AWS_DYNAMIC_INSTRUMENTATION_BREAKPOINT_POLL_INTERVAL,
    OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED,
    OTEL_AWS_DYNAMIC_INSTRUMENTATION_PROBE_POLL_INTERVAL,
    cleanup_debugger,
    get_debugger_client,
    get_debugger_config,
    is_debugger_enabled,
    start_debugger_client,
    stop_debugger_client,
)


class TestIsDebuggerEnabled(unittest.TestCase):
    """Tests for is_debugger_enabled."""

    @mock.patch.dict(os.environ, {OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED: "true"})
    def test_true_enables(self):
        self.assertTrue(is_debugger_enabled())

    @mock.patch.dict(os.environ, {OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED: "TRUE"})
    def test_uppercase_true_enables(self):
        self.assertTrue(is_debugger_enabled())

    @mock.patch.dict(os.environ, {OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED: "  true  "})
    def test_whitespace_padded_true_enables(self):
        self.assertTrue(is_debugger_enabled())

    @mock.patch.dict(os.environ, {OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED: "false"})
    def test_false_disables(self):
        self.assertFalse(is_debugger_enabled())

    @mock.patch.dict(os.environ, {OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED: ""})
    def test_empty_disables(self):
        self.assertFalse(is_debugger_enabled())

    def test_unset_disables_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED, None)
            self.assertFalse(is_debugger_enabled())


class TestGetDebuggerConfig(unittest.TestCase):
    """Tests for get_debugger_config and the embedded _parse_interval."""

    def _clean_env(self):
        return mock.patch.dict(
            os.environ,
            {
                key: ""
                for key in (
                    "OTEL_AWS_DYNAMIC_INSTRUMENTATION_API_URL",
                    OTEL_AWS_DYNAMIC_INSTRUMENTATION_PROBE_POLL_INTERVAL,
                    OTEL_AWS_DYNAMIC_INSTRUMENTATION_BREAKPOINT_POLL_INTERVAL,
                )
            },
            clear=False,
        )

    def test_defaults_when_unset(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            for key in (
                "OTEL_AWS_DYNAMIC_INSTRUMENTATION_API_URL",
                OTEL_AWS_DYNAMIC_INSTRUMENTATION_PROBE_POLL_INTERVAL,
                OTEL_AWS_DYNAMIC_INSTRUMENTATION_BREAKPOINT_POLL_INTERVAL,
            ):
                os.environ.pop(key, None)
            config = get_debugger_config()
        self.assertEqual(config["api_url"], "http://localhost:2000")
        self.assertEqual(config["probe_poll_interval"], DEFAULT_PROBE_POLL_INTERVAL)
        self.assertEqual(config["breakpoint_poll_interval"], DEFAULT_BREAKPOINT_POLL_INTERVAL)

    @mock.patch.dict(
        os.environ,
        {
            "OTEL_AWS_DYNAMIC_INSTRUMENTATION_API_URL": "http://proxy:9000",
            OTEL_AWS_DYNAMIC_INSTRUMENTATION_PROBE_POLL_INTERVAL: "120",
            OTEL_AWS_DYNAMIC_INSTRUMENTATION_BREAKPOINT_POLL_INTERVAL: "30",
        },
    )
    def test_valid_values_parsed(self):
        config = get_debugger_config()
        self.assertEqual(config["api_url"], "http://proxy:9000")
        self.assertEqual(config["probe_poll_interval"], 120)
        self.assertEqual(config["breakpoint_poll_interval"], 30)

    @mock.patch.dict(
        os.environ,
        {
            OTEL_AWS_DYNAMIC_INSTRUMENTATION_PROBE_POLL_INTERVAL: "0",
            OTEL_AWS_DYNAMIC_INSTRUMENTATION_BREAKPOINT_POLL_INTERVAL: "-5",
        },
    )
    def test_below_minimum_clamps_to_default(self):
        config = get_debugger_config()
        self.assertEqual(config["probe_poll_interval"], DEFAULT_PROBE_POLL_INTERVAL)
        self.assertEqual(config["breakpoint_poll_interval"], DEFAULT_BREAKPOINT_POLL_INTERVAL)

    @mock.patch.dict(
        os.environ,
        {
            OTEL_AWS_DYNAMIC_INSTRUMENTATION_PROBE_POLL_INTERVAL: "not-a-number",
            OTEL_AWS_DYNAMIC_INSTRUMENTATION_BREAKPOINT_POLL_INTERVAL: "abc",
        },
    )
    def test_non_integer_falls_back_to_default(self):
        config = get_debugger_config()
        self.assertEqual(config["probe_poll_interval"], DEFAULT_PROBE_POLL_INTERVAL)
        self.assertEqual(config["breakpoint_poll_interval"], DEFAULT_BREAKPOINT_POLL_INTERVAL)


class TestStartDebuggerClient(unittest.TestCase):
    """Tests for start_debugger_client / stop_debugger_client / get_debugger_client."""

    def setUp(self):
        self._original_client = debugger_module._global_debugger_client
        debugger_module._global_debugger_client = None

    def tearDown(self):
        debugger_module._global_debugger_client = self._original_client

    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.get_global_manager")
    def test_start_fails_when_manager_missing(self, mock_get_manager):
        mock_get_manager.return_value = None
        result = start_debugger_client()
        self.assertIsNone(result)
        self.assertIsNone(debugger_module._global_debugger_client)

    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.DebuggerClient")
    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.get_global_manager")
    def test_start_creates_and_starts_client(self, mock_get_manager, mock_client_cls):
        mock_get_manager.return_value = mock.MagicMock()
        fake_client = mock.MagicMock()
        fake_client.service_name = "svc"
        mock_client_cls.return_value = fake_client

        with mock.patch.object(debugger_module, "get_debugger_config") as mock_config:
            mock_config.return_value = {
                "api_url": "http://localhost:2000",
                "probe_poll_interval": 600,
                "breakpoint_poll_interval": 60,
            }
            result = start_debugger_client()

        self.assertIs(result, fake_client)
        fake_client.start_polling.assert_called_once()
        self.assertIs(debugger_module._global_debugger_client, fake_client)

    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.get_global_manager")
    def test_start_returns_existing_client(self, mock_get_manager):
        existing = mock.MagicMock()
        debugger_module._global_debugger_client = existing
        result = start_debugger_client()
        self.assertIs(result, existing)
        # Manager should not even be consulted since client already running.
        mock_get_manager.assert_not_called()

    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.get_global_manager")
    def test_start_handles_exception(self, mock_get_manager):
        mock_get_manager.side_effect = RuntimeError("boom")
        result = start_debugger_client()
        self.assertIsNone(result)

    def test_stop_client_stops_and_clears(self):
        fake_client = mock.MagicMock()
        debugger_module._global_debugger_client = fake_client
        stop_debugger_client()
        fake_client.stop_polling.assert_called_once()
        self.assertIsNone(debugger_module._global_debugger_client)

    def test_stop_client_when_none_is_noop(self):
        debugger_module._global_debugger_client = None
        # Should not raise.
        stop_debugger_client()
        self.assertIsNone(debugger_module._global_debugger_client)

    def test_stop_client_handles_exception(self):
        fake_client = mock.MagicMock()
        fake_client.stop_polling.side_effect = RuntimeError("boom")
        debugger_module._global_debugger_client = fake_client
        # Should swallow the error and still clear the client.
        stop_debugger_client()
        self.assertIsNone(debugger_module._global_debugger_client)

    def test_get_debugger_client_returns_global(self):
        fake_client = mock.MagicMock()
        debugger_module._global_debugger_client = fake_client
        self.assertIs(get_debugger_client(), fake_client)


class TestCleanupDebugger(unittest.TestCase):
    """Tests for cleanup_debugger."""

    def setUp(self):
        self._original_client = debugger_module._global_debugger_client
        debugger_module._global_debugger_client = None

    def tearDown(self):
        debugger_module._global_debugger_client = self._original_client

    @mock.patch("amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter")
    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.get_global_manager")
    def test_cleanup_shuts_down_emitter_and_clears_client(self, mock_get_manager, mock_get_emitter):
        fake_client = mock.MagicMock()
        debugger_module._global_debugger_client = fake_client
        emitter = mock.MagicMock()
        mock_get_emitter.return_value = emitter
        manager = mock.MagicMock()
        manager.get_status.return_value = {"functions": []}
        mock_get_manager.return_value = manager

        cleanup_debugger()

        fake_client.stop_polling.assert_called_once()
        emitter.shutdown.assert_called_once()
        self.assertIsNone(debugger_module._global_debugger_client)

    @mock.patch("amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter")
    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.get_global_manager")
    def test_cleanup_handles_missing_manager_and_emitter(self, mock_get_manager, mock_get_emitter):
        mock_get_manager.return_value = None
        mock_get_emitter.return_value = None
        # Should not raise even with nothing to clean up.
        cleanup_debugger()

    @mock.patch("amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter")
    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.get_global_manager")
    def test_cleanup_removes_reported_functions(self, mock_get_manager, mock_get_emitter):
        mock_get_emitter.return_value = None
        manager = mock.MagicMock()
        # The cleanup loop expects a list of dicts with a function_key field.
        manager.get_status.return_value = {"functions": [{"function_key": "myapp.handler"}]}
        mock_get_manager.return_value = manager

        cleanup_debugger()

        manager._remove_function.assert_called_once_with("myapp.handler")

    @mock.patch("amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter")
    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.get_global_manager")
    def test_cleanup_swallows_remove_errors(self, mock_get_manager, mock_get_emitter):
        mock_get_emitter.return_value = None
        manager = mock.MagicMock()
        manager.get_status.return_value = {"functions": [{"function_key": "myapp.handler"}]}
        manager._remove_function.side_effect = RuntimeError("boom")
        mock_get_manager.return_value = manager

        # Per-function removal errors must not propagate.
        cleanup_debugger()

    @mock.patch("amazon.opentelemetry.distro.debugger._function_wrapper.get_snapshot_emitter")
    @mock.patch("amazon.opentelemetry.distro.debugger.debugger.get_global_manager")
    def test_cleanup_swallows_emitter_shutdown_errors(self, mock_get_manager, mock_get_emitter):
        mock_get_manager.return_value = None
        emitter = mock.MagicMock()
        emitter.shutdown.side_effect = RuntimeError("shutdown boom")
        mock_get_emitter.return_value = emitter

        # Emitter shutdown errors must not propagate.
        cleanup_debugger()
        emitter.shutdown.assert_called_once()


if __name__ == "__main__":
    unittest.main()
