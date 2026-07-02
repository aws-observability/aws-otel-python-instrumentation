# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import sys
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock, patch

from amazon.opentelemetry.serviceevents.config import ServiceEventsConfig
from amazon.opentelemetry.serviceevents.models.resource_attributes import ResourceAttributes
from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import ServiceEventsInstrumentation


class TestServiceEventsInstrumentation(TestCase):
    """Test the ServiceEventsInstrumentation class."""

    def setUp(self):
        # initialize() registers an atexit shutdown hook. These tests construct throwaway
        # instances and don't always call shutdown(), so neutralize the registration to
        # avoid leaking hooks that fire (and log) after pytest tears down its streams.
        # The real atexit wiring is covered explicitly in test_initialize_registers_atexit.
        patcher = patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.atexit")
        self.mock_atexit = patcher.start()
        self.addCleanup(patcher.stop)

    def test_init(self):
        """Test initialization with config."""
        config = ServiceEventsConfig()
        instrumentation = ServiceEventsInstrumentation(config)

        self.assertEqual(instrumentation.config, config)
        self.assertIsNone(instrumentation.monitor_state)
        self.assertEqual(instrumentation.collectors, [])
        self.assertFalse(instrumentation._initialized)

    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks")
    def test_initialize_success(self, mock_install_hooks, mock_state):
        """Test successful initialization."""
        config = ServiceEventsConfig(enabled=True, function_instrument_enabled=True)
        instrumentation = ServiceEventsInstrumentation(config)

        mock_state.get_instance.return_value = MagicMock()

        instrumentation.initialize()

        # Should get monitor state instance
        mock_state.get_instance.assert_called_once()

        # Should install AST hooks
        mock_install_hooks.assert_called_once()

        # Should be marked as initialized
        self.assertTrue(instrumentation._initialized)

    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks")
    def test_initialize_disabled(self, mock_install_hooks, mock_state):
        """Test that initialization is skipped when disabled."""
        config = ServiceEventsConfig(enabled=False)
        instrumentation = ServiceEventsInstrumentation(config)

        instrumentation.initialize()

        # Should not install hooks or get state when disabled
        mock_install_hooks.assert_not_called()
        mock_state.get_instance.assert_not_called()
        self.assertFalse(instrumentation._initialized)

    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks")
    def test_initialize_idempotent(self, mock_install_hooks, mock_state):
        """Test that initialize can be called multiple times safely."""
        config = ServiceEventsConfig(enabled=True, function_instrument_enabled=True)
        instrumentation = ServiceEventsInstrumentation(config)

        mock_state.get_instance.return_value = MagicMock()

        # Call initialize twice
        instrumentation.initialize()
        instrumentation.initialize()

        # Should only initialize once
        self.assertEqual(mock_state.get_instance.call_count, 1)
        self.assertEqual(mock_install_hooks.call_count, 1)

    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks")
    def test_initialize_registers_atexit_and_shutdown_unregisters(self, mock_install_hooks, mock_state):
        """initialize() registers the shutdown atexit hook exactly once; shutdown() removes it."""
        config = ServiceEventsConfig(enabled=True, function_instrument_enabled=True)
        instrumentation = ServiceEventsInstrumentation(config)
        mock_state.get_instance.return_value = MagicMock()

        instrumentation.initialize()
        # A second initialize() is a no-op (already initialized) — still only one registration.
        instrumentation.initialize()

        self.mock_atexit.register.assert_called_once_with(instrumentation.shutdown)
        self.assertTrue(instrumentation._atexit_registered)

        instrumentation.shutdown()

        self.mock_atexit.unregister.assert_called_once_with(instrumentation.shutdown)
        self.assertFalse(instrumentation._atexit_registered)

    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks")
    def test_initialize_with_exception(self, mock_install_hooks, mock_state):
        """Test that exceptions during initialization are caught."""
        config = ServiceEventsConfig(enabled=True)
        instrumentation = ServiceEventsInstrumentation(config)

        # Make initialization raise an exception
        mock_state.get_instance.side_effect = RuntimeError("Test error")

        # Should not raise exception
        instrumentation.initialize()

        # Should not be marked as initialized
        self.assertFalse(instrumentation._initialized)

    def test_shutdown_not_initialized(self):
        """Test shutdown when not initialized."""
        config = ServiceEventsConfig()
        instrumentation = ServiceEventsInstrumentation(config)

        # Should not raise exception
        instrumentation.shutdown()

    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks")
    def test_shutdown_with_collectors(self, mock_install_hooks, mock_state):
        """Test shutdown with collectors."""
        config = ServiceEventsConfig(enabled=True)
        instrumentation = ServiceEventsInstrumentation(config)

        mock_state.get_instance.return_value = MagicMock()

        # Initialize
        instrumentation.initialize()

        # Add mock collectors
        mock_collector1 = MagicMock()
        mock_collector2 = MagicMock()
        instrumentation.collectors = [mock_collector1, mock_collector2]

        # Shutdown
        instrumentation.shutdown()

        # Should stop all collectors
        mock_collector1.stop.assert_called_once()
        mock_collector2.stop.assert_called_once()

        # Should be marked as not initialized
        self.assertFalse(instrumentation._initialized)

    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks")
    def test_shutdown_collector_exception(self, mock_install_hooks, mock_state):
        """Test that exceptions during collector shutdown are caught."""
        config = ServiceEventsConfig(enabled=True)
        instrumentation = ServiceEventsInstrumentation(config)

        mock_state.get_instance.return_value = MagicMock()

        instrumentation.initialize()

        # Add collector that raises exception on stop
        mock_collector = MagicMock()
        mock_collector.stop.side_effect = RuntimeError("Test error")
        instrumentation.collectors = [mock_collector]

        # Should not raise exception
        instrumentation.shutdown()

    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks")
    def test_initialize_with_packages_exclude(self, mock_install_hooks, mock_state):
        """install_ast_hooks receives both packages_include and packages_exclude from config."""
        config = ServiceEventsConfig(
            enabled=True,
            function_instrument_enabled=True,
            packages_include=["myapp", "mylib.*"],
            packages_exclude=["test.*", "build.*"],
        )
        instrumentation = ServiceEventsInstrumentation(config)

        mock_state.get_instance.return_value = MagicMock()

        instrumentation.initialize()

        call_args = mock_install_hooks.call_args
        self.assertEqual(call_args[1]["packages_exclude"], ["test.*", "build.*"])
        self.assertEqual(call_args[1]["packages_include"], {"myapp", "mylib.*"})

    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks")
    def test_no_warn_when_enabled_without_include(self, mock_install_hooks, mock_state):
        """Empty PACKAGES_INCLUDE installs hooks quietly — no WARNING.

        Function instrumentation is on by default, so an empty allowlist is a normal
        no-op (instruments nothing) rather than a misconfiguration; warning on it would
        fire on every default install and just be noise. Hooks still install — endpoint
        signals are unaffected.
        """
        config = ServiceEventsConfig(
            enabled=True,
            function_instrument_enabled=True,
            packages_include=[],
            packages_exclude=[],
        )
        instrumentation = ServiceEventsInstrumentation(config)
        mock_state.get_instance.return_value = MagicMock()

        logger_name = "amazon.opentelemetry.serviceevents.serviceevents_instrumentation"
        with self.assertLogs(logger_name, level="DEBUG") as captured:
            instrumentation.initialize()

        warn_records = [r for r in captured.records if r.levelno >= logging.WARNING]
        self.assertEqual(warn_records, [], "expected no WARNING when include list is empty")
        # Hooks are still installed (instruments nothing, but endpoint signals flow).
        mock_install_hooks.assert_called_once()


class TestServiceEventsModes(TestCase):
    """Test function-instrument (AST) vs endpoint-only mode collector wiring.

    Function-level telemetry is controlled by `function_instrument_enabled`; when off,
    only endpoint metrics and incident snapshots run.
    """

    def setUp(self):
        # Neutralize atexit registration (see TestServiceEventsInstrumentation.setUp).
        patcher = patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.atexit")
        self.mock_atexit = patcher.start()
        self.addCleanup(patcher.stop)

    def _init_and_get_collector_names(self, function_instrument_enabled=False, logs_endpoint=""):
        config = ServiceEventsConfig(
            enabled=True,
            function_instrument_enabled=function_instrument_enabled,
            logs_endpoint=logs_endpoint,
            sampling_mode="always",
        )
        inst = ServiceEventsInstrumentation(config)
        inst.initialize()
        names = [c.__class__.__name__ for c in inst.collectors]
        inst.shutdown()
        return inst, names

    def test_ast_mode_collectors(self):
        """Function-instrument mode runs EndpointMetric + IncidentSnapshot collectors.

        FunctionCall telemetry is recorded directly into the OTel histogram from the
        monitor, not via a dedicated collector, so the collector set matches
        endpoint-only mode; AST mode differs by installing the AST hooks + wiring the
        histogram.
        """
        inst, names = self._init_and_get_collector_names(
            function_instrument_enabled=True, logs_endpoint="http://localhost:4318/v1/logs"
        )
        self.assertIn("EndpointMetricCollector", names)
        self.assertIn("IncidentSnapshotCollector", names)

    def test_endpoint_only_mode_collectors(self):
        """Function instrument off: EndpointMetric + IncidentSnapshot run."""
        inst, names = self._init_and_get_collector_names(
            function_instrument_enabled=False, logs_endpoint="http://localhost:4318/v1/logs"
        )
        self.assertIn("EndpointMetricCollector", names)
        self.assertIn("IncidentSnapshotCollector", names)

    def test_incident_snapshot_always_active(self):
        """IncidentSnapshotCollector runs in every mode."""
        for ast in (True, False):
            _, names = self._init_and_get_collector_names(function_instrument_enabled=ast)
            self.assertIn(
                "IncidentSnapshotCollector",
                names,
                f"IncidentSnapshotCollector missing for ast={ast}",
            )

    def test_ast_mode_otlp_emitter_wired(self):
        """All collectors in function-instrument mode have otlp_emitter when logs_endpoint configured."""
        inst, _ = self._init_and_get_collector_names(
            function_instrument_enabled=True, logs_endpoint="http://localhost:4318/v1/logs"
        )
        for c in inst.collectors:
            self.assertIsNotNone(c.otlp_emitter, f"{c.__class__.__name__} should have otlp_emitter")

    def test_endpoint_only_mode_otlp_emitter_wired(self):
        """EndpointMetricCollector and IncidentSnapshotCollector have emitter in endpoint-only mode."""
        inst, _ = self._init_and_get_collector_names(
            function_instrument_enabled=False, logs_endpoint="http://localhost:4318/v1/logs"
        )
        for c in inst.collectors:
            if c.__class__.__name__ in ("EndpointMetricCollector", "IncidentSnapshotCollector"):
                self.assertIsNotNone(c.otlp_emitter, f"{c.__class__.__name__} should have otlp_emitter")

    def test_resource_includes_aws_local_service_as_copy_of_service_name(self):
        """ServiceEvents Resource includes aws.local.service mirroring service.name."""
        config = ServiceEventsConfig(
            enabled=True,
            service_name="my-test-service",
            function_instrument_enabled=False,
            logs_endpoint="http://localhost:4316/v1/logs",
            metrics_endpoint="http://localhost:4316/v1/metrics",
            sampling_mode="always",
        )
        inst = ServiceEventsInstrumentation(config)
        inst.initialize()
        try:
            resource_attrs = inst._otlp_logger_provider._resource.attributes
            self.assertEqual(resource_attrs.get("service.name"), "my-test-service")
            self.assertEqual(resource_attrs.get("aws.local.service"), "my-test-service")
        finally:
            inst.shutdown()

    def test_resource_includes_deployment_and_vcs_metadata_from_env(self):
        """Deployment id, git commit SHA, and repo URL flow through to the OTel Resource
        (not per-call attributes), so they ride along with every signal automatically."""
        env = {
            "OTEL_AWS_SERVICE_EVENTS_DEPLOYMENT_ID": "deploy-resource-test",
            "OTEL_AWS_SERVICE_EVENTS_GIT_COMMIT_SHA": "sha-resource-test",
            "OTEL_AWS_SERVICE_EVENTS_GIT_REPO_URL": "https://github.com/org/resource-test",
        }
        config = ServiceEventsConfig(
            enabled=True,
            service_name="resource-test-service",
            environment="resource-env",
            sdk_version="9.9.9",
            function_instrument_enabled=False,
            logs_endpoint="http://localhost:4316/v1/logs",
            metrics_endpoint="http://localhost:4316/v1/metrics",
            sampling_mode="always",
        )
        with patch.dict(os.environ, env, clear=False):
            inst = ServiceEventsInstrumentation(config)
            inst.initialize()
            try:
                resource_attrs = inst._otlp_logger_provider._resource.attributes
                self.assertEqual(resource_attrs.get("deployment.environment.name"), "resource-env")
                self.assertEqual(resource_attrs.get("aws.service_events.version"), "9.9.9")
                self.assertEqual(resource_attrs.get("aws.service_events.deployment.id"), "deploy-resource-test")
                self.assertEqual(resource_attrs.get("vcs.ref.head.revision"), "sha-resource-test")
                self.assertEqual(
                    resource_attrs.get("vcs.repository.url.full"),
                    "https://github.com/org/resource-test",
                )
            finally:
                inst.shutdown()

    def test_no_otlp_emitter_without_endpoint(self):
        """No otlp_emitter when logs_endpoint is empty."""
        inst, _ = self._init_and_get_collector_names(function_instrument_enabled=True, logs_endpoint="")
        for c in inst.collectors:
            self.assertIsNone(c.otlp_emitter, f"{c.__class__.__name__} should NOT have otlp_emitter")

    def test_endpoint_only_mode_standalone_deployment_event(self):
        """Endpoint-only mode emits DeploymentEvent at startup via the standalone path."""
        config = ServiceEventsConfig(
            enabled=True,
            function_instrument_enabled=False,
            logs_endpoint="http://localhost:4318/v1/logs",
            sampling_mode="always",
        )
        inst = ServiceEventsInstrumentation(config)
        inst.initialize()
        # OTLP emitter should be created
        self.assertIsNotNone(inst._otlp_emitter)
        inst.shutdown()


class TestSamplingModeActivation(TestCase):
    """Verify startup applies the configured sampling mode through ServiceEventsInstrumentation.

    Regression coverage for two bugs: (1) a `!= "auto"` guard that left auto mode inert because
    the module default is "always", and (2) an invalid mode (e.g. the removed "adaptive" left in
    a stale env var) aborting all of ServiceEvents init instead of falling back to the default.
    """

    def setUp(self):
        # Neutralize atexit registration (see TestServiceEventsInstrumentation.setUp).
        patcher = patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.atexit")
        self.mock_atexit = patcher.start()
        self.addCleanup(patcher.stop)
        # Restore the module-level sampling mode after each test so state doesn't leak.
        from amazon.opentelemetry.serviceevents.python_monitor import set_sampling_mode

        self.addCleanup(set_sampling_mode, "always")

    def _init_with_mode(self, mode):
        config = ServiceEventsConfig(
            enabled=True,
            function_instrument_enabled=True,
            logs_endpoint="http://localhost:4318/v1/logs",
            sampling_mode=mode,
        )
        inst = ServiceEventsInstrumentation(config)
        inst.initialize()
        return inst

    def test_auto_mode_actually_activates(self):
        """sampling_mode='auto' must reach the monitor — not be skipped by a guard."""
        from amazon.opentelemetry.serviceevents.python_monitor import get_sampling_mode

        inst = self._init_with_mode("auto")
        self.assertTrue(inst._initialized)
        self.assertEqual(get_sampling_mode(), "auto")
        inst.shutdown()

    def test_never_mode_activates(self):
        from amazon.opentelemetry.serviceevents.python_monitor import get_sampling_mode

        inst = self._init_with_mode("never")
        self.assertEqual(get_sampling_mode(), "never")
        inst.shutdown()

    def test_invalid_mode_falls_back_without_aborting_init(self):
        """A stale/removed mode (e.g. 'adaptive') logs a warning and leaves the default in place,
        rather than raising and aborting the whole ServiceEvents init."""
        from amazon.opentelemetry.serviceevents.python_monitor import get_sampling_mode

        inst = self._init_with_mode("adaptive")
        # Init must still complete (collectors/emitter wired), unlike the old ValueError abort.
        self.assertTrue(inst._initialized)
        self.assertEqual(get_sampling_mode(), "always")
        inst.shutdown()


class TestGetServiceEventsInstrumentation(TestCase):
    """Test the get_serviceevents_instrumentation singleton accessor."""

    def setUp(self):
        # The accessor mutates a module-level singleton. Reset it before and after each
        # test so cases don't leak state into one another, and neutralize atexit so the
        # constructed instance never registers a real shutdown hook.
        import amazon.opentelemetry.serviceevents.serviceevents_instrumentation as se_module

        self._se_module = se_module
        self._original_singleton = se_module._serviceevents_instance
        se_module._serviceevents_instance = None

        patcher = patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.atexit")
        self.mock_atexit = patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self._restore_singleton)

    def _restore_singleton(self):
        self._se_module._serviceevents_instance = self._original_singleton

    def test_returns_none_when_no_instance_and_no_config(self):
        """No singleton and no config returns None."""
        from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import (
            get_serviceevents_instrumentation,
        )

        self.assertIsNone(get_serviceevents_instrumentation(None))

    def test_creates_singleton_on_first_config(self):
        """First call with a config constructs and returns the singleton."""
        from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import (
            get_serviceevents_instrumentation,
        )

        config = ServiceEventsConfig(service_name="first-service")
        inst = get_serviceevents_instrumentation(config)

        self.assertIsInstance(inst, ServiceEventsInstrumentation)
        self.assertIs(inst.config, config)

    def test_first_config_wins_and_second_config_ignored(self):
        """Once created, a later config is ignored and the original instance is returned."""
        from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import (
            get_serviceevents_instrumentation,
        )

        first = get_serviceevents_instrumentation(ServiceEventsConfig(service_name="first-service"))
        second = get_serviceevents_instrumentation(ServiceEventsConfig(service_name="second-service"))

        self.assertIs(second, first)
        self.assertEqual(second.config.service_name, "first-service")

    def test_returns_existing_instance_when_called_without_config(self):
        """A subsequent call without a config returns the existing singleton."""
        from amazon.opentelemetry.serviceevents.serviceevents_instrumentation import (
            get_serviceevents_instrumentation,
        )

        first = get_serviceevents_instrumentation(ServiceEventsConfig(service_name="first-service"))
        again = get_serviceevents_instrumentation(None)

        self.assertIs(again, first)


class TestServiceEventsOutputFileMode(TestCase):
    """Test the output_file (local-testing file exporter) wiring branches."""

    def setUp(self):
        # Neutralize atexit registration (see TestServiceEventsInstrumentation.setUp).
        patcher = patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.atexit")
        self.mock_atexit = patcher.start()
        self.addCleanup(patcher.stop)

        state_patcher = patch(
            "amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState"
        )
        self.mock_state = state_patcher.start()
        self.mock_state.get_instance.return_value = MagicMock()
        self.addCleanup(state_patcher.stop)

        hooks_patcher = patch(
            "amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks"
        )
        hooks_patcher.start()
        self.addCleanup(hooks_patcher.stop)

        fd, self.output_path = tempfile.mkstemp(suffix=".ndjson")
        os.close(fd)
        self.addCleanup(lambda: os.path.exists(self.output_path) and os.remove(self.output_path))

    def test_output_file_mode_builds_emitter_with_file_exporters(self):
        """output_file mode creates the OTLP emitter using CloudWatch file exporters."""
        config = ServiceEventsConfig(
            enabled=True,
            function_instrument_enabled=True,
            output_file=self.output_path,
            sampling_mode="always",
        )
        inst = ServiceEventsInstrumentation(config)
        inst.initialize()
        try:
            self.assertIsNotNone(inst._otlp_emitter)
            self.assertIsNotNone(inst._otlp_logger_provider)
            self.assertIsNotNone(inst._otlp_meter_provider)
        finally:
            inst.shutdown()

    def test_output_file_wins_over_logs_endpoint(self):
        """When both output_file and logs_endpoint are set, output_file takes precedence."""
        config = ServiceEventsConfig(
            enabled=True,
            function_instrument_enabled=False,
            output_file=self.output_path,
            logs_endpoint="http://localhost:4318/v1/logs",
            metrics_endpoint="http://localhost:4318/v1/metrics",
            sampling_mode="always",
        )
        inst = ServiceEventsInstrumentation(config)
        logger_name = "amazon.opentelemetry.serviceevents.serviceevents_instrumentation"
        with self.assertLogs(logger_name, level="INFO") as captured:
            inst.initialize()
        try:
            self.assertIsNotNone(inst._otlp_emitter)
            self.assertTrue(
                any("OUTPUT_FILE mode" in r.getMessage() for r in captured.records),
                "expected OUTPUT_FILE mode log line",
            )
        finally:
            inst.shutdown()


class TestServiceEventsResourceAttributes(TestCase):
    """Test platform resource attribute propagation into the OTLP Resource."""

    def setUp(self):
        # Neutralize atexit registration (see TestServiceEventsInstrumentation.setUp).
        patcher = patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.atexit")
        self.mock_atexit = patcher.start()
        self.addCleanup(patcher.stop)

    def test_platform_attributes_added_to_resource(self):
        """Populated ResourceAttributes flow through to the OTel Resource."""
        resource_attributes = ResourceAttributes(
            cloud_provider="aws",
            cloud_region="us-west-2",
            host_id="i-0abc123",
            container_id="container-xyz",
        )
        config = ServiceEventsConfig(
            enabled=True,
            function_instrument_enabled=False,
            logs_endpoint="http://localhost:4316/v1/logs",
            metrics_endpoint="http://localhost:4316/v1/metrics",
            sampling_mode="always",
            resource_attributes=resource_attributes,
        )
        inst = ServiceEventsInstrumentation(config)
        inst.initialize()
        try:
            attrs = inst._otlp_logger_provider._resource.attributes
            self.assertEqual(attrs.get("cloud.provider"), "aws")
            self.assertEqual(attrs.get("cloud.region"), "us-west-2")
            self.assertEqual(attrs.get("host.id"), "i-0abc123")
            self.assertEqual(attrs.get("container.id"), "container-xyz")
        finally:
            inst.shutdown()


class TestServiceEventsLatencyThresholds(TestCase):
    """Test latency-threshold pattern wiring into the IncidentSnapshotCollector."""

    def setUp(self):
        # Neutralize atexit registration (see TestServiceEventsInstrumentation.setUp).
        patcher = patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.atexit")
        self.mock_atexit = patcher.start()
        self.addCleanup(patcher.stop)

    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState")
    @patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks")
    def test_latency_patterns_applied_to_incident_collector(self, _mock_hooks, mock_state):
        """Configured latency thresholds are applied to the IncidentSnapshotCollector."""
        mock_state.get_instance.return_value = MagicMock()
        config = ServiceEventsConfig(
            enabled=True,
            function_instrument_enabled=False,
            latency_thresholds=["POST /api/checkout:500", "GET /api/health:50"],
            sampling_mode="always",
        )
        inst = ServiceEventsInstrumentation(config)
        logger_name = "amazon.opentelemetry.serviceevents.serviceevents_instrumentation"
        with self.assertLogs(logger_name, level="INFO") as captured:
            inst.initialize()
        try:
            incident_collector = next(c for c in inst.collectors if c.__class__.__name__ == "IncidentSnapshotCollector")
            patterns = incident_collector.get_all_latency_threshold_patterns()
            self.assertIn(("POST /api/checkout", 500.0), patterns)
            self.assertTrue(
                any("latency threshold patterns" in r.getMessage() for r in captured.records),
                "expected latency-threshold log line",
            )
        finally:
            inst.shutdown()


class TestServiceEventsFrameworkHooks(TestCase):
    """Test the Flask/FastAPI/Django framework-hook error branches in initialize()."""

    def setUp(self):
        # Neutralize atexit registration (see TestServiceEventsInstrumentation.setUp).
        patcher = patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.atexit")
        self.mock_atexit = patcher.start()
        self.addCleanup(patcher.stop)

        state_patcher = patch(
            "amazon.opentelemetry.serviceevents.serviceevents_instrumentation._ServiceEventsMonitorState"
        )
        self.mock_state = state_patcher.start()
        self.mock_state.get_instance.return_value = MagicMock()
        self.addCleanup(state_patcher.stop)

        hooks_patcher = patch(
            "amazon.opentelemetry.serviceevents.serviceevents_instrumentation.install_ast_hooks"
        )
        hooks_patcher.start()
        self.addCleanup(hooks_patcher.stop)

    def _make_instrumentation(self):
        config = ServiceEventsConfig(
            enabled=True,
            function_instrument_enabled=False,
            sampling_mode="always",
        )
        return ServiceEventsInstrumentation(config)

    def test_framework_import_errors_are_handled(self):
        """Missing Flask/FastAPI/Django modules are skipped via ImportError handling."""
        # Setting the module entries to None makes the deferred imports raise ImportError,
        # exercising the "framework not installed" debug branches without uninstalling
        # the real packages.
        blocked = {
            "amazon.opentelemetry.serviceevents.instrumentation.flask_instrumentation": None,
            "amazon.opentelemetry.serviceevents.instrumentation.fastapi_instrumentation": None,
            "amazon.opentelemetry.serviceevents.instrumentation.django_instrumentation": None,
        }
        inst = self._make_instrumentation()
        with patch.dict(sys.modules, blocked):
            inst.initialize()
        try:
            self.assertTrue(inst._initialized)
        finally:
            inst.shutdown()

    def test_framework_install_exceptions_are_handled(self):
        """Errors raised while installing framework hooks are caught, not propagated."""
        inst = self._make_instrumentation()
        flask_mod = (
            "amazon.opentelemetry.serviceevents.instrumentation.flask_instrumentation.install_flask_hooks"
        )
        fastapi_mod = (
            "amazon.opentelemetry.serviceevents.instrumentation." "fastapi_instrumentation.install_fastapi_hooks"
        )
        django_mod = (
            "amazon.opentelemetry.serviceevents.instrumentation.django_instrumentation.install_django_hooks"
        )
        with patch(flask_mod, side_effect=RuntimeError("flask boom")), patch(
            fastapi_mod, side_effect=RuntimeError("fastapi boom")
        ), patch(django_mod, side_effect=RuntimeError("django boom")):
            inst.initialize()
        try:
            # Initialization still completes despite the framework-hook failures.
            self.assertTrue(inst._initialized)
        finally:
            inst.shutdown()

    def test_register_at_fork_attribute_error_is_handled(self):
        """A platform without os.register_at_fork (e.g. Windows) is handled gracefully."""
        inst = self._make_instrumentation()
        with patch(
            "amazon.opentelemetry.serviceevents.serviceevents_instrumentation.os.register_at_fork",
            side_effect=AttributeError("not available"),
        ):
            inst.initialize()
        try:
            self.assertTrue(inst._initialized)
        finally:
            inst.shutdown()


class TestServiceEventsReinitializeAfterFork(TestCase):
    """Test _reinitialize_after_fork collector restart and error handling."""

    def setUp(self):
        # Neutralize atexit registration (see TestServiceEventsInstrumentation.setUp).
        patcher = patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.atexit")
        self.mock_atexit = patcher.start()
        self.addCleanup(patcher.stop)

    def test_reinitialize_resets_and_restarts_collectors(self):
        """After fork, every collector is reset and restarted, monitor state reset is called."""
        config = ServiceEventsConfig()
        inst = ServiceEventsInstrumentation(config)
        mock_collector1 = MagicMock()
        mock_collector2 = MagicMock()
        inst.collectors = [mock_collector1, mock_collector2]

        reset_mod = "amazon.opentelemetry.serviceevents.python_monitor.reset_after_fork"
        with patch(reset_mod) as mock_reset:
            inst._reinitialize_after_fork()

        mock_reset.assert_called_once()
        mock_collector1._reset_for_fork.assert_called_once()
        mock_collector1.start.assert_called_once()
        mock_collector2._reset_for_fork.assert_called_once()
        mock_collector2.start.assert_called_once()

    def test_reinitialize_handles_exception(self):
        """An error during post-fork reset is caught and does not propagate."""
        config = ServiceEventsConfig()
        inst = ServiceEventsInstrumentation(config)

        reset_mod = "amazon.opentelemetry.serviceevents.python_monitor.reset_after_fork"
        with patch(reset_mod, side_effect=RuntimeError("fork boom")):
            # Should not raise.
            inst._reinitialize_after_fork()


class TestServiceEventsShutdownEdgeCases(TestCase):
    """Test shutdown() edge cases: atexit/emitter errors and the outer guard."""

    def setUp(self):
        # Neutralize atexit registration (see TestServiceEventsInstrumentation.setUp).
        patcher = patch("amazon.opentelemetry.serviceevents.serviceevents_instrumentation.atexit")
        self.mock_atexit = patcher.start()
        self.addCleanup(patcher.stop)

    def test_shutdown_handles_atexit_unregister_failure(self):
        """A failure in atexit.unregister during shutdown is swallowed."""
        config = ServiceEventsConfig()
        inst = ServiceEventsInstrumentation(config)
        inst._initialized = True
        inst._atexit_registered = True
        self.mock_atexit.unregister.side_effect = RuntimeError("unregister boom")

        # Should not raise; shutdown still completes.
        inst.shutdown()

        self.assertFalse(inst._atexit_registered)
        self.assertFalse(inst._initialized)

    def test_shutdown_handles_emitter_shutdown_failure(self):
        """An error while shutting down the OTLP emitter is caught."""
        config = ServiceEventsConfig()
        inst = ServiceEventsInstrumentation(config)
        inst._initialized = True
        emitter = MagicMock()
        emitter.shutdown.side_effect = RuntimeError("emitter boom")
        inst._otlp_emitter = emitter

        # Should not raise.
        inst.shutdown()

        emitter.shutdown.assert_called_once()
        self.assertFalse(inst._initialized)

    def test_shutdown_handles_unexpected_error_in_body(self):
        """An unexpected error inside the shutdown body is caught by the outer guard."""
        config = ServiceEventsConfig()
        inst = ServiceEventsInstrumentation(config)
        inst._initialized = True
        # Iterating collectors raises -> exercises the outer try/except.
        bad_collectors = MagicMock()
        bad_collectors.__iter__.side_effect = RuntimeError("iteration boom")
        inst.collectors = bad_collectors

        # Should not raise.
        inst.shutdown()
