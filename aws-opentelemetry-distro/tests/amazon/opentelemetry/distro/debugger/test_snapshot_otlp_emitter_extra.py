# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Additional unit tests for SnapshotOtlpEmitter.

Focuses on endpoint resolution from the environment and lazy initialization
behavior not exercised by test_snapshot_otlp_emitter.py. The OTLP exporter and
provider are mocked so no real network connection or background thread is created.
"""

import os
import unittest
from unittest import mock

from amazon.opentelemetry.distro.debugger import _snapshot_otlp_emitter as emitter_module
from amazon.opentelemetry.distro.debugger._snapshot_models import Snapshot
from amazon.opentelemetry.distro.debugger._snapshot_otlp_emitter import (
    _DEFAULT_LOGS_ENDPOINT,
    _LOGS_ENDPOINT_ENV_VAR,
    SnapshotOtlpEmitter,
)
from opentelemetry.sdk._events import EventLoggerProvider
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogExporter, SimpleLogRecordProcessor


class TestEndpointResolution(unittest.TestCase):
    """Tests for OTLP logs endpoint resolution."""

    def test_explicit_endpoint_wins(self):
        emitter = SnapshotOtlpEmitter(logs_endpoint="http://explicit:4318/v1/logs")
        self.assertEqual(emitter._logs_endpoint, "http://explicit:4318/v1/logs")

    @mock.patch.dict(os.environ, {_LOGS_ENDPOINT_ENV_VAR: "http://from-env:4316/v1/logs"})
    def test_endpoint_from_env(self):
        emitter = SnapshotOtlpEmitter()
        self.assertEqual(emitter._logs_endpoint, "http://from-env:4316/v1/logs")

    @mock.patch.dict(os.environ, {_LOGS_ENDPOINT_ENV_VAR: "  http://padded:4316/v1/logs  "})
    def test_endpoint_from_env_is_stripped(self):
        emitter = SnapshotOtlpEmitter()
        self.assertEqual(emitter._logs_endpoint, "http://padded:4316/v1/logs")

    @mock.patch.dict(os.environ, {_LOGS_ENDPOINT_ENV_VAR: "   "})
    def test_whitespace_only_env_falls_back_to_default(self):
        emitter = SnapshotOtlpEmitter()
        self.assertEqual(emitter._logs_endpoint, _DEFAULT_LOGS_ENDPOINT)

    @mock.patch.dict(os.environ, {_LOGS_ENDPOINT_ENV_VAR: ""})
    def test_empty_env_falls_back_to_default(self):
        emitter = SnapshotOtlpEmitter()
        self.assertEqual(emitter._logs_endpoint, _DEFAULT_LOGS_ENDPOINT)

    def test_unset_env_falls_back_to_default(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(_LOGS_ENDPOINT_ENV_VAR, None)
            emitter = SnapshotOtlpEmitter()
        self.assertEqual(emitter._logs_endpoint, _DEFAULT_LOGS_ENDPOINT)


class TestEnsureInitialized(unittest.TestCase):
    """Tests for lazy initialization in _ensure_initialized."""

    @mock.patch.object(emitter_module, "BatchLogRecordProcessor")
    @mock.patch.object(emitter_module, "OTLPLogExporter")
    def test_initialization_succeeds_and_is_cached(self, mock_exporter, mock_processor):
        emitter = SnapshotOtlpEmitter(logs_endpoint="http://localhost:4316/v1/logs")
        self.assertTrue(emitter._ensure_initialized())
        self.assertIsNotNone(emitter._event_logger)
        # Exporter created with the resolved endpoint.
        mock_exporter.assert_called_once_with(endpoint="http://localhost:4316/v1/logs")

        # Second call short-circuits without rebuilding the exporter.
        self.assertTrue(emitter._ensure_initialized())
        mock_exporter.assert_called_once()

    @mock.patch.object(emitter_module, "OTLPLogExporter", side_effect=RuntimeError("boom"))
    def test_initialization_failure_is_permanent(self, mock_exporter):
        emitter = SnapshotOtlpEmitter()
        self.assertFalse(emitter._ensure_initialized())
        self.assertTrue(emitter._init_failed)
        # Subsequent calls short-circuit on the failure flag without retrying.
        self.assertFalse(emitter._ensure_initialized())
        mock_exporter.assert_called_once()

    def test_init_failed_flag_short_circuits(self):
        emitter = SnapshotOtlpEmitter()
        emitter._init_failed = True
        self.assertFalse(emitter._ensure_initialized())

    @mock.patch.dict(os.environ, {"OTEL_SERVICE_NAME": "my-service"})
    @mock.patch.object(emitter_module, "BatchLogRecordProcessor")
    @mock.patch.object(emitter_module, "OTLPLogExporter")
    def test_default_resource_picks_up_service_name_from_env(self, _mock_exporter, _mock_processor):
        # Regression: default resource must run OTel detectors so OTEL_SERVICE_NAME
        # propagates onto OTLP-exported LogRecords. Resource.get_empty() skipped them.
        emitter = SnapshotOtlpEmitter()
        self.assertTrue(emitter._ensure_initialized())
        self.assertEqual(emitter._logger_provider.resource.attributes.get("service.name"), "my-service")

    @mock.patch.object(emitter_module, "BatchLogRecordProcessor")
    @mock.patch.object(emitter_module, "OTLPLogExporter")
    def test_explicit_resource_is_preserved(self, _mock_exporter, _mock_processor):
        from opentelemetry.sdk.resources import Resource

        explicit = Resource.create({"service.name": "explicit-service"})
        emitter = SnapshotOtlpEmitter(resource=explicit)
        self.assertTrue(emitter._ensure_initialized())
        self.assertEqual(emitter._logger_provider.resource.attributes.get("service.name"), "explicit-service")


class TestShutdownAndReset(unittest.TestCase):
    """Tests for shutdown and reset lifecycle."""

    def test_shutdown_flushes_and_clears_provider(self):
        emitter = SnapshotOtlpEmitter()
        provider = mock.MagicMock()
        emitter._logger_provider = provider
        emitter._event_logger = mock.MagicMock()

        emitter.shutdown()

        provider.force_flush.assert_called_once()
        provider.shutdown.assert_called_once()
        self.assertIsNone(emitter._logger_provider)
        self.assertIsNone(emitter._event_logger)

    def test_shutdown_without_provider_is_noop(self):
        emitter = SnapshotOtlpEmitter()
        emitter._logger_provider = None
        # Should not raise.
        emitter.shutdown()

    def test_shutdown_swallows_errors(self):
        emitter = SnapshotOtlpEmitter()
        provider = mock.MagicMock()
        provider.force_flush.side_effect = RuntimeError("boom")
        emitter._logger_provider = provider
        emitter._event_logger = mock.MagicMock()
        # Should not raise; provider/event_logger are still cleared.
        emitter.shutdown()
        self.assertIsNone(emitter._logger_provider)
        self.assertIsNone(emitter._event_logger)

    def test_reset_clears_state_and_failure_flag(self):
        emitter = SnapshotOtlpEmitter()
        emitter._logger_provider = mock.MagicMock()
        emitter._event_logger = mock.MagicMock()
        emitter._init_failed = True

        emitter.reset()

        self.assertIsNone(emitter._logger_provider)
        self.assertIsNone(emitter._event_logger)
        self.assertFalse(emitter._init_failed)


class TestEmitSnapshot(unittest.TestCase):
    """Tests for emit_snapshot using an in-memory exporter."""

    def setUp(self):
        self.log_exporter = InMemoryLogExporter()
        self.logger_provider = LoggerProvider()
        self.logger_provider.add_log_record_processor(SimpleLogRecordProcessor(self.log_exporter))
        self.emitter = SnapshotOtlpEmitter()
        self.emitter._logger_provider = self.logger_provider
        event_logger_provider = EventLoggerProvider(logger_provider=self.logger_provider)
        self.emitter._event_logger = event_logger_provider.get_event_logger("aws.dynamic_instrumentation", "1.0")

    def tearDown(self):
        self.logger_provider.shutdown()

    def test_instrumentation_type_attribute_set(self):
        snapshot = Snapshot(
            timestamp=1772082470861,
            location_hash="hash-1",
            instrumentation_type="PROBE",
        )
        self.emitter.emit_snapshot(snapshot)

        logs = self.log_exporter.get_finished_logs()
        self.assertEqual(len(logs), 1)
        attrs = dict(logs[0].log_record.attributes)
        self.assertEqual(attrs["aws.di.instrumentation_type"], "PROBE")

    def test_emit_does_not_raise_when_init_fails(self):
        emitter = SnapshotOtlpEmitter()
        emitter._init_failed = True
        # Skipped silently; no exception, no logs.
        emitter.emit_snapshot(Snapshot(timestamp=1, location_hash="x"))

    def test_emit_swallows_internal_errors(self):
        # A snapshot whose attribute access raises is handled by the broad except.
        broken = mock.MagicMock()
        type(broken).instrumentation = mock.PropertyMock(side_effect=RuntimeError("boom"))
        # Should not raise.
        self.emitter.emit_snapshot(broken)


if __name__ == "__main__":
    unittest.main()
