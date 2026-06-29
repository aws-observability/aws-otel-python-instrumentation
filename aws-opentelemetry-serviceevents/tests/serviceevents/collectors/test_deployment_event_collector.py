# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase
from unittest.mock import MagicMock

from amazon.opentelemetry.serviceevents.collectors.deployment_event_collector import DeploymentEventCollector


class TestDeploymentEventCollector(TestCase):
    """Trigger labeling and fork re-arm for DeploymentEventCollector."""

    def _build(self):
        emitter = MagicMock()
        collector = DeploymentEventCollector(
            flush_interval_ms=86_400_000,
            environment="testing",
            service_name="test-svc",
            otlp_emitter=emitter,
        )
        # The real loop sets _running=True before calling collect(); mirror that so the
        # trigger labeling ("periodic" vs "shutdown") matches a live collector without
        # spinning up the background daemon thread.
        collector._running = True
        return collector, emitter

    def _triggers(self, emitter):
        return [call.kwargs.get("trigger") for call in emitter.emit_deployment_event.call_args_list]

    def test_first_collect_is_startup_then_periodic(self):
        """First emit is 'startup'; subsequent emits while running are 'periodic'."""
        collector, emitter = self._build()

        collector.collect()
        collector.collect()

        self.assertEqual(self._triggers(emitter), ["startup", "periodic"])

    def test_final_collect_after_stop_is_shutdown(self):
        """After _running flips false (stop), the next collect is labeled 'shutdown'."""
        collector, emitter = self._build()
        collector.collect()  # startup
        collector._running = False

        collector.collect()

        self.assertEqual(self._triggers(emitter)[-1], "shutdown")

    def test_reset_for_fork_rearms_startup(self):
        """Each forked worker re-emits its own 'startup' on first collect after fork."""
        collector, emitter = self._build()
        collector.collect()  # parent: startup, _first_collect now False
        self.assertFalse(collector._first_collect)

        collector._reset_for_fork()

        self.assertTrue(collector._first_collect)
        collector.collect()
        self.assertEqual(self._triggers(emitter)[-1], "startup")

    def test_collect_no_emitter_is_noop(self):
        """With no emitter configured, collect() returns early without arming the startup flag."""
        collector = DeploymentEventCollector(
            flush_interval_ms=86_400_000,
            environment="testing",
            service_name="test-svc",
            otlp_emitter=None,
        )
        collector._running = True

        collector.collect()

        # Early return happens before the startup-flag transition.
        self.assertTrue(collector._first_collect)

    def test_collect_swallows_emit_errors(self):
        """An emitter failure is swallowed so telemetry never crashes the customer app."""
        collector, emitter = self._build()
        emitter.emit_deployment_event.side_effect = RuntimeError("boom")

        # Should not raise despite the emitter blowing up.
        collector.collect()

        emitter.emit_deployment_event.assert_called_once()
