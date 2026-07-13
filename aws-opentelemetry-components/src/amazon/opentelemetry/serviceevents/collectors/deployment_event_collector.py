# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""DeploymentEventCollector — emits aws.service_events.deployment_event at startup and every flush interval."""

import logging
from typing import Optional

from amazon.opentelemetry.serviceevents.collectors.base_collector import BaseCollector
from amazon.opentelemetry.serviceevents.models import DeploymentEventTelemetry, ResourceAttributes

logger = logging.getLogger(__name__)


class DeploymentEventCollector(BaseCollector):
    """Emits DeploymentEvent every flush_interval_ms, independent of AST mode.

    BaseCollector._run_loop calls collect() immediately on start and then once per
    flush_interval_ms thereafter, so the first emit happens at startup and subsequent
    emits at +interval, +2*interval, etc.

    Trigger attribute:
    - First collect() call -> "startup"
    - Subsequent calls while running -> "periodic"
    - Final collect() after stop (BaseCollector._running is False) -> "shutdown"
    """

    def __init__(
        self,
        flush_interval_ms: int,
        environment: Optional[str] = None,
        service_name: Optional[str] = None,
        sdk_version: str = "",
        resource_attributes: Optional[ResourceAttributes] = None,
        otlp_emitter=None,
    ):
        super().__init__(flush_interval_ms, "DeploymentEventCollector", otlp_emitter)
        # Environment from config (None/empty when unset — omitted from emitted signals)
        self.environment = environment
        self.service_name = service_name or "UnknownService"
        self.sdk_version = sdk_version
        self.resource_attributes = resource_attributes or ResourceAttributes()
        self._first_collect = True

    def _reset_for_fork(self):
        """Reset collector state after fork.

        Re-arm the startup emit so each forked worker (e.g. a gunicorn worker) emits its
        own "startup" DeploymentEvent on its first collect. Without this the child inherits
        _first_collect=False and its first emit is mislabeled "periodic", losing the
        per-worker startup signal.
        """
        super()._reset_for_fork()
        self._first_collect = True

    def collect(self):
        if not self.otlp_emitter:
            return
        if self._first_collect:
            trigger = "startup"
            self._first_collect = False
        elif not self._running:
            trigger = "shutdown"
        else:
            trigger = "periodic"
        try:
            event = DeploymentEventTelemetry.create(
                service_name=self.service_name,
                environment=self.environment,
                sdk_version=self.sdk_version,
                resource_attributes=self.resource_attributes,
            )
            self.otlp_emitter.emit_deployment_event(event, trigger=trigger)
            logger.info("Exported DeploymentEvent (trigger=%s)", trigger)
        except Exception:  # pylint: disable=broad-exception-caught
            # Telemetry must never crash the customer app; swallow all errors.
            logger.warning("Failed to emit DeploymentEvent", exc_info=True)
