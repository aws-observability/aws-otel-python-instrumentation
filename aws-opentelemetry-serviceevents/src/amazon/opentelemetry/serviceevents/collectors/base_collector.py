# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Base collector class for periodic telemetry collection.
"""

import logging
import threading
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """
    Base class for periodic telemetry collectors.

    Provides common functionality for background collection threads,
    start/stop lifecycle, and periodic flushing.
    """

    def __init__(self, flush_interval_ms: int, name: str, otlp_emitter=None):
        """
        Initialize the base collector.

        Args:
            flush_interval_ms: How often to collect data (milliseconds)
            name: Name of the collector for logging
            otlp_emitter: Optional ServiceEventsOtlpEmitter for OTLP export
        """
        self.flush_interval_ms = flush_interval_ms
        self.name = name
        self.otlp_emitter = otlp_emitter
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._running = False

    @property
    def flush_interval_sec(self):
        """Derived from flush_interval_ms to ensure consistency."""
        return self.flush_interval_ms / 1000.0

    def start(self):
        """Start the collector background thread."""
        if self._running:
            logger.warning("%s already running", self.name)
            return

        logger.info("Starting %s (interval: %sms)", self.name, self.flush_interval_ms)
        self._stop_event.clear()
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name=self.name)
        self._thread.start()

    def stop(self):
        """Stop the collector background thread."""
        if not self._running:
            return

        logger.info("Stopping %s", self.name)
        self._running = False
        self._stop_event.set()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                logger.warning("%s thread did not stop cleanly", self.name)

    def _reset_for_fork(self):
        """Reset collector state after fork. The old thread is dead in the child process."""
        self._stop_event = threading.Event()
        self._thread = None
        self._running = False

    def _run_loop(self):
        """Main collection loop (runs in background thread)."""
        logger.debug("%s collection loop started", self.name)

        try:
            while not self._stop_event.is_set():
                try:
                    # Collect and export data
                    self.collect()
                # telemetry must never crash host app
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.error("Error in %s collection: %s", self.name, exc, exc_info=True)

                # Wait for next collection interval (interruptible)
                self._stop_event.wait(timeout=self.flush_interval_sec)

            # Final collection on shutdown
            try:
                logger.debug("%s performing final collection", self.name)
                self.collect()
            except Exception as exc:  # pylint: disable=broad-exception-caught  # telemetry must never crash host app
                logger.error("Error in %s final collection: %s", self.name, exc, exc_info=True)

        finally:
            logger.debug("%s collection loop stopped", self.name)

    @abstractmethod
    def collect(self):
        """
        Collect and export telemetry data.

        This method is called periodically by the background thread.
        Subclasses must implement this method to define collection behavior.
        """
