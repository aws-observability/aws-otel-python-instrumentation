# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Main entry point for the refactored debugger.

This module provides a simple facade for initializing and managing the debugger,
including environment variable configuration and lifecycle management.
"""

import logging
import os
from typing import Optional

try:
    from opentelemetry.trace import TracerProvider, get_tracer_provider
except ImportError:
    TracerProvider = None
    get_tracer_provider = None

from amazon.opentelemetry.distro.debugger._debugger_client import (
    DEFAULT_BREAKPOINT_POLL_INTERVAL,
    DEFAULT_PROBE_POLL_INTERVAL,
    DebuggerClient,
)
from amazon.opentelemetry.distro.debugger._status_reporter import StatusReporter
from amazon.opentelemetry.distro.debugger.instrumentation_manager import get_global_manager, initialize_global_manager

logger = logging.getLogger(__name__)

# Environment variables for configuration (aligned with Java ADOT agent naming)
OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED = "OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED"
OTEL_AWS_DYNAMIC_INSTRUMENTATION_API_URL = "OTEL_AWS_DYNAMIC_INSTRUMENTATION_API_URL"
OTEL_AWS_DYNAMIC_INSTRUMENTATION_PROBE_POLL_INTERVAL = "OTEL_AWS_DYNAMIC_INSTRUMENTATION_PROBE_POLL_INTERVAL"
OTEL_AWS_DYNAMIC_INSTRUMENTATION_BREAKPOINT_POLL_INTERVAL = "OTEL_AWS_DYNAMIC_INSTRUMENTATION_BREAKPOINT_POLL_INTERVAL"

# Global debugger client instance
_global_debugger_client: Optional[DebuggerClient] = None
# PID at which the debugger was initialized — used to detect fork
_initialized_pid: Optional[int] = None


def is_debugger_enabled() -> bool:
    """
    Check if debugger is enabled via environment variable.

    Disabled by default (opt-in). Set OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED=true to enable.

    Returns:
        True if debugger is explicitly enabled, False otherwise (default)
    """
    enabled = os.environ.get(OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED, "false")
    return enabled.strip().lower() == "true"


def get_debugger_config() -> dict:
    """
    Get debugger configuration from environment variables.

    Returns:
        Dictionary with debugger configuration
    """
    api_url = os.environ.get(OTEL_AWS_DYNAMIC_INSTRUMENTATION_API_URL, "http://localhost:2000")
    probe_poll_interval_str = os.environ.get(
        OTEL_AWS_DYNAMIC_INSTRUMENTATION_PROBE_POLL_INTERVAL, str(DEFAULT_PROBE_POLL_INTERVAL)
    )
    breakpoint_poll_interval_str = os.environ.get(
        OTEL_AWS_DYNAMIC_INSTRUMENTATION_BREAKPOINT_POLL_INTERVAL,
        str(DEFAULT_BREAKPOINT_POLL_INTERVAL),
    )

    def _parse_interval(raw: str, name: str, default: int) -> int:
        try:
            value = int(raw)
            if value < 1:
                logger.warning("Invalid %s poll interval %d, using default %d", name, value, default)
                return default
            return value
        except ValueError:
            logger.warning("Invalid %s poll interval '%s', using default %d", name, raw, default)
            return default

    probe_poll_interval = _parse_interval(probe_poll_interval_str, "probe", DEFAULT_PROBE_POLL_INTERVAL)
    breakpoint_poll_interval = _parse_interval(
        breakpoint_poll_interval_str, "breakpoint", DEFAULT_BREAKPOINT_POLL_INTERVAL
    )

    return {
        "api_url": api_url,
        "probe_poll_interval": probe_poll_interval,
        "breakpoint_poll_interval": breakpoint_poll_interval,
    }


def initialize_debugger(
    tracer_provider: Optional[TracerProvider] = None,
) -> bool:
    """
    Initialize the debugger with the provided tracer provider.

    This function initializes the InstrumentationManager and starts the
    DebuggerClient for configuration polling.

    Handles gunicorn/uWSGI prefork model: registers an ``os.register_at_fork``
    callback so that after fork(), the debugger is fully re-initialized in the
    child (worker) process. This is necessary because:
    - Daemon threads (poller, status reporter) don't survive fork()
    - The worker may re-import application modules, losing any monkey-patched wrappers
    - The global manager and client state from the master process are stale

    Args:
        tracer_provider: OpenTelemetry TracerProvider instance. If None,
                        will use the global tracer provider.

    Returns:
        True if initialization succeeded, False otherwise
    """
    if not is_debugger_enabled():
        logger.debug("Debugger is disabled")
        return False

    # Skip DI in Lambda environments where CloudWatch Agent is not available
    lambda_function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
    if lambda_function_name is not None and lambda_function_name.strip():
        logger.info("Lambda environment detected, skipping Dynamic Instrumentation")
        return False

    global _initialized_pid  # pylint: disable=global-statement

    try:
        # Use provided tracer provider or get the global one
        if tracer_provider is None and get_tracer_provider is not None:
            tracer_provider = get_tracer_provider()

        # Initialize the global InstrumentationManager
        manager = initialize_global_manager(tracer_provider)
        if manager is None:
            logger.error("Failed to initialize InstrumentationManager")
            return False

        logger.debug("InstrumentationManager initialized successfully")

        # Start the debugger client for configuration polling
        client = start_debugger_client()
        if client is None:
            logger.warning("Debugger initialized but client failed to start")
            return False

        # Initialize status reporter with client and manager
        if client and manager:
            manager._status_reporter = StatusReporter(client, manager)
            manager._status_reporter.start()
            logger.debug("Status reporter started")

        # Register a post-fork callback to re-initialize in child processes.
        # This handles gunicorn/uWSGI prefork workers where daemon threads
        # (poller, status reporter) don't survive fork() and the worker
        # re-imports application modules, losing any monkey-patched wrappers.
        _register_fork_handler()

        _initialized_pid = os.getpid()
        logger.info("Debugger initialized successfully (pid %d)", _initialized_pid)
        return True

    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.error("Failed to initialize debugger: %s", exception, exc_info=True)
        return False


def _reset_debugger_state():
    """
    Reset all global debugger state after a fork.

    After fork(), daemon threads from the parent process are dead, and the
    global singletons (client, manager) hold stale references. This function
    clears them so initialize_debugger() can start fresh in the worker.
    """
    global _global_debugger_client, _initialized_pid  # pylint: disable=global-statement

    logger.debug("Resetting debugger state for worker process (pid %d)", os.getpid())

    # Clear the client — its poller threads are dead after fork
    _global_debugger_client = None

    # Clear the global manager — it holds stale state from the master
    # Import here to avoid circular imports at module level
    # pylint: disable=import-outside-toplevel
    from amazon.opentelemetry.distro.debugger import instrumentation_manager

    instrumentation_manager._global_manager_instance = None  # pylint: disable=protected-access

    # Clear the stale snapshot emitter — its LoggerProvider background threads are dead after fork
    # pylint: disable=import-outside-toplevel
    from amazon.opentelemetry.distro.debugger._function_wrapper import get_snapshot_emitter, set_snapshot_emitter

    emitter = get_snapshot_emitter()
    if emitter and hasattr(emitter, "reset"):
        emitter.reset()
    set_snapshot_emitter(None)

    # Reset PID tracker
    _initialized_pid = None

    logger.debug("Debugger state reset complete")


_fork_handler_registered = False


def _register_fork_handler():
    """
    Register an os.register_at_fork callback to re-initialize the debugger
    in child processes after fork().

    This is critical for gunicorn/uWSGI prefork workers where:
    - Daemon threads (poller, status reporter) don't survive fork()
    - Workers re-import application modules, losing monkey-patched wrappers
    - Global state (manager, client) from the master is stale

    The callback is registered at most once (idempotent).
    Only available on Python 3.7+ (where os.register_at_fork exists).
    """
    global _fork_handler_registered  # pylint: disable=global-statement

    if _fork_handler_registered:
        return

    if not hasattr(os, "register_at_fork"):
        logger.debug("os.register_at_fork not available, skipping fork handler registration")
        return

    def _after_fork_in_child():
        """Re-initialize debugger in the child process after fork."""
        try:
            logger.info("Post-fork: re-initializing debugger in worker (pid %d)", os.getpid())
            _reset_debugger_state()
            initialize_debugger()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Post-fork debugger re-initialization failed: %s", exc)

    os.register_at_fork(after_in_child=_after_fork_in_child)
    _fork_handler_registered = True
    logger.debug("Registered post-fork handler for debugger re-initialization")


def start_debugger_client() -> Optional[DebuggerClient]:
    """
    Start the debugger client for configuration polling.

    Returns:
        DebuggerClient instance if started successfully, None otherwise
    """
    global _global_debugger_client  # pylint: disable=global-statement

    if _global_debugger_client is not None:
        logger.warning("Debugger client is already running")
        return _global_debugger_client

    try:
        # Check if manager is initialized
        manager = get_global_manager()
        if manager is None:
            logger.error("Cannot start debugger client: InstrumentationManager not initialized")
            return None

        # Get client configuration
        config = get_debugger_config()
        api_url = config["api_url"]
        probe_poll_interval = config["probe_poll_interval"]
        breakpoint_poll_interval = config["breakpoint_poll_interval"]

        # Create and start client
        _global_debugger_client = DebuggerClient(
            api_url=api_url,
            probe_poll_interval=probe_poll_interval,
            breakpoint_poll_interval=breakpoint_poll_interval,
        )
        _global_debugger_client.start_polling()

        logger.info("Debugger client started")
        logger.debug("API URL: %s", api_url)
        logger.debug("Service: %s", _global_debugger_client.service_name)

        return _global_debugger_client

    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.error("Failed to start debugger client: %s", exception, exc_info=True)
        return None


def stop_debugger_client():
    """Stop the debugger client if it's running."""
    global _global_debugger_client  # pylint: disable=global-statement

    if _global_debugger_client is not None:
        try:
            _global_debugger_client.stop_polling()
            logger.info("Debugger client stopped")
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error stopping debugger client: %s", exception)
        finally:
            _global_debugger_client = None


def get_debugger_client() -> Optional[DebuggerClient]:
    """
    Get the global debugger client instance.

    Returns:
        DebuggerClient instance if running, None otherwise
    """
    return _global_debugger_client


def cleanup_debugger():
    """
    Clean up debugger resources.

    This function should be called during application shutdown to properly
    clean up debugger resources.
    """
    try:
        # Stop debugger client
        stop_debugger_client()

        # Flush and shutdown the OTLP snapshot emitter's LoggerProvider
        # pylint: disable=import-outside-toplevel
        from amazon.opentelemetry.distro.debugger._function_wrapper import get_snapshot_emitter

        emitter = get_snapshot_emitter()
        if emitter and hasattr(emitter, "shutdown"):
            try:
                emitter.shutdown()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("Error shutting down snapshot emitter: %s", exc)

        # Clear all instrumentation
        manager = get_global_manager()
        if manager:
            # Get all instrumented functions and remove them
            status = manager.get_status()
            for func_info in status.get("functions", []):
                function_key = func_info.get("function_key")
                if function_key:
                    try:
                        manager._remove_function(function_key)
                    except Exception as exception:  # pylint: disable=broad-exception-caught
                        logger.warning("Error removing function %s: %s", function_key, exception)

            logger.info("Debugger cleanup completed")

    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.error("Error during debugger cleanup: %s", exception, exc_info=True)
