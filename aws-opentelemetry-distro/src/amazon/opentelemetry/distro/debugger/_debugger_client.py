# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Refactored SDK client for debugger API - simplified to use InstrumentationManager.
"""

import json
import logging
import os
import random
import threading
import time
from typing import Any, Dict, List, Optional

from amazon.opentelemetry.distro._aws_resource_attribute_configurator import _OTEL_UNKNOWN_SERVICE_PREFIX
from amazon.opentelemetry.distro._aws_span_processing_util import UNKNOWN_SERVICE
from amazon.opentelemetry.distro.debugger._data_models import BreakpointConfiguration
from amazon.opentelemetry.distro.debugger.instrumentation_manager import get_global_manager
from opentelemetry import trace
from opentelemetry.semconv.resource import ResourceAttributes

try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None

logger = logging.getLogger(__name__)

# Debugger client configuration constants
DEFAULT_PROBE_POLL_INTERVAL = 600  # 10 minutes for PROBE configs
DEFAULT_BREAKPOINT_POLL_INTERVAL = 60  # 1 minute for BREAKPOINT configs
DEFAULT_API_URL = "http://localhost:2000"  # Default debugger API URL
BASE_BACKOFF_INTERVAL = 10  # Base interval for exponential backoff (seconds)
MAX_BACKOFF_ATTEMPTS = 3  # Maximum number of backoff attempts for initial fetch
DEGRADED_POLL_INTERVAL = 300  # 5 minutes — used when API endpoint is unreachable


class DebuggerClient:
    """SDK client for fetching configuration from debugger API."""

    def __init__(
        self,
        probe_poll_interval: int,
        breakpoint_poll_interval: int,
        service_name: Optional[str] = None,
        api_url: Optional[str] = None,
        timeout: int = 30,  # HTTP request timeout in seconds
    ):
        """Initialize the debugger client.

        Args:
            probe_poll_interval: Poll interval for PROBE configs in seconds
            breakpoint_poll_interval: Poll interval for BREAKPOINT configs in seconds
            service_name: Service name for configuration (auto-discovered if None)
            api_url: API Proxy URL
            timeout: HTTP request timeout in seconds (default: 30)
        """
        if not REQUESTS_AVAILABLE:
            raise ImportError("requests library is required for DebuggerClient")

        self._service_name_override = service_name
        self._cached_service_name: Optional[str] = None
        self._cached_environment: Optional[str] = None
        self.proxy_url = api_url or self._get_api_url()
        self.timeout = timeout
        self.probe_poll_interval = probe_poll_interval
        self.breakpoint_poll_interval = breakpoint_poll_interval

        # Internal state
        self._poller: Optional["ConfigurationPoller"] = None
        self._session = self._create_session()

        logger.debug("Initialized DebuggerClient")
        logger.debug("Proxy URL: %s", self.proxy_url)
        logger.debug("PROBE poll interval: %ds", self.probe_poll_interval)
        logger.debug("BREAKPOINT poll interval: %ds", self.breakpoint_poll_interval)

    @property
    def service_name(self) -> str:
        """
        Get service name from OpenTelemetry resource or environment variable.

        Only caches successful service name resolution to handle timing issues with Resource population.
        If service name is not yet available, returns fallback without caching,
        allowing automatic retry on next call.
        """
        if self._service_name_override:
            return self._service_name_override

        if self._cached_service_name:
            return self._cached_service_name

        # Try OpenTelemetry resource first
        try:
            # Always get fresh tracer provider to handle ProxyTracerProvider -> Real TracerProvider transition
            tracer_provider = trace.get_tracer_provider()
            global_resource = tracer_provider.resource

            service_name = global_resource.attributes.get("service.name")

            if service_name and not service_name.startswith(_OTEL_UNKNOWN_SERVICE_PREFIX):
                # SUCCESS! Cache it so we never query again
                self._cached_service_name = service_name
                logger.debug("Service name resolved and cached: %s", service_name)
                return service_name
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.debug("Error getting service name from OpenTelemetry resource: %s", exception)

        # Fall back to environment variable
        service_name = os.environ.get("OTEL_SERVICE_NAME")
        if service_name:
            # Cache environment variable value as well
            self._cached_service_name = service_name
            logger.debug("Service name from environment variable: %s", service_name)
            return service_name

        # Attribute not available yet - don't cache, try again next time
        logger.debug("service.name attribute not yet available, will retry on next call")
        return UNKNOWN_SERVICE  # Don't cache - Resource might populate later

    @property
    def environment(self) -> str:
        """
        Get environment from OpenTelemetry resource (lazy-loaded, cached after successful resolution).

        Only caches successful environment resolution to handle timing issues with Resource population.
        If environment is not yet available, returns "UnknownEnvironment" without caching,
        allowing automatic retry on next call.
        """
        # Return cached value if we successfully found it before
        if self._cached_environment:
            return self._cached_environment

        # Try OpenTelemetry resource first
        try:
            # Always get fresh tracer provider to handle ProxyTracerProvider -> Real TracerProvider transition
            tracer_provider = trace.get_tracer_provider()
            global_resource = tracer_provider.resource

            # Try deployment.environment.name first, then DEPLOYMENT_ENVIRONMENT
            environment = global_resource.attributes.get(
                "deployment.environment.name"
            ) or global_resource.attributes.get(ResourceAttributes.DEPLOYMENT_ENVIRONMENT)

            if environment:
                # SUCCESS! Cache it so we never query again
                self._cached_environment = environment
                logger.debug("Deployment environment resolved and cached: %s", environment)
                return environment
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.debug("Error getting environment from OpenTelemetry resource: %s", exception)

        # Attribute not available yet - don't cache, try again next time
        logger.debug("deployment.environment.name attribute not yet available, will retry on next call")
        return "UnknownEnvironment"  # Don't cache - Resource might populate later

    @staticmethod
    def _get_api_url() -> str:
        """Get proxy URL from environment variable or use default."""
        proxy_url = os.environ.get("OTEL_AWS_DYNAMIC_INSTRUMENTATION_API_URL")
        if proxy_url:
            logger.debug("Using proxy URL from environment variable: %s", proxy_url)
            return proxy_url

        return DEFAULT_API_URL

    @staticmethod
    def _create_session() -> "requests.Session":
        """Create HTTP session for API calls."""
        session = requests.Session()
        session.headers.update({"User-Agent": "DebuggerClient/1.0"})
        return session

    def start_polling(self) -> None:
        """Start periodic polling for configuration updates."""
        if self._poller is not None:
            logger.warning("Polling already started")
            return

        self._poller = ConfigurationPoller(self)
        self._poller.start()
        logger.debug(
            "Started configuration polling - PROBE: %ds, BREAKPOINT: %ds",
            self.probe_poll_interval,
            self.breakpoint_poll_interval,
        )

    def stop_polling(self) -> None:
        """Stop periodic polling."""
        if self._poller is None:
            logger.warning("Polling not started")
            return

        self._poller.stop()
        self._poller = None
        logger.debug("Stopped configuration polling")

    def fetch_configuration_by_type(
        self, instrumentation_type: str, last_sync_time: Optional[float] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch configuration for specific instrumentation type.

        Args:
            instrumentation_type: "PROBE" or "BREAKPOINT"
            last_sync_time: Last sync time to include in request

        Returns:
            Configuration from API, or None if fetch fails (graceful degradation)
        """
        try:
            # Suppress HTTP instrumentation to prevent recursive tracing
            try:
                # pylint: disable=import-outside-toplevel
                from opentelemetry.instrumentation.utils import suppress_http_instrumentation

                suppression_context = suppress_http_instrumentation()
            except ImportError:
                # pylint: disable=import-outside-toplevel
                from contextlib import nullcontext

                suppression_context = nullcontext()

            all_configs = []
            next_token = None
            response_last_sync_time = None
            next_sync_interval = None
            changed = True

            # TODO (srprash): This may not be true. Confirm the page size and adjust accordingly
            # TODO (srprash): Also, this may not be the best way to query paginated AWS APIs.
            # We should paginate until no NextToken is found
            # API returns max 50 results per page, so 3 pages covers 150 configs (more than enough)
            max_pages = 3
            with suppression_context:
                for _ in range(max_pages):
                    # Build payload
                    payload = {
                        "Service": self.service_name,
                        "Environment": self.environment,
                        "InstrumentationType": instrumentation_type,
                    }

                    if next_token and response_last_sync_time is not None:
                        payload["NextToken"] = next_token
                        payload["SyncedAt"] = response_last_sync_time
                    elif last_sync_time is not None:
                        payload["SyncedAt"] = last_sync_time

                    url = self.proxy_url + "/list-instrumentation-configurations"
                    logger.debug("Making request to: %s for %s", url, instrumentation_type)
                    response = self._session.post(url, json=payload, timeout=self.timeout)

                    # Check response status
                    if response.status_code == 200:
                        try:
                            raw_config = response.json()

                            logger.debug("Raw Config Response from /ListInstrumentationConfigurations: %s", raw_config)

                            if not raw_config:
                                raw_config = {}

                            # Store values from first response
                            if not next_token:
                                changed = raw_config.get("Changed", True)
                                response_last_sync_time = raw_config.get("SyncedAt") or raw_config.get("LastSyncTime")
                                next_sync_interval = raw_config.get("SyncInterval") or raw_config.get(
                                    "NextSyncInterval"
                                )

                            # Process LatestConfigurations and deserialize ConfigurationData
                            for item in raw_config.get("LatestConfigurations") or []:
                                config_item = item.copy()
                                config_data_str = item.get("ConfigurationData")
                                if config_data_str:
                                    config_item["ConfigurationData"] = json.loads(config_data_str)
                                # Ensure AttributeFilters is a list or None
                                if config_item.get("AttributeFilters") is None:
                                    config_item["AttributeFilters"] = []
                                all_configs.append(config_item)

                            # Check for next page
                            next_token = raw_config.get("NextToken")
                            if not next_token:
                                break

                        except json.JSONDecodeError as exception:
                            logger.error("Invalid JSON response: %s", exception)
                            return None

                    elif response.status_code == 400:
                        logger.error("Bad request: %s", response.text)
                        return None
                    elif response.status_code == 404:
                        logger.debug("No configuration found")
                        return {"Changed": False, "LatestConfigurations": []}
                    elif response.status_code >= 500:
                        logger.error("Server error (%d): %s", response.status_code, response.text)
                        return None
                    else:
                        logger.error("Unexpected response (%d): %s", response.status_code, response.text)
                        return None

            # Build final config
            config = {
                "Changed": changed,
                "SyncedAt": response_last_sync_time,
                "SyncInterval": next_sync_interval,
                "LatestConfigurations": all_configs,
            }

            logger.debug(
                "Fetched configuration for InstrumentationType: %s: %d configs",
                instrumentation_type,
                len(config["LatestConfigurations"]),
            )
            return config

        except requests.exceptions.Timeout:
            logger.error("Request timeout after %ds", self.timeout)
            return None
        except requests.exceptions.ConnectionError as exception:
            logger.error("Connection error: %s", exception)
            return None
        except requests.exceptions.RequestException as exception:
            logger.error("Request failed: %s", exception)
            return None
        except Exception as e:
            logger.error("Error fetching %s configuration: %s", instrumentation_type, e)
            return None


class ConfigurationPoller:
    """Background threads that poll the API for PROBE and BREAKPOINT configurations.

    This class manages two independent polling threads:
    - PROBE thread: Polls every 10 minutes (configurable)
    - BREAKPOINT thread: Polls every 1 minute (configurable)

    PROBE and BREAKPOINT configurations are cached separately and merged atomically before
    application via the InstrumentationManager.

    Thread Safety:
    - Uses a single shared lock (_config_lock) for PROBE/BREAKPOINT configuration operations
    - Ensures atomic merge-then-apply to prevent partial state

    Failure Handling:
    - Partial failures: If one type fails, continues with cached data for that type
    - Staleness detection: Logs warnings if configurations become stale
    - Graceful degradation: System continues operating with cached data during outages
    - Degraded polling: After 3 failed initial attempts, each poller independently enters a
      degraded polling mode (every 300s) and retries indefinitely until the API endpoint is available
    """

    # Staleness thresholds
    PROBE_STALENESS_THRESHOLD = 30 * 60  # 30 minutes
    BREAKPOINT_STALENESS_THRESHOLD = 5 * 60  # 5 minutes

    def __init__(self, client: DebuggerClient):
        """Initialize configuration poller.

        Args:
            client: DebuggerClient instance
        """
        self.client = client

        # Thread management
        self._probe_thread: Optional[threading.Thread] = None
        self._breakpoint_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._running = False

        # Synchronization - single lock for PROBE/BREAKPOINT config operations
        self._config_lock = threading.Lock()

        # Configuration caching for PROBE/BREAKPOINT
        self._cached_probe_configs: List[BreakpointConfiguration] = []
        self._cached_breakpoint_configs: List[BreakpointConfiguration] = []

        # State tracking for incremental sync
        self._probe_last_sync_time: Optional[float] = None
        self._breakpoint_last_sync_time: Optional[float] = None

        # Success tracking for staleness detection
        self._probe_last_success_time: Optional[float] = None
        self._breakpoint_last_success_time: Optional[float] = None

        logger.debug("Initialized ConfigurationPoller")

    def start(self) -> None:
        """Start all polling threads."""
        if self._running:
            logger.warning("Configuration poller already running")
            return

        self._stop_event.clear()
        self._running = True

        # Start PROBE polling thread
        self._probe_thread = threading.Thread(target=self._poll_probes_loop, name="ProbePoller", daemon=True)
        self._probe_thread.start()
        logger.debug("Started PROBE polling thread (interval: %d)", self.client.probe_poll_interval)

        # Start BREAKPOINT polling thread
        self._breakpoint_thread = threading.Thread(
            target=self._poll_breakpoints_loop, name="BreakpointPoller", daemon=True
        )
        self._breakpoint_thread.start()
        logger.debug("Started BREAKPOINT polling thread (interval: %d)", self.client.breakpoint_poll_interval)

        logger.debug("Configuration poller started")

    def stop(self) -> None:
        """Stop all polling threads and wait for completion."""
        if not self._running:
            logger.warning("Configuration poller not running")
            return

        logger.debug("Stopping configuration poller...")
        self._stop_event.set()

        # Wait for PROBE thread to finish
        if self._probe_thread and self._probe_thread.is_alive():
            self._probe_thread.join(timeout=5.0)
            if self._probe_thread.is_alive():
                logger.warning("PROBE thread did not stop within timeout")

        # Wait for BREAKPOINT thread to finish
        if self._breakpoint_thread and self._breakpoint_thread.is_alive():
            self._breakpoint_thread.join(timeout=5.0)
            if self._breakpoint_thread.is_alive():
                logger.warning("BREAKPOINT thread did not stop within timeout")

        self._running = False
        logger.debug("Configuration poller stopped")

    def _check_degraded_mode(self, poller_name: str, attempt: int) -> None:
        """Log a warning when entering degraded polling mode after repeated initial-fetch failures.

        Previously this was a circuit breaker that stopped all polling permanently. Now the poller
        transitions to a degraded polling interval (DEGRADED_POLL_INTERVAL seconds) and keeps
        retrying indefinitely until the API endpoint becomes available. Each poller operates
        independently.

        Args:
            poller_name: name of the poller (for logging)
            attempt: current attempt number
        """
        if attempt == MAX_BACKOFF_ATTEMPTS:
            logger.warning(
                "[%s] Dynamic Instrumentation API endpoint unreachable after %d attempts, "
                "entering degraded polling mode (every %ds). Will resume normal polling when "
                "the endpoint becomes available. Verify the API endpoint "
                "(OTEL_AWS_DYNAMIC_INSTRUMENTATION_API_URL) is reachable or set "
                "OTEL_AWS_DYNAMIC_INSTRUMENTATION_ENABLED=false to disable.",
                poller_name,
                MAX_BACKOFF_ATTEMPTS,
                DEGRADED_POLL_INTERVAL,
            )

    def _poll_probes_loop(self):
        logger.debug("Starting PROBE polling loop for %s : %s", self.client.service_name, self.client.environment)

        is_first_fetch = True
        attempt = 0

        while not self._stop_event.is_set():
            try:
                # Calculate wait interval
                if is_first_fetch:
                    if attempt >= MAX_BACKOFF_ATTEMPTS:
                        # Degraded mode: API endpoint unreachable, poll slowly until it becomes available
                        base_interval = DEGRADED_POLL_INTERVAL
                        jitter = random.uniform(0, DEGRADED_POLL_INTERVAL * 0.25)
                    else:
                        # Exponential backoff for initial fetch: [10, 30, 120] seconds
                        intervals = [BASE_BACKOFF_INTERVAL, BASE_BACKOFF_INTERVAL * 3, BASE_BACKOFF_INTERVAL * 12]
                        base_interval = intervals[min(attempt, len(intervals) - 1)]
                        jitter = random.uniform(0, BASE_BACKOFF_INTERVAL * 0.5)  # 0-5 seconds
                else:
                    # Regular polling with 0-25% jitter
                    base_interval = self.client.probe_poll_interval
                    jitter = random.uniform(0, self.client.probe_poll_interval * 0.25)

                # Wait before fetching
                if self._stop_event.wait(base_interval + jitter):
                    logger.debug("PROBE polling stopped during wait")
                    return

                # Fetch PROBE configuration
                logger.debug("Fetching probe configuration")
                config = self.client.fetch_configuration_by_type("PROBE", self._probe_last_sync_time)

                # Handle fetch failure
                if config is None:
                    if is_first_fetch:
                        attempt += 1
                        self._check_degraded_mode("PROBE", attempt)
                        logger.warning("[PROBE] Initial fetch attempt %d failed, will retry", attempt)
                        continue
                    else:
                        logger.warning("[PROBE] Fetch failed, continuing with cached configuration")
                        self._check_staleness()
                        continue

                # Update last sync time
                if config.get("SyncedAt"):
                    self._probe_last_sync_time = config.get("SyncedAt")

                # Update success time on any successful fetch (not just when Changed)
                self._probe_last_success_time = time.time()

                # Apply configurations if changed
                if config.get("Changed") is not False:
                    probe_configs = self._parse_api_response_to_configs(config)
                    self._apply_merged_configuration(new_probe_configs=probe_configs, new_breakpoint_configs=None)

                # Mark first fetch as complete
                if is_first_fetch:
                    logger.debug("Initial probe configuration fetch successful")
                    is_first_fetch = False
                    attempt = 0

            except Exception as exc:
                logger.error("Unexpected error during probe configuration polling: %s", exc)
                if is_first_fetch:
                    attempt += 1
                    self._check_degraded_mode("PROBE", attempt)

        logger.debug("Probe polling loop ended")

    def _poll_breakpoints_loop(self) -> None:
        logger.debug("Starting BREAKPOINT polling loop for %s : %s", self.client.service_name, self.client.environment)

        is_first_fetch = True
        attempt = 0

        while not self._stop_event.is_set():
            try:
                # Calculate wait interval
                if is_first_fetch:
                    if attempt >= MAX_BACKOFF_ATTEMPTS:
                        # Degraded mode: API endpoint unreachable, poll slowly until it becomes available
                        base_interval = DEGRADED_POLL_INTERVAL
                        jitter = random.uniform(0, DEGRADED_POLL_INTERVAL * 0.25)
                    else:
                        # Exponential backoff for initial fetch: [10, 30, 120] seconds
                        intervals = [BASE_BACKOFF_INTERVAL, BASE_BACKOFF_INTERVAL * 3, BASE_BACKOFF_INTERVAL * 12]
                        base_interval = intervals[min(attempt, len(intervals) - 1)]
                        jitter = random.uniform(0, BASE_BACKOFF_INTERVAL * 0.5)  # 0-5 seconds
                else:
                    # Regular polling with 0-25% jitter
                    base_interval = self.client.breakpoint_poll_interval
                    jitter = random.uniform(0, self.client.breakpoint_poll_interval * 0.25)

                # Wait before fetching
                if self._stop_event.wait(base_interval + jitter):
                    logger.debug("BREAKPOINT polling stopped during wait")
                    return

                # Fetch BREAKPOINT configuration
                logger.debug("Fetching breakpoint configuration")
                config = self.client.fetch_configuration_by_type("BREAKPOINT", self._breakpoint_last_sync_time)

                # Handle fetch failure
                if config is None:
                    if is_first_fetch:
                        attempt += 1
                        self._check_degraded_mode("BREAKPOINT", attempt)
                        logger.warning("[BREAKPOINT] Initial fetch attempt %d failed, will retry", attempt)
                        continue
                    else:
                        logger.warning("[BREAKPOINT] Fetch failed, continuing with cached configuration")
                        self._check_staleness()
                        continue

                # Update last_sync_time
                if config.get("SyncedAt"):
                    self._breakpoint_last_sync_time = config.get("SyncedAt")
                # Update poll interval if provided
                next_sync_interval = config.get("SyncInterval")
                if isinstance(next_sync_interval, int) and next_sync_interval > 0:
                    if next_sync_interval != self.client.breakpoint_poll_interval:
                        logger.debug(
                            "Updating BREAKPOINT poll interval from %ds to %ds",
                            self.client.breakpoint_poll_interval,
                            next_sync_interval,
                        )
                        self.client.breakpoint_poll_interval = next_sync_interval

                # Update success time on any successful fetch (not just when Changed)
                self._breakpoint_last_success_time = time.time()

                # Apply configuration if changed
                if config.get("Changed") is not False:
                    breakpoint_configs = self._parse_api_response_to_configs(config)
                    self._apply_merged_configuration(new_probe_configs=None, new_breakpoint_configs=breakpoint_configs)

                # Mark first fetch as complete
                if is_first_fetch:
                    logger.debug("Initial breakpoint configuration fetch successful")
                    is_first_fetch = False
                    attempt = 0

            except Exception as exc:
                logger.error("Unexpected error during breakpoint configuration polling: %s", exc)
                if is_first_fetch:
                    attempt += 1
                    self._check_degraded_mode("BREAKPOINT", attempt)

        logger.debug("Breakpoint polling loop ended")

    def _parse_api_response_to_configs(self, api_response: Dict[str, Any]) -> List[BreakpointConfiguration]:
        """Parse API response into list of BreakpointConfiguration objects.

        This method filters configurations based on attribute filters before parsing.

        Args:
            api_response: Raw API response with LatestConfigurations

        Returns:
            List of BreakpointConfiguration objects that match attribute filters
        """
        configs = []
        latest_configs = api_response.get("LatestConfigurations", [])

        for config_item in latest_configs:
            # Check attribute filters first (early filtering)
            attribute_filters = config_item.get("AttributeFilters", [])
            if not self._matches_attribute_filters(attribute_filters):
                logger.debug(
                    "Skipping config due to attribute filter mismatch: %s",
                    config_item.get("Location", "unknown"),
                )
                continue

            # Parse into BreakpointConfiguration
            try:
                bp_config = BreakpointConfiguration.from_api_config(config_item)
                # Only add valid configs (from_api_config returns None for invalid configs)
                if bp_config is not None:
                    configs.append(bp_config)
            except Exception as exception:  # pylint: disable=broad-exception-caught
                logger.error("Error parsing config item: %s", exception)
                continue

        logger.debug("Parsed %d configurations from %d API items", len(configs), len(latest_configs))
        return configs

    @staticmethod
    def _matches_attribute_filters(attribute_filters: list) -> bool:
        """Check if resource attributes match at least one filter object.

        Args:
            attribute_filters: List of filter objects, each containing key-value pairs

        Returns:
            True if at least one filter object matches all its key-value pairs in resource
        """
        if not attribute_filters:
            return True

        try:
            tracer_provider = trace.get_tracer_provider()
            resource_attrs = tracer_provider.resource.attributes

            # Check if at least one filter object matches
            for filter_obj in attribute_filters:
                if not isinstance(filter_obj, dict):
                    logger.warning("Invalid filter object type: %s, skipping", type(filter_obj))
                    continue

                # Filter out empty string keys and invalid entries
                valid_filters = {k: v for k, v in filter_obj.items() if k and isinstance(k, str)}
                if not valid_filters:
                    continue

                if all(resource_attrs.get(k) == v for k, v in valid_filters.items()):
                    return True

            return False
        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.warning("Error checking attribute filters: %s", exception)
            return True  # Default to allowing instrumentation on error

    def _check_staleness(self) -> None:
        """Check and log staleness warnings for both configuration types."""
        now = time.time()

        # Check PROBE staleness
        if self._probe_last_success_time:
            probe_age = now - self._probe_last_success_time
            if probe_age > self.PROBE_STALENESS_THRESHOLD:
                logger.warning(
                    "[PROBE] Configurations are stale: %.0fs old (threshold: %ds)",
                    probe_age,
                    self.PROBE_STALENESS_THRESHOLD,
                )

        # Check BREAKPOINT staleness
        if self._breakpoint_last_success_time:
            breakpoint_age = now - self._breakpoint_last_success_time
            if breakpoint_age > self.BREAKPOINT_STALENESS_THRESHOLD:
                logger.warning(
                    "[BREAKPOINT] Configurations are stale: %.0fs old (threshold: %ds)",
                    breakpoint_age,
                    self.BREAKPOINT_STALENESS_THRESHOLD,
                )

    def _apply_merged_configuration(
        self,
        new_probe_configs: Optional[List[BreakpointConfiguration]],
        new_breakpoint_configs: Optional[List[BreakpointConfiguration]],
    ) -> None:
        """Atomically merge and apply configurations from either or both pollers.

        This method ensures thread-safe configuration updates by:
        1. Acquiring exclusive lock
        2. Updating the appropriate cache (PROBE or BREAKPOINT or both)
        3. Merging both caches to create complete configuration snapshot
        4. Applying merged configuration to InstrumentationManager

        Args:
            new_probe_configs: New PROBE configurations (None if not updated)
            new_breakpoint_configs: New BREAKPOINT configurations (None if not updated)
        """
        try:
            with self._config_lock:
                # Update cache for whichever type was fetched
                if new_probe_configs is not None:
                    self._cached_probe_configs = new_probe_configs
                    logger.debug("Updated probes cache: %d configs", len(new_probe_configs))

                if new_breakpoint_configs is not None:
                    self._cached_breakpoint_configs = new_breakpoint_configs
                    logger.debug("Updated breakpoints cache: %d configs", len(new_breakpoint_configs))

                # Merge both caches to create complete configuration snapshot
                all_configs = self._cached_probe_configs + self._cached_breakpoint_configs
                manager = get_global_manager()
                if manager is None:
                    logger.warning("InstrumentationManager not initialized, cannot apply configuration")
                    return

                # Delegate to manager - it handles everything:
                # - Change detection
                # - State preservation
                # - Error isolation
                # - Atomic application
                result = manager.apply_configuration(all_configs)

                # Log results
                logger.debug(
                    "Applied configuration: %d successful, %d failed, %d unchanged",
                    result["applied"],
                    result["failed"],
                    result["unchanged"],
                )

                # Log failures (ERROR status is reported immediately by the manager)
                if result["details"]["failed"]:
                    for failure in result["details"]["failed"]:
                        logger.warning("Failed to apply %s: %s", failure["function_key"], failure["error"])

                # Trigger out-of-band status report (for Ready/Disabled statuses)
                manager.report_initial_status()

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error("Error applying configuration: %s", exception)

    def is_running(self) -> bool:
        """Check if poller is running.

        Returns:
            True if poller is running
        """
        return self._running
