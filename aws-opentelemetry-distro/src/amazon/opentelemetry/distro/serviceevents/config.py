# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Configuration management for ServiceEvents instrumentation.

Provides environment variable parsing and configuration defaults for all
ServiceEvents features including AST transformation, collectors, and exporters.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from amazon.opentelemetry.distro.serviceevents.models.resource_attributes import ResourceAttributes
from amazon.opentelemetry.distro.version import __version__ as ADOT_VERSION

_logger = logging.getLogger(__name__)


def _get_service_name_from_resource_attributes() -> Optional[str]:
    """Extract service.name from OTEL_RESOURCE_ATTRIBUTES environment variable.

    Parses the comma-separated key=value pairs in OTEL_RESOURCE_ATTRIBUTES
    looking for 'service.name'.

    Example: OTEL_RESOURCE_ATTRIBUTES='service.name=shoppingcart,deployment.environment=production'

    Returns:
        Service name if found, None otherwise.
    """
    env_resources = os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "")
    if not env_resources:
        return None

    for pair in env_resources.split(","):
        if "=" in pair:
            key, value = pair.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key == "service.name":
                return value

    return None


def _get_environment_from_resource_attributes() -> Optional[str]:
    """Extract deployment environment from OTEL_RESOURCE_ATTRIBUTES environment variable.

    Parses the comma-separated key=value pairs in OTEL_RESOURCE_ATTRIBUTES
    looking for 'deployment.environment.name' (preferred) or 'deployment.environment'.

    Example: OTEL_RESOURCE_ATTRIBUTES='service.name=shoppingcart,deployment.environment=production'

    Returns:
        Environment name if found, None otherwise.
    """
    env_resources = os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "")
    if not env_resources:
        return None

    for pair in env_resources.split(","):
        if "=" in pair:
            key, value = pair.split("=", 1)
            key = key.strip()
            value = value.strip()
            # Prefer deployment.environment.name (newer OTel convention)
            if key == "deployment.environment.name":
                return value
            # Fallback to deployment.environment (older convention)
            if key == "deployment.environment":
                return value

    return None


# Internal test-config hook. NOT a customer-facing surface — undocumented, gated, and applied
# only when DEBUG_SE_TEST_CONFIG is set. Black-box contract/e2e suites run the SDK in a separate
# process and can only inject configuration via env; this lets them override the small set of
# internal knobs that no longer have their own env vars. Format is dependency-free delimited
# "KEY=value;KEY=value" where KEY is the former env-var suffix.
_TEST_CONFIG_HOOK_ENV = "DEBUG_SE_TEST_CONFIG"
_test_config_hook_warned = False


def _apply_test_config_hook(config: "ServiceEventsConfig") -> None:
    """Apply internal test-only config overrides from DEBUG_SE_TEST_CONFIG.

    Gated: a literal no-op when the env var is unset or empty. When active it overrides only a
    fixed allowlist of internal fields and emits a one-time WARN. Unknown keys and unparsable
    values are silently ignored. This is a test affordance, NOT a supported configuration
    surface.
    """
    raw = os.getenv(_TEST_CONFIG_HOOK_ENV)
    if not raw:
        return

    global _test_config_hook_warned  # pylint: disable=global-statement  # module-level singleton warn-once flag
    if not _test_config_hook_warned:
        _logger.warning(
            "ServiceEvents: %s is set — applying internal test config overrides. "
            "This is a test-only hook and is NOT for production use.",
            _TEST_CONFIG_HOOK_ENV,
        )
        _test_config_hook_warned = True

    def _set_int(field_name: str, value: str) -> None:
        try:
            setattr(config, field_name, int(value))
        except ValueError:
            pass

    # Recognized keys are the bare env-var suffixes the container/e2e suites need.
    int_fields = {
        "ENDPOINT_FLUSH_INTERVAL": "endpoint_flush_interval",
        "INCIDENT_SNAPSHOT_FLUSH_INTERVAL": "incident_snapshot_flush_interval",
        "SAMPLE_TIER1_THRESHOLD": "sample_tier1_threshold",
        "SAMPLE_TIER2_THRESHOLD": "sample_tier2_threshold",
        "SAMPLE_TIER2_RATE": "sample_tier2_rate",
        "SAMPLE_TIER3_RATE": "sample_tier3_rate",
    }
    str_fields = {
        "LOG_GROUP": "log_group",
        "LOG_STREAM": "log_stream",
    }

    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry or "=" not in entry:
            continue
        key, value = entry.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key in int_fields:
            _set_int(int_fields[key], value)
        elif key in str_fields:
            setattr(config, str_fields[key], value)
        # Unknown keys silently ignored.


@dataclass
class ServiceEventsConfig:
    """Configuration for ServiceEvents instrumentation."""

    # Enable/Disable. Default false: OTEL_AWS_SERVICE_EVENTS_ENABLED is unset by default,
    # and the outer bundling gate in aws_opentelemetry_configurator._init_serviceevents
    # is authoritative for "should ServiceEvents run" (bundled with App Signals, Lambda
    # excluded, explicit override). Callers that bypass the outer gate must set
    # enabled=True explicitly.
    enabled: bool = False  # OTEL_AWS_SERVICE_EVENTS_ENABLED

    # Whether Application Signals is enabled. Used to suppress ServiceEvents signals
    # that App Signals already covers (e.g. EndpointSummary — App Signals emits
    # equivalent per-endpoint duration + error metrics). Populated from
    # OTEL_AWS_APPLICATION_SIGNALS_ENABLED at fromEnv time.
    application_signals_enabled: bool = False  # OTEL_AWS_APPLICATION_SIGNALS_ENABLED

    # Local-testing file exporter. When set, replaces the OTLP network exporters
    # (LOGS_ENDPOINT and METRICS_ENDPOINT are ignored). Output is CloudWatch-faithful
    # NDJSON — one flat line per LogRecord, EMF envelope per metric data point.
    output_file: str = ""  # OTEL_AWS_SERVICE_EVENTS_OUTPUT_FILE
    service_name: str = "UnknownService"  # OTEL_SERVICE_NAME or OTEL_RESOURCE_ATTRIBUTES[service.name]

    # Environment and SDK. No default sentinel: when none of the resolution sources
    # (OTEL_RESOURCE_ATTRIBUTES[deployment.environment(.name)] / ENVIRONMENT) is set,
    # environment stays None and is omitted from every signal rather than emitting a
    # placeholder value.
    environment: Optional[str] = None  # OTEL_RESOURCE_ATTRIBUTES[deployment.environment(.name)] or ENVIRONMENT
    sdk_version: str = ADOT_VERSION  # Automatically fetched from ADOT package version

    # Flush Intervals (milliseconds). Internal: hardcoded, no env override (test hook may set
    # endpoint_flush_interval and incident_snapshot_flush_interval).
    endpoint_flush_interval: int = 30000
    incident_snapshot_flush_interval: int = 10000
    deployment_event_flush_interval: int = 86_400_000

    # Incident Snapshot Settings. The rate-limit window is fixed at 1 minute; this is
    # the max snapshots per that fixed window.
    incident_snapshot_max_per_minute: int = 100  # OTEL_AWS_SERVICE_EVENTS_INCIDENT_SNAPSHOT_MAX_PER_MINUTE
    incident_snapshot_duration_threshold_ms: int = (
        5000  # OTEL_AWS_SERVICE_EVENTS_INCIDENT_SNAPSHOT_DURATION_THRESHOLD_MS
    )
    incident_snapshot_max_same_error: int = 1  # OTEL_AWS_SERVICE_EVENTS_INCIDENT_SNAPSHOT_MAX_SAME_ERROR

    # Per-endpoint latency thresholds for latency-triggered incident snapshots
    # Format: "METHOD /route:threshold_ms,METHOD /route:threshold_ms,..."
    # Example: "POST /api/checkout:500,GET /api/health:50,GET /api/reports:5000"
    # Django note: routes are matched WITHOUT a leading slash (e.g. "api/checkout"), to
    # mirror Application Signals. Write Django patterns slash-less, e.g. "POST api/checkout:500".
    # Exception: unmatched/scanner requests (no route) are recorded with a leading-slash
    # first-segment label (e.g. "/wp-admin"), so thresholds targeting them keep the slash.
    latency_thresholds: List[str] = field(default_factory=list)  # OTEL_AWS_SERVICE_EVENTS_LATENCY_THRESHOLDS

    # Incident Snapshot Request Payload Capture Settings. Hardcoded off — no longer a
    # customer-facing env opt-in. The collector capture code stays dormant.
    incident_snapshot_capture_request_body: bool = False

    # Endpoint Filtering - glob patterns in format "METHOD /route" or "* /route" or "METHOD *"
    # If include_patterns is set, only track matching endpoints; then exclude_patterns removes from that set
    # Example: "GET /api/*,POST /api/*" or "* /health,* /metrics"
    # Django note: routes are recorded WITHOUT a leading slash (e.g. "api/users"), to mirror
    # Application Signals. Write Django patterns slash-less, e.g. "GET api/*"; Flask/FastAPI
    # routes carry the leading slash, so their patterns keep it, e.g. "GET /api/*".
    # Exception: unmatched/scanner requests (no route) are recorded with a leading-slash
    # first-segment label (e.g. "/wp-admin"), so filters targeting them keep the slash even
    # on Django, e.g. "* /wp-admin".
    endpoint_include_patterns: List[str] = field(
        default_factory=list
    )  # OTEL_AWS_SERVICE_EVENTS_ENDPOINT_INCLUDE_PATTERNS
    endpoint_exclude_patterns: List[str] = field(
        default_factory=list
    )  # OTEL_AWS_SERVICE_EVENTS_ENDPOINT_EXCLUDE_PATTERNS

    # AST Instrumentation — function-instrumentation denylist. Always wins over
    # PACKAGES_INCLUDE (rule 2 in ast_transformation.py). Empty by default; user
    # adds patterns to subtract from PACKAGES_INCLUDE.
    packages_exclude: List[str] = field(default_factory=list)  # OTEL_AWS_SERVICE_EVENTS_PACKAGES_EXCLUDE

    # Sampling Mode: "auto" (tiered), "always" (100%), "never" (0%)
    sampling_mode: str = "always"  # OTEL_AWS_SERVICE_EVENTS_SAMPLING_MODE

    # Sampling Thresholds (for "auto" mode: 3-tier sampling). Internal: hardcoded, no env
    # override (test hook may set them).
    sample_tier1_threshold: int = 100
    sample_tier2_threshold: int = 1000
    sample_tier2_rate: int = 10
    sample_tier3_rate: int = 100

    # OTLP Export Settings. Empty here means "unset" — the 4316 default is applied
    # by the endpoint policy in aws_opentelemetry_configurator._init_serviceevents so it
    # can require non-empty values when Application Signals is off. ServiceEvents does
    # NOT fall through to OTEL_EXPORTER_OTLP_*_ENDPOINT.
    logs_endpoint: str = ""  # OTEL_AWS_OTLP_LOGS_ENDPOINT
    metrics_endpoint: str = ""  # OTEL_AWS_OTLP_METRICS_ENDPOINT
    # Logging destination. Internal: hardcoded, no env override (test hook may set them).
    log_group: str = "/aws/serviceevents/telemetry"
    log_stream: str = ""  # default: service_name

    # Function instrumentation (AST-based in Python, analogue of Java's bytecode mode).
    # On by default: function-level instrumentation owns FunctionCall + incident
    # call_path capture. It only instruments functions matched by packages_include
    # (empty by default, see below), so with no allowlist it installs the hooks but
    # instruments nothing until OTEL_AWS_SERVICE_EVENTS_PACKAGES_INCLUDE is set.
    function_instrument_enabled: bool = True  # OTEL_AWS_SERVICE_EVENTS_FUNCTION_INSTRUMENT_ENABLED

    # Function-instrumentation allowlist.
    # Empty = no functions instrumented (no implicit default scope). The only way
    # to opt in. Supported syntax: "foo.*" (prefix); bare "*" is rejected.
    packages_include: List[str] = field(default_factory=list)  # OTEL_AWS_SERVICE_EVENTS_PACKAGES_INCLUDE

    # Resource attributes from OTel Resource detectors (cloud/host/container/k8s metadata)
    # Populated by OTel configurator when available, empty for manual init
    resource_attributes: ResourceAttributes = field(default_factory=ResourceAttributes)

    @classmethod
    def from_env(cls, resource_attributes: Optional[ResourceAttributes] = None) -> "ServiceEventsConfig":
        """Build configuration from environment variables.

        Uses class attribute defaults as fallback when environment variables are not set.
        This ensures the class definition is the single source of truth for default values.

        Args:
            resource_attributes: Optional ResourceAttributes from OTel Resource detectors.
                                 Defaults to empty ResourceAttributes() when not provided.
        """
        # Create default config instance to get class default values
        defaults = cls()

        def get_bool(env_var: str, default: bool) -> bool:
            """Parse boolean environment variable."""
            return os.getenv(env_var, str(default)).lower() == "true"

        def get_int(env_var: str, default: int) -> int:
            """Parse integer environment variable."""
            try:
                return int(os.getenv(env_var, str(default)))
            except ValueError:
                return default

        def get_str(env_var: str, default: str) -> str:
            """Parse string environment variable."""
            return os.getenv(env_var, default)

        def get_list(env_var: str, default: List[str]) -> List[str]:
            """Parse comma-separated list environment variable."""
            value = os.getenv(env_var, "")
            if value:
                return [item.strip() for item in value.split(",") if item.strip()]
            return default

        def get_pattern_list(env_var: str, default: List[str]) -> List[str]:
            """Parse a package-pattern list env var, rejecting the bare '*' sentinel.

            Bare `*` is rejected as too broad (it would match every module, defeating
            the point of an explicit allowlist), so we strip any lone `*` entry and log
            once per invocation. An empty list instruments nothing — there is no implicit
            default scope (see the scope rule in ast_transformation.py).
            """
            raw = get_list(env_var, default)
            normalized = []
            saw_star = False
            for item in raw:
                if item == "*":
                    saw_star = True
                    continue
                normalized.append(item)
            if saw_star:
                _logger.info(
                    "ServiceEvents: ignoring bare '*' entry in %s; use specific package "
                    "prefixes (e.g. myapp). An empty list instruments nothing.",
                    env_var,
                )
            return normalized

        def get_service_name(default: str) -> str:
            """Get service name with fallback chain.

            Priority:
            1. OTEL_SERVICE_NAME environment variable
            2. service.name from OTEL_RESOURCE_ATTRIBUTES
            3. Default value
            """
            # First try OTEL_SERVICE_NAME
            service_name = os.getenv("OTEL_SERVICE_NAME")
            if service_name:
                return service_name

            # Then try OTEL_RESOURCE_ATTRIBUTES
            service_name = _get_service_name_from_resource_attributes()
            if service_name:
                return service_name

            # Fall back to default
            return default

        def get_environment(default: Optional[str]) -> Optional[str]:
            """Get environment with fallback chain.

            Priority:
            1. deployment.environment.name from OTEL_RESOURCE_ATTRIBUTES
            2. deployment.environment from OTEL_RESOURCE_ATTRIBUTES
            3. ENVIRONMENT environment variable
            4. Default value (None) — no sentinel; environment is omitted everywhere when unset
            """
            # First try OTEL_RESOURCE_ATTRIBUTES
            environment = _get_environment_from_resource_attributes()
            if environment:
                return environment

            # Then try ENVIRONMENT env var
            environment = os.getenv("ENVIRONMENT")
            if environment:
                return environment

            # Fall back to default (None when unset)
            return default

        # Build configuration using class defaults as fallback
        config = cls(
            # Enable/Disable
            enabled=get_bool("OTEL_AWS_SERVICE_EVENTS_ENABLED", defaults.enabled),
            # Mirror aws_opentelemetry_configurator._is_application_signals_enabled precedence:
            # the new OTEL_AWS_APPLICATION_SIGNALS_ENABLED wins, falling back to the deprecated
            # OTEL_AWS_APP_SIGNALS_ENABLED when the new var is unset. The outer gate that starts
            # ServiceEvents honors the deprecated var, so this flag must too — otherwise a
            # customer who set only the deprecated var would get EndpointSummary emitted
            # alongside App Signals' equivalent metrics (the suppression is keyed on this flag).
            application_signals_enabled=get_bool(
                "OTEL_AWS_APPLICATION_SIGNALS_ENABLED",
                get_bool("OTEL_AWS_APP_SIGNALS_ENABLED", defaults.application_signals_enabled),
            ),
            # Local-testing file exporter (literal path; empty = disabled)
            output_file=get_str("OTEL_AWS_SERVICE_EVENTS_OUTPUT_FILE", defaults.output_file),
            service_name=get_service_name(defaults.service_name),
            # Environment and SDK
            environment=get_environment(defaults.environment),
            sdk_version=defaults.sdk_version,
            # Flush Intervals (internal; hardcoded defaults, reachable only via the test hook)
            endpoint_flush_interval=defaults.endpoint_flush_interval,
            incident_snapshot_flush_interval=defaults.incident_snapshot_flush_interval,
            deployment_event_flush_interval=defaults.deployment_event_flush_interval,
            # Incident Snapshot Settings (rate-limit window fixed at 1 minute)
            incident_snapshot_max_per_minute=get_int(
                "OTEL_AWS_SERVICE_EVENTS_INCIDENT_SNAPSHOT_MAX_PER_MINUTE", defaults.incident_snapshot_max_per_minute
            ),
            incident_snapshot_duration_threshold_ms=get_int(
                "OTEL_AWS_SERVICE_EVENTS_INCIDENT_SNAPSHOT_DURATION_THRESHOLD_MS",
                defaults.incident_snapshot_duration_threshold_ms,
            ),
            incident_snapshot_max_same_error=get_int(
                "OTEL_AWS_SERVICE_EVENTS_INCIDENT_SNAPSHOT_MAX_SAME_ERROR", defaults.incident_snapshot_max_same_error
            ),
            latency_thresholds=get_list("OTEL_AWS_SERVICE_EVENTS_LATENCY_THRESHOLDS", defaults.latency_thresholds),
            # Incident Snapshot Request Payload Capture Settings: hardcoded off (no env opt-in).
            # Endpoint Filtering
            endpoint_include_patterns=get_list(
                "OTEL_AWS_SERVICE_EVENTS_ENDPOINT_INCLUDE_PATTERNS", defaults.endpoint_include_patterns
            ),
            endpoint_exclude_patterns=get_list(
                "OTEL_AWS_SERVICE_EVENTS_ENDPOINT_EXCLUDE_PATTERNS", defaults.endpoint_exclude_patterns
            ),
            # AST Instrumentation — function-instrumentation denylist. Always wins over
            # PACKAGES_INCLUDE (rule 2 in ast_transformation.py). Bare '*' entries are
            # normalized away.
            packages_exclude=get_pattern_list("OTEL_AWS_SERVICE_EVENTS_PACKAGES_EXCLUDE", defaults.packages_exclude),
            # Sampling Mode
            sampling_mode=get_str("OTEL_AWS_SERVICE_EVENTS_SAMPLING_MODE", defaults.sampling_mode),
            # Sampling Thresholds (internal; hardcoded defaults, reachable only via the test hook)
            sample_tier1_threshold=defaults.sample_tier1_threshold,
            sample_tier2_threshold=defaults.sample_tier2_threshold,
            sample_tier2_rate=defaults.sample_tier2_rate,
            sample_tier3_rate=defaults.sample_tier3_rate,
            # OTLP Export Settings
            logs_endpoint=get_str("OTEL_AWS_OTLP_LOGS_ENDPOINT", defaults.logs_endpoint),
            metrics_endpoint=get_str("OTEL_AWS_OTLP_METRICS_ENDPOINT", defaults.metrics_endpoint),
            # Logging destination (internal; hardcoded defaults, reachable only via the test hook)
            log_group=defaults.log_group,
            log_stream=defaults.log_stream,
            # Function instrumentation
            function_instrument_enabled=get_bool(
                "OTEL_AWS_SERVICE_EVENTS_FUNCTION_INSTRUMENT_ENABLED", defaults.function_instrument_enabled
            ),
            packages_include=get_pattern_list("OTEL_AWS_SERVICE_EVENTS_PACKAGES_INCLUDE", defaults.packages_include),
            # Resource attributes (from OTel Resource detectors)
            resource_attributes=resource_attributes if resource_attributes is not None else ResourceAttributes(),
        )
        _apply_test_config_hook(config)
        return config

    def get_latency_threshold_patterns(self) -> List[Tuple[str, float]]:
        """Parse latency_thresholds list into pattern -> threshold_ms tuples.

        Supports glob patterns using fnmatch (*, ?, [seq], [!seq]).
        Each entry should be in format "METHOD /route:threshold_ms".

        Examples:
            - "GET /api/users:500" - exact match
            - "* /server_request:50" - any method to /server_request
            - "GET /api/*:100" - any GET to /api/* routes
            - "* *:200" - all endpoints (catch-all)

        Django note: routes are recorded WITHOUT a leading slash (to mirror Application
        Signals), so Django thresholds must omit it, e.g. "GET api/users:500". Flask/FastAPI
        routes carry the leading slash, e.g. "GET /api/users:500". Exception: unmatched/scanner
        requests (no route) are recorded with a leading-slash first-segment label (e.g.
        "/wp-admin"), so thresholds targeting them keep the slash even on Django.

        Returns:
            List of (pattern, threshold_ms) tuples. Order matters - first match wins.
        """
        result: List[Tuple[str, float]] = []

        for entry in self.latency_thresholds:
            entry = entry.strip()
            if not entry or ":" not in entry:
                continue

            # Split on last colon to handle routes that might contain colons
            last_colon_idx = entry.rfind(":")
            if last_colon_idx <= 0:
                continue

            api_part = entry[:last_colon_idx].strip()
            threshold_part = entry[last_colon_idx + 1 :].strip()

            # Parse threshold
            try:
                threshold_ms = float(threshold_part)
            except ValueError:
                continue

            # Parse "METHOD /route" format
            parts = api_part.split(" ", 1)
            if len(parts) != 2:
                continue

            method = parts[0].strip().upper()
            route = parts[1].strip()

            # Store as pattern string "METHOD /route"
            pattern = f"{method} {route}"
            result.append((pattern, threshold_ms))

        return result

    def should_track_endpoint(self, route: str, method: str) -> bool:
        """Check if an endpoint should be tracked based on include/exclude patterns.

        Filter logic:
        1. If include_patterns is empty -> track all endpoints (default)
        2. If include_patterns is set -> only track endpoints matching at least one include pattern
        3. Then, if exclude_patterns is set -> remove any endpoints matching exclude patterns

        Patterns use glob-style matching (fnmatch):
        - "*" matches anything
        - "?" matches any single character
        - Format: "METHOD /route" (e.g., "GET /api/*", "* /health", "POST /api/users")

        Django note: routes are recorded WITHOUT a leading slash (to mirror Application
        Signals), so Django patterns must omit it, e.g. "GET api/*". Flask/FastAPI routes
        carry the leading slash, e.g. "GET /api/*". Exception: unmatched/scanner requests
        (no route) are recorded with a leading-slash first-segment label (e.g. "/wp-admin"),
        so patterns targeting them keep the slash even on Django, e.g. "* /wp-admin".

        Args:
            route: The endpoint route (e.g., "/api/users" for Flask/FastAPI, "api/users" for Django)
            method: The HTTP method (e.g., "GET", "POST")

        Returns:
            True if the endpoint should be tracked, False if filtered out.
        """
        # Lazy import: fnmatch is only needed on this filtering path, not at module load.
        import fnmatch  # pylint: disable=import-outside-toplevel

        endpoint_str = f"{method.upper()} {route}"

        # Step 1: Check include patterns
        if self.endpoint_include_patterns:
            # Must match at least one include pattern
            included = False
            for pattern in self.endpoint_include_patterns:
                if fnmatch.fnmatch(endpoint_str, pattern):
                    included = True
                    break
            if not included:
                return False

        # Step 2: Check exclude patterns
        if self.endpoint_exclude_patterns:
            for pattern in self.endpoint_exclude_patterns:
                if fnmatch.fnmatch(endpoint_str, pattern):
                    return False

        return True
