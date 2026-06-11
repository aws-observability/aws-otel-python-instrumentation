# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.serviceevents.config import ServiceEventsConfig


class TestServiceEventsConfig(TestCase):
    """Test the ServiceEventsConfig class."""

    def test_init_with_defaults(self):
        """Test initialization with default parameters."""
        config = ServiceEventsConfig()

        # Default false: mirrors unset OTEL_AWS_SERVICE_EVENTS_ENABLED. The outer bundling
        # gate in the configurator decides whether ServiceEvents actually runs.
        self.assertFalse(config.enabled)
        self.assertEqual(config.service_name, "UnknownService")
        self.assertEqual(config.endpoint_flush_interval, 30000)
        self.assertEqual(config.incident_snapshot_flush_interval, 10000)
        self.assertEqual(config.incident_snapshot_max_per_minute, 100)
        self.assertEqual(config.incident_snapshot_duration_threshold_ms, 5000)
        self.assertEqual(config.incident_snapshot_max_same_error, 1)
        # packages_exclude is empty by default. The non-configurable SDK_SELF_EXCLUDE
        # (in ast_transformation.py) is the only built-in filter.
        self.assertEqual(config.packages_exclude, [])

    @patch.dict(os.environ, {}, clear=True)
    def test_from_env_with_defaults(self):
        """Test from_env with no environment variables set."""
        config = ServiceEventsConfig.from_env()

        # OTEL_AWS_SERVICE_EVENTS_ENABLED is unset by default → config.enabled is False.
        # The outer bundling gate (see _is_serviceevents_enabled in configurator) is
        # authoritative for "should ServiceEvents run".
        self.assertFalse(config.enabled)
        self.assertEqual(config.service_name, "UnknownService")
        self.assertEqual(config.endpoint_flush_interval, 30000)
        # Function instrumentation is on by default (instruments nothing until
        # packages_include is set, which defaults empty).
        self.assertTrue(config.function_instrument_enabled)

    @patch.dict(
        os.environ,
        {
            "OTEL_AWS_SERVICE_EVENTS_ENABLED": "false",
            "OTEL_SERVICE_NAME": "test-service",
            "OTEL_AWS_SERVICE_EVENTS_INCIDENT_SNAPSHOT_MAX_PER_MINUTE": "10",
        },
    )
    def test_from_env_with_custom_values(self):
        """Test from_env with custom (release) environment variables."""
        config = ServiceEventsConfig.from_env()

        self.assertFalse(config.enabled)
        self.assertEqual(config.service_name, "test-service")
        self.assertEqual(config.incident_snapshot_max_per_minute, 10)

    @patch.dict(
        os.environ,
        {
            "OTEL_AWS_SERVICE_EVENTS_PACKAGES_EXCLUDE": "test.*,build.*",
        },
    )
    def test_from_env_with_lists(self):
        """OTEL_AWS_SERVICE_EVENTS_PACKAGES_EXCLUDE binds to the packages_exclude field."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.packages_exclude, ["test.*", "build.*"])

    @patch.dict(os.environ, {"OTEL_AWS_SERVICE_EVENTS_PACKAGES_INCLUDE": "myapp,mylib.*"})
    def test_packages_include_from_env(self):
        """OTEL_AWS_SERVICE_EVENTS_PACKAGES_INCLUDE binds to the packages_include field."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.packages_include, ["myapp", "mylib.*"])

    @patch.dict(os.environ, {}, clear=True)
    def test_packages_include_default_empty(self):
        """Empty by default — no implicit default scope, so no functions are instrumented."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.packages_include, [])

    @patch.dict(os.environ, {"OTEL_AWS_SERVICE_EVENTS_PACKAGES_INCLUDE": "*"})
    def test_packages_include_bare_star_normalized_away(self):
        """Bare '*' is invalid input — normalized away to empty list (equivalent to unset)."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.packages_include, [])

    @patch.dict(os.environ, {"OTEL_AWS_SERVICE_EVENTS_PACKAGES_INCLUDE": "myapp,*,other"})
    def test_packages_include_bare_star_stripped_from_mixed_list(self):
        """Bare '*' entries are stripped; other entries pass through."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.packages_include, ["myapp", "other"])

    @patch.dict(os.environ, {"OTEL_AWS_SERVICE_EVENTS_PACKAGES_EXCLUDE": "*"})
    def test_packages_exclude_bare_star_normalized_away(self):
        """Same bare-'*' rejection applies to PACKAGES_EXCLUDE."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.packages_exclude, [])

    @patch.dict(os.environ, {"OTEL_AWS_SERVICE_EVENTS_INCIDENT_SNAPSHOT_MAX_PER_MINUTE": "invalid"})
    def test_from_env_with_invalid_int(self):
        """Test from_env with invalid integer value falls back to default."""
        config = ServiceEventsConfig.from_env()

        # Should fall back to default
        self.assertEqual(config.incident_snapshot_max_per_minute, 100)

    @patch.dict(os.environ, {"OTEL_AWS_SERVICE_EVENTS_ENABLED": "TRUE"}, clear=True)
    def test_from_env_case_insensitive_bool(self):
        """Test boolean parsing is case-insensitive."""
        config = ServiceEventsConfig.from_env()
        self.assertTrue(config.enabled)

    @patch.dict(os.environ, {"OTEL_AWS_SERVICE_EVENTS_ENABLED": "yes"})
    def test_from_env_non_true_bool_is_false(self):
        """Test that non-'true' values for booleans are treated as false."""
        config = ServiceEventsConfig.from_env()
        self.assertFalse(config.enabled)

    def test_sampling_thresholds_defaults(self):
        """Test that sampling threshold fields have correct default values."""
        config = ServiceEventsConfig()
        self.assertEqual(config.sample_tier1_threshold, 100)
        self.assertEqual(config.sample_tier2_threshold, 1000)
        self.assertEqual(config.sample_tier2_rate, 10)
        self.assertEqual(config.sample_tier3_rate, 100)
        self.assertEqual(config.hot_endpoint_cycles, 100)

    @patch.dict(
        os.environ,
        {
            "OTEL_AWS_SERVICE_EVENTS_SAMPLE_TIER1_THRESHOLD": "50",
            "OTEL_AWS_SERVICE_EVENTS_SAMPLE_TIER2_THRESHOLD": "500",
            "OTEL_AWS_SERVICE_EVENTS_SAMPLE_TIER2_RATE": "5",
            "OTEL_AWS_SERVICE_EVENTS_SAMPLE_TIER3_RATE": "50",
            "OTEL_AWS_SERVICE_EVENTS_HOT_ENDPOINT_CYCLES": "200",
        },
    )
    def test_sampling_thresholds_env_is_ignored(self):
        """Sampling thresholds are internal — the former env vars no longer have any effect."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.sample_tier1_threshold, 100)
        self.assertEqual(config.sample_tier2_threshold, 1000)
        self.assertEqual(config.sample_tier2_rate, 10)
        self.assertEqual(config.sample_tier3_rate, 100)
        self.assertEqual(config.hot_endpoint_cycles, 100)

    # ─── Internal test-config hook (DEBUG_SE_TEST_CONFIG) ──

    @patch.dict(os.environ, {}, clear=True)
    def test_test_config_hook_unset_is_noop(self):
        """With the hook env unset, internal fields keep their hardcoded defaults."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.endpoint_flush_interval, 30000)
        self.assertEqual(config.sample_tier1_threshold, 100)
        self.assertEqual(config.log_group, "/aws/serviceevents/telemetry")

    @patch.dict(
        os.environ,
        {
            "DEBUG_SE_TEST_CONFIG": (
                "ENDPOINT_FLUSH_INTERVAL=2000;"
                "INCIDENT_SNAPSHOT_FLUSH_INTERVAL=1500;"
                "SAMPLE_TIER1_THRESHOLD=7;SAMPLE_TIER2_THRESHOLD=70;SAMPLE_TIER2_RATE=3;"
                "SAMPLE_TIER3_RATE=30;LOG_GROUP=/test/group;"
                "LOG_STREAM=test-stream"
            )
        },
        clear=True,
    )
    def test_test_config_hook_overrides_recognized_keys(self):
        """The hook overrides exactly the recognized internal fields."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.endpoint_flush_interval, 2000)
        self.assertEqual(config.incident_snapshot_flush_interval, 1500)
        self.assertEqual(config.sample_tier1_threshold, 7)
        self.assertEqual(config.sample_tier2_threshold, 70)
        self.assertEqual(config.sample_tier2_rate, 3)
        self.assertEqual(config.sample_tier3_rate, 30)
        self.assertEqual(config.log_group, "/test/group")
        self.assertEqual(config.log_stream, "test-stream")

    @patch.dict(
        os.environ,
        {"DEBUG_SE_TEST_CONFIG": "UNKNOWN_KEY=1;ENDPOINT_FLUSH_INTERVAL=notanint;LOG_GROUP=/ok"},
        clear=True,
    )
    def test_test_config_hook_ignores_unknown_and_garbage(self):
        """Unknown keys and unparsable values are silently ignored; valid keys still apply."""
        config = ServiceEventsConfig.from_env()
        # Unknown key: no crash, no attribute created.
        self.assertFalse(hasattr(config, "UNKNOWN_KEY"))
        # Garbage int: field keeps its default.
        self.assertEqual(config.endpoint_flush_interval, 30000)
        # Valid string key still applied.
        self.assertEqual(config.log_group, "/ok")

    # ─── Function-instrument mode flag ──

    @patch.dict(os.environ, {}, clear=True)
    def test_function_instrument_flag_default(self):
        """Function instrumentation is on by default."""
        config = ServiceEventsConfig.from_env()
        self.assertTrue(config.function_instrument_enabled)

    @patch.dict(
        os.environ,
        {"OTEL_AWS_SERVICE_EVENTS_FUNCTION_INSTRUMENT_ENABLED": "false"},
    )
    def test_function_instrument_can_be_disabled(self):
        """Function instrumentation turns off when its env flag is set to false."""
        config = ServiceEventsConfig.from_env()
        self.assertFalse(config.function_instrument_enabled)

    @patch.dict(
        os.environ,
        {"OTEL_AWS_SERVICE_EVENTS_FUNCTION_INSTRUMENT_ENABLED": "true"},
    )
    def test_function_instrument_can_be_enabled(self):
        """Function instrumentation turns on when its env flag is set."""
        config = ServiceEventsConfig.from_env()
        self.assertTrue(config.function_instrument_enabled)

    @patch.dict(os.environ, {"OTEL_AWS_APPLICATION_SIGNALS_ENABLED": "true"})
    def test_application_signals_enabled_from_env(self):
        """application_signals_enabled mirrors OTEL_AWS_APPLICATION_SIGNALS_ENABLED."""
        config = ServiceEventsConfig.from_env()
        self.assertTrue(config.application_signals_enabled)

    @patch.dict(os.environ, {}, clear=True)
    def test_application_signals_disabled_by_default(self):
        config = ServiceEventsConfig.from_env()
        self.assertFalse(config.application_signals_enabled)

    # ─── service_name resolution from OTEL_RESOURCE_ATTRIBUTES ──

    @patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "service.name=shoppingcart,deployment.environment=production"},
        clear=True,
    )
    def test_service_name_from_resource_attributes(self):
        """service.name is read from OTEL_RESOURCE_ATTRIBUTES when OTEL_SERVICE_NAME is unset."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.service_name, "shoppingcart")

    @patch.dict(
        os.environ,
        {
            "OTEL_SERVICE_NAME": "from-service-name",
            "OTEL_RESOURCE_ATTRIBUTES": "service.name=from-resource",
        },
        clear=True,
    )
    def test_service_name_env_var_wins_over_resource_attributes(self):
        """OTEL_SERVICE_NAME takes priority over OTEL_RESOURCE_ATTRIBUTES[service.name]."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.service_name, "from-service-name")

    @patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "deployment.environment=prod,other.key=value"},
        clear=True,
    )
    def test_service_name_falls_back_to_default_when_not_in_resource_attributes(self):
        """service_name falls back to the default when no service.name pair is present."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.service_name, "UnknownService")

    @patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "  service.name = padded-name , k=v"},
        clear=True,
    )
    def test_service_name_strips_whitespace_around_pairs(self):
        """Whitespace around the key and value in a resource-attribute pair is stripped."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.service_name, "padded-name")

    @patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "malformed,service.name=valid"},
        clear=True,
    )
    def test_service_name_skips_pairs_without_equals(self):
        """Pairs without an '=' are skipped while later valid pairs are still parsed."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.service_name, "valid")

    # ─── environment resolution from OTEL_RESOURCE_ATTRIBUTES / ENVIRONMENT ──

    @patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "deployment.environment.name=staging"},
        clear=True,
    )
    def test_environment_from_resource_attributes_name_convention(self):
        """deployment.environment.name (newer convention) is read from OTEL_RESOURCE_ATTRIBUTES."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.environment, "staging")

    @patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "deployment.environment=production"},
        clear=True,
    )
    def test_environment_from_resource_attributes_legacy_convention(self):
        """deployment.environment (older convention) is read from OTEL_RESOURCE_ATTRIBUTES."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.environment, "production")

    @patch.dict(os.environ, {"ENVIRONMENT": "qa"}, clear=True)
    def test_environment_from_environment_env_var(self):
        """environment falls back to the ENVIRONMENT env var when resource attributes lack it."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.environment, "qa")

    @patch.dict(
        os.environ,
        {
            "OTEL_RESOURCE_ATTRIBUTES": "deployment.environment=from-resource",
            "ENVIRONMENT": "from-env-var",
        },
        clear=True,
    )
    def test_environment_resource_attributes_win_over_environment_var(self):
        """OTEL_RESOURCE_ATTRIBUTES takes priority over the ENVIRONMENT env var."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.environment, "from-resource")

    @patch.dict(os.environ, {}, clear=True)
    def test_environment_defaults_to_none_when_unset(self):
        """environment stays None when no resolution source is set (no placeholder sentinel)."""
        config = ServiceEventsConfig.from_env()
        self.assertIsNone(config.environment)

    @patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "malformed,deployment.environment=valid"},
        clear=True,
    )
    def test_environment_skips_pairs_without_equals(self):
        """Pairs without an '=' are skipped while later valid environment pairs are parsed."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.environment, "valid")

    # ─── test-config hook entry parsing edge cases ──

    @patch.dict(
        os.environ,
        {"DEBUG_SE_TEST_CONFIG": ";;LOG_GROUP=/ok;noequals;  ;LOG_STREAM=stream"},
        clear=True,
    )
    def test_test_config_hook_skips_empty_and_equals_less_entries(self):
        """Empty entries and entries lacking '=' are skipped; valid entries still apply."""
        config = ServiceEventsConfig.from_env()
        self.assertEqual(config.log_group, "/ok")
        self.assertEqual(config.log_stream, "stream")

    # ─── get_latency_threshold_patterns ──

    def test_latency_threshold_patterns_parses_entries(self):
        """Well-formed "METHOD /route:threshold" entries parse into (pattern, threshold) tuples."""
        config = ServiceEventsConfig(latency_thresholds=["POST /api/checkout:500", "get /api/health:50"])
        self.assertEqual(
            config.get_latency_threshold_patterns(),
            [("POST /api/checkout", 500.0), ("GET /api/health", 50.0)],
        )

    def test_latency_threshold_patterns_route_with_colon(self):
        """Splitting on the last colon preserves routes that themselves contain colons."""
        config = ServiceEventsConfig(latency_thresholds=["GET /api/v1:resource:250"])
        self.assertEqual(
            config.get_latency_threshold_patterns(),
            [("GET /api/v1:resource", 250.0)],
        )

    def test_latency_threshold_patterns_skips_invalid_entries(self):
        """Entries that are empty, colon-less, leading-colon, non-numeric, or missing the route are skipped."""
        config = ServiceEventsConfig(
            latency_thresholds=[
                "",
                "  ",
                "no-colon-here",
                ":500",
                "GET /api/users:notanumber",
                "GET/api/users:100",
                "GET /api/ok:300",
            ]
        )
        self.assertEqual(
            config.get_latency_threshold_patterns(),
            [("GET /api/ok", 300.0)],
        )

    def test_latency_threshold_patterns_empty_default(self):
        """An empty latency_thresholds list yields no patterns."""
        config = ServiceEventsConfig()
        self.assertEqual(config.get_latency_threshold_patterns(), [])

    # ─── should_track_endpoint ──

    def test_should_track_endpoint_no_patterns_tracks_all(self):
        """With no include/exclude patterns, every endpoint is tracked."""
        config = ServiceEventsConfig()
        self.assertTrue(config.should_track_endpoint("/api/users", "GET"))

    def test_should_track_endpoint_include_match(self):
        """An endpoint matching an include pattern is tracked."""
        config = ServiceEventsConfig(endpoint_include_patterns=["GET /api/*"])
        self.assertTrue(config.should_track_endpoint("/api/users", "get"))

    def test_should_track_endpoint_include_no_match(self):
        """An endpoint not matching any include pattern is filtered out."""
        config = ServiceEventsConfig(endpoint_include_patterns=["GET /api/*"])
        self.assertFalse(config.should_track_endpoint("/health", "GET"))

    def test_should_track_endpoint_exclude_match(self):
        """An endpoint matching an exclude pattern is filtered out."""
        config = ServiceEventsConfig(endpoint_exclude_patterns=["* /health"])
        self.assertFalse(config.should_track_endpoint("/health", "GET"))

    def test_should_track_endpoint_exclude_no_match_tracks(self):
        """An endpoint not matching any exclude pattern is tracked."""
        config = ServiceEventsConfig(endpoint_exclude_patterns=["* /health"])
        self.assertTrue(config.should_track_endpoint("/api/users", "GET"))

    def test_should_track_endpoint_include_then_exclude(self):
        """Include selects the set, then exclude removes a matching endpoint from it."""
        config = ServiceEventsConfig(
            endpoint_include_patterns=["GET /api/*"],
            endpoint_exclude_patterns=["GET /api/secret"],
        )
        self.assertTrue(config.should_track_endpoint("/api/users", "GET"))
        self.assertFalse(config.should_track_endpoint("/api/secret", "GET"))
