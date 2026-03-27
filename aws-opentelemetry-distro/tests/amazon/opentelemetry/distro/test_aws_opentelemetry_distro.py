# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import importlib
import logging
import os
import sys
from importlib.metadata import PackageNotFoundError, version
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import APPLICATION_SIGNALS_ENABLED_CONFIG
from amazon.opentelemetry.distro.aws_opentelemetry_distro import (
    AGENT_OBSERVABILITY_DISABLED_INSTRUMENTATIONS,
    AwsOpenTelemetryDistro,
)
from opentelemetry import propagate
from opentelemetry.environment_variables import (
    OTEL_LOGS_EXPORTER,
    OTEL_METRICS_EXPORTER,
    OTEL_PROPAGATORS,
    OTEL_PYTHON_ID_GENERATOR,
    OTEL_TRACES_EXPORTER,
)
from opentelemetry.instrumentation.environment_variables import OTEL_PYTHON_DISABLED_INSTRUMENTATIONS
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.environment_variables import (
    _OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED as OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED,
)
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
    OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION,
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    OTEL_TRACES_SAMPLER,
)


class TestAwsOpenTelemetryDistro(TestCase):
    def setUp(self):
        # Store original env vars if they exist
        self.env_vars_to_restore = {}
        self.env_vars_to_check = [
            OTEL_EXPORTER_OTLP_PROTOCOL,
            OTEL_PROPAGATORS,
            OTEL_PYTHON_ID_GENERATOR,
            OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION,
            "AGENT_OBSERVABILITY_ENABLED",
            OTEL_TRACES_EXPORTER,
            OTEL_LOGS_EXPORTER,
            OTEL_METRICS_EXPORTER,
            "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT",
            OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
            OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
            OTEL_TRACES_SAMPLER,
            OTEL_PYTHON_DISABLED_INSTRUMENTATIONS,
            OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED,
            APPLICATION_SIGNALS_ENABLED_CONFIG,
            "OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS",
            "DJANGO_SETTINGS_MODULE",
            OTEL_EXPORTER_OTLP_ENDPOINT,
            OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
            "AGENT_OBSERVABILITY_VERSION",
        ]

        # First, save all current values
        for var in self.env_vars_to_check:
            if var in os.environ:
                self.env_vars_to_restore[var] = os.environ[var]

        # Then clear ALL of them to ensure clean state
        for var in self.env_vars_to_check:
            if var in os.environ:
                del os.environ[var]

        # Preserve the original sys.path
        self.original_sys_path = sys.path.copy()

    def tearDown(self):
        # Clear all env vars first
        for var in self.env_vars_to_check:
            if var in os.environ:
                del os.environ[var]

        # Then restore original values
        for var, value in self.env_vars_to_restore.items():
            os.environ[var] = value

        # Restore the original sys.path
        sys.path[:] = self.original_sys_path

    def test_package_available(self):
        try:
            version("aws-opentelemetry-distro")
        except PackageNotFoundError:
            self.fail("aws-opentelemetry-distro not installed")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_configure_sets_default_values(self, mock_super_configure, mock_apply_patches):
        """Test that _configure sets default environment variables"""
        distro = AwsOpenTelemetryDistro()
        distro._configure(apply_patches=True)

        # Check that default values are set
        self.assertEqual(os.environ.get(OTEL_EXPORTER_OTLP_PROTOCOL), "http/protobuf")
        self.assertEqual(os.environ.get(OTEL_PROPAGATORS), "baggage,xray,tracecontext")
        self.assertEqual(os.environ.get(OTEL_PYTHON_ID_GENERATOR), "xray")
        self.assertEqual(
            os.environ.get(OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION),
            "base2_exponential_bucket_histogram",
        )

        # Verify super()._configure() was called
        mock_super_configure.assert_called_once()

        # Verify patches were applied
        mock_apply_patches.assert_called_once()

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_configure_without_patches(self, mock_super_configure, mock_apply_patches):  # pylint: disable=no-self-use
        """Test that _configure can skip applying patches"""
        distro = AwsOpenTelemetryDistro()
        distro._configure(apply_patches=False)

        # Verify patches were NOT applied
        mock_apply_patches.assert_not_called()

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_configure_with_agent_observability_enabled(
        self,
        mock_super_configure,
        mock_apply_patches,
        mock_is_installed,
        mock_is_agent_observability,
        mock_get_aws_region,
    ):
        """Test that _configure sets agent observability defaults when enabled"""
        mock_is_agent_observability.return_value = True
        mock_get_aws_region.return_value = "us-west-2"
        mock_is_installed.return_value = False  # Mock Django as not installed to avoid interference

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Check agent observability defaults
        self.assertEqual(os.environ.get(OTEL_TRACES_EXPORTER), "otlp")
        self.assertEqual(os.environ.get(OTEL_LOGS_EXPORTER), "otlp")
        self.assertEqual(os.environ.get(OTEL_METRICS_EXPORTER), "awsemf")
        self.assertEqual(os.environ.get("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"), "true")
        self.assertEqual(
            os.environ.get(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT), "https://xray.us-west-2.amazonaws.com/v1/traces"
        )
        self.assertEqual(
            os.environ.get(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT), "https://logs.us-west-2.amazonaws.com/v1/logs"
        )
        self.assertEqual(os.environ.get(OTEL_TRACES_SAMPLER), "parentbased_always_on")
        self.assertEqual(
            os.environ.get(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS),
            AGENT_OBSERVABILITY_DISABLED_INSTRUMENTATIONS,
        )
        self.assertEqual(os.environ.get(OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED), "true")
        self.assertEqual(os.environ.get(APPLICATION_SIGNALS_ENABLED_CONFIG), "false")
        self.assertEqual(os.environ.get("OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS"), "false")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_configure_with_agent_observability_no_region(
        self, mock_super_configure, mock_apply_patches, mock_is_agent_observability, mock_get_aws_region
    ):
        """Test that _configure handles missing AWS region gracefully"""
        mock_is_agent_observability.return_value = True
        mock_get_aws_region.return_value = None  # No region found

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Check that OTLP endpoints are not set when region is not available
        self.assertNotIn(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, os.environ)
        self.assertNotIn(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, os.environ)

        # But verify that the exporters are still set to otlp (will use default endpoints)
        self.assertEqual(os.environ.get(OTEL_TRACES_EXPORTER), "otlp")
        self.assertEqual(os.environ.get(OTEL_LOGS_EXPORTER), "otlp")
        self.assertEqual(os.environ.get(OTEL_METRICS_EXPORTER), "awsemf")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_configure_with_agent_observability_disabled(
        self, mock_super_configure, mock_apply_patches, mock_is_agent_observability
    ):
        """Test that _configure doesn't set agent observability defaults when disabled"""
        mock_is_agent_observability.return_value = False

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Check that agent observability defaults are not set
        self.assertNotIn(OTEL_TRACES_EXPORTER, os.environ)
        self.assertNotIn(OTEL_LOGS_EXPORTER, os.environ)
        self.assertNotIn(OTEL_METRICS_EXPORTER, os.environ)
        self.assertNotIn("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", os.environ)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_configure_preserves_existing_env_vars(
        self, mock_super_configure, mock_apply_patches, mock_is_agent_observability, mock_get_aws_region
    ):
        """Test that _configure doesn't override existing environment variables"""
        mock_is_agent_observability.return_value = True
        mock_get_aws_region.return_value = "us-east-1"

        # Set existing values
        os.environ[OTEL_TRACES_EXPORTER] = "custom_exporter"
        os.environ[OTEL_EXPORTER_OTLP_TRACES_ENDPOINT] = "https://custom.endpoint.com"

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Check that existing values are preserved
        self.assertEqual(os.environ.get(OTEL_TRACES_EXPORTER), "custom_exporter")
        self.assertEqual(os.environ.get(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT), "https://custom.endpoint.com")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    @patch("os.getcwd")
    def test_configure_adds_cwd_to_sys_path(self, mock_getcwd, mock_super_configure, mock_apply_patches):
        """Test that _configure adds current working directory to sys.path"""
        mock_getcwd.return_value = "/test/working/directory"

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Check that cwd was added to sys.path
        self.assertIn("/test/working/directory", sys.path)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_configure_with_agent_observability_endpoints_already_set(
        self, mock_super_configure, mock_apply_patches, mock_is_agent_observability, mock_get_aws_region
    ):
        """Test that user-provided OTLP endpoints are preserved even when region detection fails"""
        mock_is_agent_observability.return_value = True
        mock_get_aws_region.return_value = None  # No region found

        # User has already set custom endpoints
        os.environ[OTEL_EXPORTER_OTLP_TRACES_ENDPOINT] = "https://my-custom-traces.example.com"
        os.environ[OTEL_EXPORTER_OTLP_LOGS_ENDPOINT] = "https://my-custom-logs.example.com"

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Verify that user-provided endpoints are preserved
        self.assertEqual(os.environ.get(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT), "https://my-custom-traces.example.com")
        self.assertEqual(os.environ.get(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT), "https://my-custom-logs.example.com")

        # And exporters are still set to otlp
        self.assertEqual(os.environ.get(OTEL_TRACES_EXPORTER), "otlp")
        self.assertEqual(os.environ.get(OTEL_LOGS_EXPORTER), "otlp")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_configure_agent_observability_defaults_to_v1_when_version_not_set(
        self,
        mock_super_configure,
        mock_apply_patches,
        mock_is_installed,
        mock_is_agent_observability,
        mock_get_aws_region,
    ):
        """Test that when AGENT_OBSERVABILITY_VERSION is not set, it defaults to v1 configuration"""
        mock_is_agent_observability.return_value = True
        mock_get_aws_region.return_value = "us-east-1"
        mock_is_installed.return_value = False
        os.environ.pop("AGENT_OBSERVABILITY_VERSION", None)

        AwsOpenTelemetryDistro()._configure()

        self.assertEqual(os.environ.get(OTEL_METRICS_EXPORTER), "awsemf")
        self.assertEqual(
            os.environ.get(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT), "https://xray.us-east-1.amazonaws.com/v1/traces"
        )
        self.assertEqual(
            os.environ.get(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT), "https://logs.us-east-1.amazonaws.com/v1/logs"
        )
        self.assertEqual(os.environ.get("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"), "true")
        self.assertEqual(os.environ.get(OTEL_TRACES_SAMPLER), "parentbased_always_on")
        self.assertEqual(os.environ.get(OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED), "true")
        self.assertEqual(os.environ.get(APPLICATION_SIGNALS_ENABLED_CONFIG), "false")
        self.assertEqual(os.environ.get("OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS"), "false")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_configure_agent_observability_v2(
        self,
        mock_super_configure,
        mock_apply_patches,
        mock_is_installed,
        mock_is_agent_observability,
        mock_get_aws_region,
    ):
        """Test that version 2 uses localhost collector endpoint and otlp metrics"""
        mock_is_agent_observability.return_value = True
        mock_get_aws_region.return_value = "us-east-1"
        mock_is_installed.return_value = False
        os.environ["AGENT_OBSERVABILITY_VERSION"] = "2"

        AwsOpenTelemetryDistro()._configure()

        self.assertEqual(os.environ.get(OTEL_METRICS_EXPORTER), "otlp")
        self.assertEqual(os.environ.get(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT), "http://localhost:4318/v1/traces")
        self.assertEqual(os.environ.get(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT), "http://localhost:4318/v1/logs")
        self.assertEqual(os.environ.get(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT), "http://localhost:4318/v1/metrics")
        self.assertEqual(os.environ.get("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"), "true")
        self.assertEqual(os.environ.get(OTEL_TRACES_SAMPLER), "parentbased_always_on")
        self.assertEqual(os.environ.get(OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED), "true")
        self.assertEqual(os.environ.get(APPLICATION_SIGNALS_ENABLED_CONFIG), "false")
        self.assertEqual(os.environ.get("OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS"), "false")

        os.environ.pop("AGENT_OBSERVABILITY_VERSION", None)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_user_defined_propagators(self, mock_super_configure, mock_apply_patches):
        """Test that user-defined propagators are respected"""
        # Set user-defined propagators
        os.environ[OTEL_PROPAGATORS] = "xray"

        # Force the reload of the propagate module otherwise the above environment
        # variable doesn't taker effect.
        importlib.reload(propagate)

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Verify that user-defined propagators are preserved
        propagators = propagate.get_global_textmap()
        self.assertTrue(isinstance(propagators, CompositePropagator))
        expected_propagators = ["AwsXRayPropagator"]
        individual_propagators = propagators._propagators
        self.assertEqual(1, len(individual_propagators))
        actual_propagators = []
        for prop in individual_propagators:
            actual_propagators.append(type(prop).__name__)
        self.assertEqual(expected_propagators, actual_propagators)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_otel_propagators_added_when_not_user_defined(self, mock_super_configure, mock_apply_patches):
        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Verify that the propagators are set correctly by ADOT
        propagators = propagate.get_global_textmap()

        self.assertTrue(isinstance(propagators, CompositePropagator))

        expected_propagators = ["W3CBaggagePropagator", "AwsXRayPropagator", "TraceContextTextMapPropagator"]
        individual_propagators = propagators._propagators
        self.assertEqual(3, len(individual_propagators))
        actual_propagators = []
        for prop in individual_propagators:
            actual_propagators.append(type(prop).__name__)
        self.assertEqual(expected_propagators, actual_propagators)

    # Django Instrumentation Tests
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_django_instrumentation_enabled_with_settings_module(
        self, mock_super_configure, mock_apply_patches, mock_is_installed
    ):
        """Test that Django instrumentation is enabled when DJANGO_SETTINGS_MODULE is set"""
        mock_is_installed.return_value = True
        os.environ["DJANGO_SETTINGS_MODULE"] = "myproject.settings"

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Verify that django is NOT in disabled instrumentations
        disabled_instrumentations = os.environ.get(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS, "")
        self.assertNotIn("django", disabled_instrumentations)

        mock_is_installed.assert_called_once_with("django")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_django_instrumentation_disabled_without_settings_module(
        self, mock_super_configure, mock_apply_patches, mock_is_installed
    ):
        """Test that Django instrumentation is disabled when DJANGO_SETTINGS_MODULE is not set"""
        mock_is_installed.return_value = True

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Verify that django is in disabled instrumentations
        disabled_instrumentations = os.environ.get(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS, "")
        self.assertIn("django", disabled_instrumentations)

        mock_is_installed.assert_called_once_with("django")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_django_instrumentation_disabled_with_existing_disabled_instrumentations(
        self, mock_super_configure, mock_apply_patches, mock_is_installed
    ):
        """Test that Django is appended to existing disabled instrumentations"""
        mock_is_installed.return_value = True
        os.environ[OTEL_PYTHON_DISABLED_INSTRUMENTATIONS] = "flask,fastapi"

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Verify that django is appended to existing disabled instrumentations
        disabled_instrumentations = os.environ.get(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS, "")
        self.assertEqual("flask,fastapi,django", disabled_instrumentations)

        mock_is_installed.assert_called_once_with("django")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_django_not_installed_no_effect(self, mock_super_configure, mock_apply_patches, mock_is_installed):
        """Test that when Django is not installed, no changes are made to disabled instrumentations"""
        mock_is_installed.return_value = False

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Verify that OTEL_PYTHON_DISABLED_INSTRUMENTATIONS is not affected
        disabled_instrumentations = os.environ.get(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS, "")
        self.assertEqual("", disabled_instrumentations)

        mock_is_installed.assert_called_once_with("django")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_django_instrumentation_enabled_with_settings_module_and_existing_disabled(
        self, mock_super_configure, mock_apply_patches, mock_is_installed
    ):
        """Test that Django instrumentation is enabled even with existing disabled instrumentations"""
        mock_is_installed.return_value = True
        os.environ["DJANGO_SETTINGS_MODULE"] = "myproject.settings"
        os.environ[OTEL_PYTHON_DISABLED_INSTRUMENTATIONS] = "flask,fastapi"

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Verify that django is NOT added to disabled instrumentations
        disabled_instrumentations = os.environ.get(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS, "")
        self.assertEqual("flask,fastapi", disabled_instrumentations)
        self.assertNotIn("django", disabled_instrumentations)

        mock_is_installed.assert_called_once_with("django")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_django_instrumentation_disabled_empty_settings_module(
        self, mock_super_configure, mock_apply_patches, mock_is_installed
    ):
        """Test that Django instrumentation is disabled when DJANGO_SETTINGS_MODULE is empty"""
        mock_is_installed.return_value = True
        os.environ["DJANGO_SETTINGS_MODULE"] = ""

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Verify that django is in disabled instrumentations
        disabled_instrumentations = os.environ.get(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS, "")
        self.assertIn("django", disabled_instrumentations)

        mock_is_installed.assert_called_once_with("django")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_agent_observability_enabled")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_application_signals_dimensions_disabled_with_agent_observability(
        self,
        mock_super_configure,
        mock_apply_patches,
        mock_is_installed,
        mock_is_agent_observability,
        mock_get_aws_region,
    ):
        """Test that OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS is disabled in agent observability mode"""
        mock_is_agent_observability.return_value = True
        mock_get_aws_region.return_value = "us-west-2"
        mock_is_installed.return_value = False

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        self.assertEqual(os.environ.get("OTEL_METRICS_ADD_APPLICATION_SIGNALS_DIMENSIONS"), "false")

    def test_agent_observability_respects_custom_disabled_instrumentations(self):
        os.environ[OTEL_PYTHON_DISABLED_INSTRUMENTATIONS] = "custom_lib"
        self._configure_with_agent_observability()
        self.assertEqual(os.environ.get(OTEL_PYTHON_DISABLED_INSTRUMENTATIONS), "custom_lib")

    def test_base_otlp_endpoint_prevents_specific_endpoints_v1(self):
        os.environ[OTEL_EXPORTER_OTLP_ENDPOINT] = "http://my-collector:4318"
        self._configure_with_agent_observability()
        self.assertNotIn(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, os.environ)
        self.assertNotIn(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, os.environ)

    def test_base_otlp_endpoint_prevents_specific_endpoints_v2(self):
        os.environ[OTEL_EXPORTER_OTLP_ENDPOINT] = "http://my-collector:4318"
        os.environ["AGENT_OBSERVABILITY_VERSION"] = "2"
        self._configure_with_agent_observability()
        self.assertNotIn(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, os.environ)
        self.assertNotIn(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, os.environ)
        self.assertNotIn(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, os.environ)

    def _configure_with_agent_observability(self, region="us-west-2"):
        with patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure"), patch(
            "amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches"
        ), patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.is_installed", return_value=False), patch(
            "amazon.opentelemetry.distro.aws_opentelemetry_distro.is_agent_observability_enabled", return_value=True
        ), patch(
            "amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region", return_value=region
        ):
            AwsOpenTelemetryDistro()._configure()


class TestVersionCompatibilityCheck(TestCase):
    """Tests for the OpenTelemetry version compatibility check."""

    MODULE_PATH = "amazon.opentelemetry.distro.aws_opentelemetry_distro"

    def test_no_warning_when_versions_match(self):
        """No warning should be logged when installed versions match expected versions."""
        from amazon.opentelemetry.distro.aws_opentelemetry_distro import _check_otel_version_compatibility

        with patch(f"{self.MODULE_PATH}._get_requires") as mock_requires, patch(
            f"{self.MODULE_PATH}._get_version"
        ) as mock_version:
            mock_requires.return_value = [
                "opentelemetry-api == 1.40.0",
                "opentelemetry-sdk == 1.40.0",
            ]
            mock_version.side_effect = lambda pkg: "1.40.0"

            with self.assertLogs(self.MODULE_PATH, level="WARNING") as cm:
                logging.getLogger(self.MODULE_PATH).warning("dummy")
                _check_otel_version_compatibility()

            # Only the dummy log should be present
            self.assertEqual(len(cm.output), 1)
            self.assertIn("dummy", cm.output[0])

    def test_warning_when_api_version_mismatched(self):
        """Warning should be logged when opentelemetry-api version doesn't match expected."""
        from amazon.opentelemetry.distro.aws_opentelemetry_distro import _check_otel_version_compatibility

        with patch(f"{self.MODULE_PATH}._get_requires") as mock_requires, patch(
            f"{self.MODULE_PATH}._get_version"
        ) as mock_version:
            mock_requires.return_value = [
                "opentelemetry-api == 1.40.0",
                "opentelemetry-sdk == 1.40.0",
            ]
            mock_version.side_effect = lambda pkg: {"opentelemetry-api": "1.33.1", "opentelemetry-sdk": "1.40.0"}[pkg]

            with self.assertLogs(self.MODULE_PATH, level="WARNING") as cm:
                _check_otel_version_compatibility()

            self.assertEqual(len(cm.output), 1)
            self.assertIn("opentelemetry-api==1.33.1", cm.output[0])
            self.assertIn("opentelemetry-api==1.40.0", cm.output[0])

    def test_warning_when_both_versions_mismatched(self):
        """Warning should include both packages when api and sdk are both mismatched."""
        from amazon.opentelemetry.distro.aws_opentelemetry_distro import _check_otel_version_compatibility

        with patch(f"{self.MODULE_PATH}._get_requires") as mock_requires, patch(
            f"{self.MODULE_PATH}._get_version"
        ) as mock_version:
            mock_requires.return_value = [
                "opentelemetry-api == 1.40.0",
                "opentelemetry-sdk == 1.40.0",
            ]
            mock_version.side_effect = lambda pkg: "1.33.1"

            with self.assertLogs(self.MODULE_PATH, level="WARNING") as cm:
                _check_otel_version_compatibility()

            self.assertEqual(len(cm.output), 1)
            self.assertIn("opentelemetry-api==1.33.1", cm.output[0])
            self.assertIn("opentelemetry-sdk==1.33.1", cm.output[0])
            self.assertIn("opentelemetry-api==1.40.0", cm.output[0])
            self.assertIn("opentelemetry-sdk==1.40.0", cm.output[0])

    def test_exception_does_not_propagate(self):
        """Check should silently handle exceptions without blocking startup."""
        from amazon.opentelemetry.distro.aws_opentelemetry_distro import _check_otel_version_compatibility

        with patch(f"{self.MODULE_PATH}._get_requires", side_effect=Exception("metadata unavailable")):
            # Should not raise
            _check_otel_version_compatibility()

    def test_parsing_skips_similar_package_names(self):
        """Parser should not confuse opentelemetry-sdk with opentelemetry-sdk-extension-aws."""
        from amazon.opentelemetry.distro.aws_opentelemetry_distro import _check_otel_version_compatibility

        with patch(f"{self.MODULE_PATH}._get_requires") as mock_requires, patch(
            f"{self.MODULE_PATH}._get_version"
        ) as mock_version:
            mock_requires.return_value = [
                "opentelemetry-api == 1.40.0",
                "opentelemetry-sdk-extension-aws == 2.1.0",
                "opentelemetry-sdk == 1.40.0",
            ]
            # Return 2.1.0 for sdk to verify it doesn't pick up sdk-extension-aws version
            mock_version.side_effect = lambda pkg: "1.40.0"

            with self.assertLogs(self.MODULE_PATH, level="WARNING") as cm:
                logging.getLogger(self.MODULE_PATH).warning("dummy")
                _check_otel_version_compatibility()

            # Only the dummy log — no mismatch
            self.assertEqual(len(cm.output), 1)
            self.assertIn("dummy", cm.output[0])

    def test_parsing_handles_no_spaces(self):
        """Parser should handle requirement strings without spaces around ==."""
        from amazon.opentelemetry.distro.aws_opentelemetry_distro import _check_otel_version_compatibility

        with patch(f"{self.MODULE_PATH}._get_requires") as mock_requires, patch(
            f"{self.MODULE_PATH}._get_version"
        ) as mock_version:
            mock_requires.return_value = [
                "opentelemetry-api==1.40.0",
                "opentelemetry-sdk==1.40.0",
            ]
            mock_version.side_effect = lambda pkg: "1.33.1"

            with self.assertLogs(self.MODULE_PATH, level="WARNING") as cm:
                _check_otel_version_compatibility()

            self.assertIn("opentelemetry-api==1.33.1", cm.output[0])
            self.assertIn("opentelemetry-api==1.40.0", cm.output[0])

    def test_parsing_handles_environment_markers(self):
        """Parser should strip environment markers from version strings."""
        from amazon.opentelemetry.distro.aws_opentelemetry_distro import _check_otel_version_compatibility

        with patch(f"{self.MODULE_PATH}._get_requires") as mock_requires, patch(
            f"{self.MODULE_PATH}._get_version"
        ) as mock_version:
            mock_requires.return_value = [
                'opentelemetry-api == 1.40.0 ; python_version >= "3.9"',
                "opentelemetry-sdk == 1.40.0",
            ]
            mock_version.side_effect = lambda pkg: "1.33.1"

            with self.assertLogs(self.MODULE_PATH, level="WARNING") as cm:
                _check_otel_version_compatibility()

            self.assertIn("opentelemetry-api==1.33.1", cm.output[0])
            self.assertIn("opentelemetry-api==1.40.0", cm.output[0])
