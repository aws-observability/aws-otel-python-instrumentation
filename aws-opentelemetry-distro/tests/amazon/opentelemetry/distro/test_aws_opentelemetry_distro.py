# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import importlib
import os
import sys
from importlib.metadata import PackageNotFoundError, version
from unittest import TestCase
from unittest.mock import patch

from amazon.opentelemetry.distro.aws_opentelemetry_distro import AwsOpenTelemetryDistro
from opentelemetry import propagate
from opentelemetry.propagators.composite import CompositePropagator


class TestAwsOpenTelemetryDistro(TestCase):
    def setUp(self):
        # Store original env vars if they exist
        self.env_vars_to_restore = {}
        self.env_vars_to_check = [
            "OTEL_EXPORTER_OTLP_PROTOCOL",
            "OTEL_PROPAGATORS",
            "OTEL_PYTHON_ID_GENERATOR",
            "OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION",
            "AGENT_OBSERVABILITY_ENABLED",
            "OTEL_TRACES_EXPORTER",
            "OTEL_LOGS_EXPORTER",
            "OTEL_METRICS_EXPORTER",
            "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            "OTEL_TRACES_SAMPLER",
            "OTEL_PYTHON_DISABLED_INSTRUMENTATIONS",
            "OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED",
            "OTEL_AWS_APPLICATION_SIGNALS_ENABLED",
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
        self.assertEqual(os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL"), "http/protobuf")
        self.assertEqual(os.environ.get("OTEL_PROPAGATORS"), "baggage,xray,tracecontext")
        self.assertEqual(os.environ.get("OTEL_PYTHON_ID_GENERATOR"), "xray")
        self.assertEqual(
            os.environ.get("OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION"),
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
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_configure_with_agent_observability_enabled(
        self, mock_super_configure, mock_apply_patches, mock_is_agent_observability, mock_get_aws_region
    ):
        """Test that _configure sets agent observability defaults when enabled"""
        mock_is_agent_observability.return_value = True
        mock_get_aws_region.return_value = "us-west-2"

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Check agent observability defaults
        self.assertEqual(os.environ.get("OTEL_TRACES_EXPORTER"), "otlp")
        self.assertEqual(os.environ.get("OTEL_LOGS_EXPORTER"), "otlp")
        self.assertEqual(os.environ.get("OTEL_METRICS_EXPORTER"), "awsemf")
        self.assertEqual(os.environ.get("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"), "true")
        self.assertEqual(
            os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"), "https://xray.us-west-2.amazonaws.com/v1/traces"
        )
        self.assertEqual(
            os.environ.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"), "https://logs.us-west-2.amazonaws.com/v1/logs"
        )
        self.assertEqual(os.environ.get("OTEL_TRACES_SAMPLER"), "parentbased_always_on")
        self.assertEqual(
            os.environ.get("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"),
            "http,sqlalchemy,psycopg2,pymysql,sqlite3,aiopg,asyncpg,mysql_connector,"
            "urllib3,requests,system_metrics,google-genai",
        )
        self.assertEqual(os.environ.get("OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED"), "true")
        self.assertEqual(os.environ.get("OTEL_AWS_APPLICATION_SIGNALS_ENABLED"), "false")

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
        self.assertNotIn("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", os.environ)
        self.assertNotIn("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", os.environ)

        # But verify that the exporters are still set to otlp (will use default endpoints)
        self.assertEqual(os.environ.get("OTEL_TRACES_EXPORTER"), "otlp")
        self.assertEqual(os.environ.get("OTEL_LOGS_EXPORTER"), "otlp")
        self.assertEqual(os.environ.get("OTEL_METRICS_EXPORTER"), "awsemf")

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
        self.assertNotIn("OTEL_TRACES_EXPORTER", os.environ)
        self.assertNotIn("OTEL_LOGS_EXPORTER", os.environ)
        self.assertNotIn("OTEL_METRICS_EXPORTER", os.environ)
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
        os.environ["OTEL_TRACES_EXPORTER"] = "custom_exporter"
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "https://custom.endpoint.com"

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Check that existing values are preserved
        self.assertEqual(os.environ.get("OTEL_TRACES_EXPORTER"), "custom_exporter")
        self.assertEqual(os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"), "https://custom.endpoint.com")

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
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "https://my-custom-traces.example.com"
        os.environ["OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"] = "https://my-custom-logs.example.com"

        distro = AwsOpenTelemetryDistro()
        distro._configure()

        # Verify that user-provided endpoints are preserved
        self.assertEqual(os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"), "https://my-custom-traces.example.com")
        self.assertEqual(os.environ.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"), "https://my-custom-logs.example.com")

        # And exporters are still set to otlp
        self.assertEqual(os.environ.get("OTEL_TRACES_EXPORTER"), "otlp")
        self.assertEqual(os.environ.get("OTEL_LOGS_EXPORTER"), "otlp")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches")
    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.OpenTelemetryDistro._configure")
    def test_user_defined_propagators(self, mock_super_configure, mock_apply_patches):
        """Test that user-defined propagators are respected"""
        # Set user-defined propagators
        os.environ["OTEL_PROPAGATORS"] = "xray"

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
