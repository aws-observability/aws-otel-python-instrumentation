# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from unittest import TestCase
from unittest.mock import patch, MagicMock

from pkg_resources import DistributionNotFound, require

from amazon.opentelemetry.distro.aws_opentelemetry_distro import AwsOpenTelemetryDistro


class TestAwsOpenTelemetryDistro(TestCase):
    def setUp(self):
        # Store original environment
        self.original_env = os.environ.copy()

    def tearDown(self):
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_env)

    def test_package_available(self):
        try:
            require(["aws-opentelemetry-distro"])
        except DistributionNotFound:
            self.fail("aws-opentelemetry-distro not installed")

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region")
    def test_configure_with_none_region(self, mock_get_aws_region):
        # Set up mock to return None (no region found)
        mock_get_aws_region.return_value = None

        # Enable agent observability
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"

        # Clear any pre-existing endpoint settings
        os.environ.pop("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None)
        os.environ.pop("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", None)

        # Create distro and configure
        distro = AwsOpenTelemetryDistro()

        # Mock the logger to capture warnings
        with self.assertLogs(level="WARNING") as cm:
            distro._configure(apply_patches=False)

        # Verify warning was logged
        self.assertIn("AWS region could not be determined", cm.output[0])

        # Verify endpoints were NOT set when region is None
        self.assertNotIn("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", os.environ)
        self.assertNotIn("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", os.environ)

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region")
    def test_configure_with_valid_region(self, mock_get_aws_region):
        # Set up mock to return a valid region
        mock_get_aws_region.return_value = "us-west-2"

        # Enable agent observability
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"

        # Clear any pre-existing endpoint settings
        os.environ.pop("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None)
        os.environ.pop("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", None)

        # Create distro and configure
        distro = AwsOpenTelemetryDistro()
        distro._configure(apply_patches=False)

        # Verify endpoints were set correctly
        self.assertEqual(
            os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"), "https://xray.us-west-2.amazonaws.com/v1/traces"
        )
        self.assertEqual(
            os.environ.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"), "https://logs.us-west-2.amazonaws.com/v1/logs"
        )

    @patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region")
    def test_agent_observability_all_defaults(self, mock_get_aws_region):
        # Set up mock to return a valid region
        mock_get_aws_region.return_value = "us-east-1"

        # Enable agent observability
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"

        # Clear any pre-existing settings to test defaults
        keys_to_clear = [
            "OTEL_TRACES_EXPORTER",
            "OTEL_LOGS_EXPORTER",
            "OTEL_METRICS_EXPORTER",
            "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT",
            "OTEL_TRACES_SAMPLER",
            "OTEL_PYTHON_DISABLED_INSTRUMENTATIONS",
            "OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED",
            "OTEL_AWS_APPLICATION_SIGNALS_ENABLED",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        ]
        for key in keys_to_clear:
            os.environ.pop(key, None)

        # Create distro and configure
        distro = AwsOpenTelemetryDistro()
        distro._configure(apply_patches=False)

        # Verify all defaults were set correctly
        self.assertEqual(os.environ.get("OTEL_TRACES_EXPORTER"), "otlp")
        self.assertEqual(os.environ.get("OTEL_LOGS_EXPORTER"), "otlp")
        self.assertEqual(os.environ.get("OTEL_METRICS_EXPORTER"), "awsemf")
        self.assertEqual(os.environ.get("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"), "true")
        self.assertEqual(os.environ.get("OTEL_TRACES_SAMPLER"), "parentbased_always_on")
        self.assertEqual(
            os.environ.get("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"),
            "http,sqlalchemy,psycopg2,pymysql,sqlite3,aiopg,asyncpg,mysql_connector,botocore,boto3,urllib3,requests,starlette",
        )
        self.assertEqual(os.environ.get("OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED"), "true")
        self.assertEqual(os.environ.get("OTEL_AWS_APPLICATION_SIGNALS_ENABLED"), "false")
        self.assertEqual(
            os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"), "https://xray.us-east-1.amazonaws.com/v1/traces"
        )
        self.assertEqual(
            os.environ.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"), "https://logs.us-east-1.amazonaws.com/v1/logs"
        )

    def test_agent_observability_disabled_respects_user_settings(self):
        # Ensure agent observability is disabled
        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)

        # Set custom values for some environment variables
        os.environ["OTEL_TRACES_SAMPLER"] = "traceidratio"
        os.environ["OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED"] = "false"

        # Create distro and configure
        distro = AwsOpenTelemetryDistro()
        distro._configure(apply_patches=False)

        # Verify user settings were not overridden
        self.assertEqual(os.environ.get("OTEL_TRACES_SAMPLER"), "traceidratio")
        self.assertEqual(os.environ.get("OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED"), "false")

    def test_agent_observability_enabled_respects_user_settings(self):
        # Enable agent observability
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"

        # Set custom values for some environment variables
        os.environ["OTEL_TRACES_SAMPLER"] = "traceidratio"
        os.environ["OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"] = "django,flask"
        os.environ["OTEL_AWS_APPLICATION_SIGNALS_ENABLED"] = "true"

        # Create distro and configure
        distro = AwsOpenTelemetryDistro()
        distro._configure(apply_patches=False)

        # Verify user settings were not overridden
        self.assertEqual(os.environ.get("OTEL_TRACES_SAMPLER"), "traceidratio")
        self.assertEqual(os.environ.get("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"), "django,flask")
        self.assertEqual(os.environ.get("OTEL_AWS_APPLICATION_SIGNALS_ENABLED"), "true")
