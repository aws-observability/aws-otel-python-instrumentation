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
