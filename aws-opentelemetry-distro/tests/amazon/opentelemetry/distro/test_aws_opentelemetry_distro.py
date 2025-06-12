# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from unittest import TestCase
from unittest.mock import patch

from pkg_resources import DistributionNotFound, require


class TestAwsOpenTelemetryDistro(TestCase):
    def test_package_available(self):
        try:
            require(["aws-opentelemetry-distro"])
        except DistributionNotFound:
            self.fail("aws-opentelemetry-distro not installed")

    def setUp(self):
        # Store original env vars for agent observability tests
        self.original_env = {}
        env_vars = [
            "AGENT_OBSERVABILITY_ENABLED",
            "OTEL_TRACES_SAMPLER",
            "OTEL_PYTHON_DISABLED_INSTRUMENTATIONS",
            "OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED",
            "OTEL_AWS_APPLICATION_SIGNALS_ENABLED",
        ]
        for var in env_vars:
            self.original_env[var] = os.environ.get(var)
            os.environ.pop(var, None)

    def tearDown(self):
        # Restore original env vars
        for var, value in self.original_env.items():
            if value is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = value

    def test_agent_observability_sets_new_defaults(self):
        # Set up the environment to trigger agent observability
        os.environ["AGENT_OBSERVABILITY_ENABLED"] = "true"

        # Import and configure
        from amazon.opentelemetry.distro.aws_opentelemetry_distro import AwsOpenTelemetryDistro

        with patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches"):
            with patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.get_aws_region", return_value="us-west-2"):
                # We need to mock the parent class to avoid its side effects
                with patch("opentelemetry.distro.OpenTelemetryDistro._configure"):
                    distro = AwsOpenTelemetryDistro()
                    distro._configure()

        # Check the new defaults are set
        self.assertEqual(os.environ.get("OTEL_TRACES_SAMPLER"), "parentbased_always_on")
        self.assertEqual(
            os.environ.get("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS"),
            "http,sqlalchemy,psycopg2,pymysql,sqlite3,aiopg,asyncpg,mysql_connector,botocore,boto3,urllib3,requests,starlette",
        )
        self.assertEqual(os.environ.get("OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED"), "true")
        self.assertEqual(os.environ.get("OTEL_AWS_APPLICATION_SIGNALS_ENABLED"), "false")

    def test_new_defaults_not_set_when_agent_observability_disabled(self):
        # Don't set AGENT_OBSERVABILITY_ENABLED or set it to false
        os.environ.pop("AGENT_OBSERVABILITY_ENABLED", None)

        from amazon.opentelemetry.distro.aws_opentelemetry_distro import AwsOpenTelemetryDistro

        with patch("amazon.opentelemetry.distro.aws_opentelemetry_distro.apply_instrumentation_patches"):
            with patch("opentelemetry.distro.OpenTelemetryDistro._configure"):
                distro = AwsOpenTelemetryDistro()
                distro._configure()

        # These should not be set when agent observability is disabled
        self.assertNotIn("OTEL_TRACES_SAMPLER", os.environ)
        self.assertNotIn("OTEL_PYTHON_DISABLED_INSTRUMENTATIONS", os.environ)
        self.assertNotIn("OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED", os.environ)
        self.assertNotIn("OTEL_AWS_APPLICATION_SIGNALS_ENABLED", os.environ)
