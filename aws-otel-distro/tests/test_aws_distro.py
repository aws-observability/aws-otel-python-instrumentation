import os
from unittest import TestCase

from pkg_resources import DistributionNotFound, require
from opentelemetry.distro import AWSDistro
from opentelemetry.environment_variables import (
    OTEL_METRICS_EXPORTER,
    OTEL_TRACES_EXPORTER,
)
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_PROTOCOL


class TestAWSDistro(TestCase):
    def test_package_available(self):
        try:
            require(["opentelemetry-distro-aws"])
        except DistributionNotFound:
            self.fail("opentelemetry-distro-aws not installed")

    def test_default_configuration(self):
        distro = AWSDistro()
        self.assertIsNone(os.environ.get(OTEL_TRACES_EXPORTER))
        self.assertIsNone(os.environ.get(OTEL_METRICS_EXPORTER))
        distro.configure()
        self.assertEqual(
            "console", os.environ.get(OTEL_TRACES_EXPORTER)
        )
        self.assertEqual(
            "console", os.environ.get(OTEL_METRICS_EXPORTER)
        )
        self.assertEqual(
            "grpc", os.environ.get(OTEL_EXPORTER_OTLP_PROTOCOL)
        )