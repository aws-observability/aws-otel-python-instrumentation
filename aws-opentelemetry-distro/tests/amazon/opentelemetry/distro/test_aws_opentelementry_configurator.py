# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import AwsOpenTelemetryConfigurator, AwsTracerProvider


class TestAwsOpenTelemetryConfigurator(TestCase):
    def test_default_configuration(self):
        configurator = AwsOpenTelemetryConfigurator()
        configurator.configure()
        trace_provider = configurator.get_trace_provider()
        self.assertTrue(isinstance(trace_provider, AwsTracerProvider))
