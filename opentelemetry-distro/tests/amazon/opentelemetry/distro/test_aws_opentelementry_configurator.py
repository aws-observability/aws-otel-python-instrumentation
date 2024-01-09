# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase

from opentelemetry.distro.aws_opentelemetry_configurator import AwsOpenTelemetryConfigurator, AwsTracerProvider


class TestAwsOpenTelemetryConfigurator(TestCase):
    # pylint: disable=no-self-use
    def test_default_configuration(self):
        configurator = AwsOpenTelemetryConfigurator()
        configurator.configure()
        trace_provider = configurator.get_trace_provider()
        assert isinstance(trace_provider, AwsTracerProvider)
