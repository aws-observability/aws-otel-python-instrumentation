# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from amazon.opentelemetry.distro.aws_opentelemetry_configurator import AwsOpenTelemetryConfigurator
from opentelemetry.sdk._configuration import _OTelSDKConfigurator


class TestAwsOpenTelemetryConfigurator(TestCase):
    def test_default_configuration(self):
        configurator = AwsOpenTelemetryConfigurator()
        self.assertTrue(isinstance(configurator, _OTelSDKConfigurator))
