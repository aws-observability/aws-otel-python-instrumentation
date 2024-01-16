# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import getLogger

from opentelemetry.distro import OpenTelemetryDistro

logger = getLogger(__name__)


class AwsOpenTelemetryDistro(OpenTelemetryDistro):
    def _configure(self, **kwargs):
        super(AwsOpenTelemetryDistro, self)._configure()
