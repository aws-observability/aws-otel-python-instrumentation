# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from logging import getLogger

from opentelemetry.instrumentation.distro import BaseDistro

logger = getLogger(__name__)


class AwsOpenTelemetryDistro(BaseDistro):
    def _configure(self, **kwargs):
        super(AwsOpenTelemetryDistro, self)._configure()
