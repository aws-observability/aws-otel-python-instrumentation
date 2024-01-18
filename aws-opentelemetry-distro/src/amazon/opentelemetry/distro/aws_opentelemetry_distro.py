# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
from logging import getLogger

from opentelemetry.distro import OpenTelemetryDistro
from opentelemetry.environment_variables import OTEL_PROPAGATORS, OTEL_PYTHON_ID_GENERATOR

logger = getLogger(__name__)


class AwsOpenTelemetryDistro(OpenTelemetryDistro):
    def _configure(self, **kwargs):
        super(AwsOpenTelemetryDistro, self)._configure()
        os.environ.setdefault(OTEL_PROPAGATORS, "xray,tracecontext,b3,b3multi")
        os.environ.setdefault(OTEL_PYTHON_ID_GENERATOR, "xray")
        # TODO: Unlike opentelemetry Java, "otel.aws.imds.endpointOverride" is not configured on python.
        #  Need to figure out if we rely on ec2 and eks resource to get context about the platform for python
        #  and if we need endpoint override support.
