# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from opentelemetry.sdk._logs import LoggerProvider


class LLOHandler:
    """
    Utility class for handling Large Language Objects (LLO) in OpenTelemetry spans.

    LLOHandler performs three primary functions:
    1. Identifies input/output prompt content in spans
    2. Extracts and transforms these attributes into an OpenTelemetry Gen AI Event
    3. Filters input/output prompts from spans to maintain privacy and reduce span size

    This LLOHandler supports the following third-party instrumentation libraries:
    - Strands
    - OpenInference
    - Traceloop/OpenLLMetry
    - OpenLIT
    """

    def __init__(self, logger_provider: LoggerProvider):
        """
        Initialize an LLOHandler with the specified logger provider.

        This constructor sets up the event logger provider, configures the event logger,
        and initializes the patterns used to identify LLO attributes.

        Args:
            logger_provider: The OpenTelemetry LoggerProvider used for emitting events.
                           Global LoggerProvider instance injected from our AwsOpenTelemetryConfigurator
        """
