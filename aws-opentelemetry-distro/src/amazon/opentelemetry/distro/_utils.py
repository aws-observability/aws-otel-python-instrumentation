# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re


def is_otlp_endpoint_cloudwatch(otlp_endpoint=None):
    # Detects if it's the OTLP endpoint in CloudWatchs
    if not otlp_endpoint:
        return False

    pattern = r"https://xray\.([a-z0-9-]+)\.amazonaws\.com/v1/traces$"

    return bool(re.match(pattern, otlp_endpoint.lower()))
