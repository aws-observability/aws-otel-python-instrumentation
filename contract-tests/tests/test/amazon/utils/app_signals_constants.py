# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Constants for attributes and metric names defined in AppSignals.
"""

# Metric names
LATENCY_METRIC: str = "latency"
ERROR_METRIC: str = "error"
FAULT_METRIC: str = "fault"

# Attribute names
AWS_LOCAL_SERVICE: str = "aws.local.service"
AWS_LOCAL_OPERATION: str = "aws.local.operation"
AWS_REMOTE_SERVICE: str = "aws.remote.service"
AWS_REMOTE_OPERATION: str = "aws.remote.operation"
AWS_REMOTE_TARGET: str = "aws.remote.target"
AWS_SPAN_KIND: str = "aws.span.kind"
