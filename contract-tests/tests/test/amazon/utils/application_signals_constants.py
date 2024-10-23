# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Constants for attributes and metric names defined in Application Signals.
"""

# Metric names
LATENCY_METRIC: str = "latency"
ERROR_METRIC: str = "error"
FAULT_METRIC: str = "fault"

PYTHON_PROCESS_GC_COUNT = "process.runtime.cpython.gc_count"
PYTHON_PROCESS_MEMORY_USED = "process.runtime.cpython.memory"
PYTHON_PROCESS_THREAD_COUNT = "process.runtime.cpython.thread_count"
PYTHON_PROCESS_CPU_TIME = "process.runtime.cpython.cpu_time"
PYTHON_PROCESS_CPU_UTILIZATION = "process.runtime.cpython.cpu.utilization"

# Attribute names
AWS_CLOUDFORMATION_PRIMARY_IDENTIFIER: str = "aws.remote.resource.cfn.primary.identifier"
AWS_LOCAL_SERVICE: str = "aws.local.service"
AWS_LOCAL_OPERATION: str = "aws.local.operation"
AWS_REMOTE_DB_USER: str = "aws.remote.db.user"
AWS_REMOTE_SERVICE: str = "aws.remote.service"
AWS_REMOTE_OPERATION: str = "aws.remote.operation"
AWS_REMOTE_RESOURCE_TYPE: str = "aws.remote.resource.type"
AWS_REMOTE_RESOURCE_IDENTIFIER: str = "aws.remote.resource.identifier"
AWS_SPAN_KIND: str = "aws.span.kind"
