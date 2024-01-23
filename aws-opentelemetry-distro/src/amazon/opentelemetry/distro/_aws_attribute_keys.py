# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Utility module holding attribute keys with special meaning to AWS components"""

AWS_SPAN_KIND: str = "aws.span.kind"
AWS_LOCAL_SERVICE: str = "aws.local.service"
AWS_LOCAL_OPERATION: str = "aws.local.operation"
AWS_REMOTE_SERVICE: str = "aws.remote.service"
AWS_REMOTE_OPERATION: str = "aws.remote.operation"
AWS_REMOTE_TARGET: str = "aws.remote.target"
AWS_SDK_DESCENDANT: str = "aws.sdk.descendant"
AWS_CONSUMER_PARENT_SPAN_KIND: str = "aws.consumer.parent.span.kind"
