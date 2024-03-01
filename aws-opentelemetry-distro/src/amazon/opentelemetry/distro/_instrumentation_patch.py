# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import importlib
import ssl
from urllib.request import Request, urlopen

import opentelemetry.sdk.extension.aws.resource.ec2 as ec2_resource
import opentelemetry.sdk.extension.aws.resource.eks as eks_resource
from opentelemetry.instrumentation.botocore.extensions import _KNOWN_EXTENSIONS
from opentelemetry.instrumentation.botocore.extensions.sqs import _SqsExtension
from opentelemetry.instrumentation.botocore.extensions.types import _AttributeMapT, _AwsSdkExtension
from opentelemetry.semconv.trace import SpanAttributes


def apply_instrumentation_patches() -> None:
    """Apply patches to upstream libraries.

    This method is invoked to apply changes to upstream libraries, typically when changes to upstream
    are required on a timeline that cannot wait for upstream release. Generally speaking, patches should be short-term
    local solutions that are comparable to long-term upstream solutions.

    Where possible, automated testing should be run to catch upstream changes resulting in broken patches
    """
    _apply_botocore_instrumentation_patches()

    _apply_resource_detector_patches()


def _apply_botocore_instrumentation_patches() -> None:
    """Botocore instrumentation patches

    Adds patches to provide additional support and Java parity for Kinesis, S3, and SQS.
    """
    _apply_botocore_kinesis_patch()
    _apply_botocore_s3_patch()
    _apply_botocore_sqs_patch()


def _apply_resource_detector_patches() -> None:
    """AWS Resource Detector patches for getting the following unreleased change (as of v2.0.1) in the upstream:
    https://github.com/open-telemetry/opentelemetry-python-contrib/commit/a5ec3f7f55494cb80b4b53c652e31c465b8d5e80
    """

    def patch_ec2_aws_http_request(method, path, headers):
        with urlopen(
            Request("http://169.254.169.254" + path, headers=headers, method=method),
            timeout=5,
        ) as response:
            return response.read().decode("utf-8")

    def patch_eks_aws_http_request(method, path, cred_value):
        with urlopen(
            Request(
                "https://kubernetes.default.svc" + path,
                headers={"Authorization": cred_value},
                method=method,
            ),
            timeout=5,
            context=ssl.create_default_context(cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"),
        ) as response:
            return response.read().decode("utf-8")

    ec2_resource._aws_http_request = patch_ec2_aws_http_request
    eks_resource._aws_http_request = patch_eks_aws_http_request


def _apply_botocore_kinesis_patch() -> None:
    """Botocore instrumentation patch for Kinesis

    This patch adds an extension to the upstream's list of known extension for Kinesis. Extensions allow for custom
    logic for adding service-specific information to spans, such as attributes. Specifically, we are adding logic to add
    the `aws.kinesis.stream_name` attribute, to be used to generate RemoteTarget and achieve parity with the Java
    instrumentation.
    """
    _KNOWN_EXTENSIONS["kinesis"] = _lazy_load(".", "_KinesisExtension")


def _apply_botocore_s3_patch() -> None:
    """Botocore instrumentation patch for S3

    This patch adds an extension to the upstream's list of known extension for S3. Extensions allow for custom
    logic for adding service-specific information to spans, such as attributes. Specifically, we are adding logic to add
    the AWS_S3_BUCKET attribute, to be used to generate RemoteTarget and achieve parity with the Java instrumentation.
    Callout that AWS_S3_BUCKET is in the AWS Semantic Conventions, and is simply not implemented in Python
    instrumentation.
    """
    _KNOWN_EXTENSIONS["s3"] = _lazy_load(".", "_S3Extension")


def _apply_botocore_sqs_patch() -> None:
    """Botocore instrumentation patch for SQS

    This patch extends the existing upstream extension for SQS. Extensions allow for custom logic for adding
    service-specific information to spans, such as attributes. Specifically, we are adding logic to add
    `aws.sqs.queue_url` and `aws.sqs.queue_name` attributes, to be used to generate RemoteTarget and achieve parity
    with the Java instrumentation. Callout that today, the upstream logic adds `aws.queue_url` but we feel that
    `aws.sqs` is more in line with existing AWS Semantic Convention attributes like `AWS_S3_BUCKET`, etc.
    """
    old_extract_attributes = _SqsExtension.extract_attributes

    def patch_extract_attributes(self, attributes: _AttributeMapT):
        old_extract_attributes(self, attributes)
        queue_name = self._call_context.params.get("QueueName")
        queue_url = self._call_context.params.get("QueueUrl")
        if queue_name:
            attributes["aws.sqs.queue_name"] = queue_name
        if queue_url:
            attributes["aws.sqs.queue_url"] = queue_url

    _SqsExtension.extract_attributes = patch_extract_attributes


# The OpenTelemetry Authors code
def _lazy_load(module, cls):
    """Clone of upstream opentelemetry.instrumentation.botocore.extensions.lazy_load

    The logic in this method is dependent on the file path of where it is implemented, so must be cloned here.
    """

    def loader():
        imported_mod = importlib.import_module(module, __name__)
        return getattr(imported_mod, cls, None)

    return loader


# END The OpenTelemetry Authors code


class _S3Extension(_AwsSdkExtension):
    def extract_attributes(self, attributes: _AttributeMapT):
        bucket_name = self._call_context.params.get("Bucket")
        if bucket_name:
            attributes[SpanAttributes.AWS_S3_BUCKET] = bucket_name


class _KinesisExtension(_AwsSdkExtension):
    def extract_attributes(self, attributes: _AttributeMapT):
        stream_name = self._call_context.params.get("StreamName")
        if stream_name:
            attributes["aws.kinesis.stream_name"] = stream_name
