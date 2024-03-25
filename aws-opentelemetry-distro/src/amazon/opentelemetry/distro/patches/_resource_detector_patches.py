# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Modifications Copyright The OpenTelemetry Authors. Licensed under the Apache License 2.0 License.
import ssl
from urllib.request import Request, urlopen

import opentelemetry.sdk.extension.aws.resource.ec2 as ec2_resource
import opentelemetry.sdk.extension.aws.resource.eks as eks_resource


# The OpenTelemetry Authors code
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


# END The OpenTelemetry Authors code
