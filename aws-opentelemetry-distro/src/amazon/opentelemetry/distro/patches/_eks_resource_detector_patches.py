# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import base64
import json
import logging

import opentelemetry.sdk.extension.aws.resource.eks as eks_resource

logger = logging.getLogger(__name__)

_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

_original_is_eks = eks_resource._is_eks


def _is_eks_with_fallback(cred_value):  # pylint: disable=broad-exception-caught
    try:
        return _original_is_eks(cred_value)
    except Exception:
        logger.debug("aws-auth configmap check failed, falling back to JWT issuer detection")

    try:
        with open(_TOKEN_PATH, encoding="utf8") as token_file:
            token = token_file.read()
        payload = token.split(".")[1]
        payload += "=" * (4 - len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        issuer = claims.get("iss", "")
        if issuer.startswith("https://oidc.eks."):
            return True
    except Exception as exception:
        logger.debug("JWT issuer fallback also failed: %s", exception)

    return False


def _apply_eks_resource_detector_patches() -> None:
    eks_resource._is_eks = _is_eks_with_fallback
