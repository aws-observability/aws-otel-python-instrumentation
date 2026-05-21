# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from threading import Lock

import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import Session

from amazon.opentelemetry.distro.patches._pip_system_certs_patches import (
    apply_pip_system_certs_compatibility_patch,
)

_logger = logging.getLogger(__name__)


class AwsAuthSession(requests.Session):
    """
    A custom requests Session that adds AWS SigV4 authentication to HTTP requests.

    This class extends the standard requests.Session to automatically sign requests
    with AWS Signature Version 4 (SigV4) authentication. It's specifically designed
    for use with the OpenTelemetry Logs and Traces exporters that send data to AWS OTLP endpoints:
    X-Ray (traces) and CloudWatch Logs.

    The session requires botocore to be installed for signing headers. If botocore
    is not available, the session will fall back to standard unauthenticated requests
    and log an error message.

    Usage:
        session = AwsAuthSession(aws_region="us-west-2", service="logs")
        response = session.request("POST", "https://logs.us-west-2.amazonaws.com/v1/logs",
                                    data=payload, headers=headers)

    Args:
        aws_region (str): The AWS region to use for signing (e.g., "us-east-1")
        service (str): The AWS service name for signing (e.g., "logs" or "xray")
    """

    def __init__(self, aws_region: str, service: str, session: Session):
        self._aws_region: str = aws_region
        self._service: str = service
        self._session: Session = session

        # Cached credentials are resolved on the first ``request()`` call. The returned
        # ``Credentials`` / ``RefreshableCredentials`` object handles its own expiry and
        # rotation when its attributes are accessed, so caching the reference does not
        # cache the underlying credential values.
        self._credentials = None
        self._credentials_resolved = False
        self._credentials_lock = Lock()

        super().__init__()

    def _ensure_initialized(self) -> None:
        """Apply one-time, deferred initialization on the first ``request()`` call.

        This runs after sitecustomize has fully completed (i.e., after any ``.pth``
        based ``ssl.SSLContext`` injection from packages such as ``pip_system_certs``),
        which is the only point at which we can safely re-align stale ``SSLContext``
        references captured by ``botocore`` / ``urllib3`` during ADOT startup.

        Credentials are also resolved once here. ``RefreshableCredentials`` handles
        rotation internally on attribute access, so caching the reference is safe.
        """
        if self._credentials_resolved:
            return

        with self._credentials_lock:
            if self._credentials_resolved:
                return

            # Realign stale ssl.SSLContext references in botocore / urllib3 before
            # the first credential resolution constructs an SSL context. This is a
            # no-op when pip_system_certs is not installed.
            try:
                apply_pip_system_certs_compatibility_patch()
            except Exception as patch_error:  # pylint: disable=broad-except
                _logger.warning(
                    "Failed to apply pip_system_certs compatibility patch: %s", patch_error
                )

            try:
                self._credentials = self._session.get_credentials()
            except Exception as cred_error:  # pylint: disable=broad-except
                _logger.error("Failed to load AWS Credentials: %s", cred_error)
                self._credentials = None

            self._credentials_resolved = True

    def request(self, method, url, *args, data=None, headers=None, **kwargs):
        self._ensure_initialized()

        if self._credentials:
            signer = SigV4Auth(self._credentials, self._service, self._aws_region)
            request = AWSRequest(
                method="POST",
                url=url,
                data=data,
                headers={"Content-Type": "application/x-protobuf"},
            )

            try:
                signer.add_auth(request)

                if headers is None:
                    headers = {}

                headers.update(dict(request.headers))

            except Exception as signing_error:  # pylint: disable=broad-except
                _logger.error("Failed to sign request: %s", signing_error)
        else:
            _logger.error("Failed to load AWS Credentials")

        return super().request(method=method, url=url, *args, data=data, headers=headers, **kwargs)
