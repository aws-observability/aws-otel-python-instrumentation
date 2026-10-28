# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import re
from threading import Lock
from typing import Optional
from urllib.parse import urlparse

import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import Session

from amazon.opentelemetry.distro._utils import AWS_DNS_SUFFIX_PATTERN, get_aws_session
from amazon.opentelemetry.distro.exporter.otlp.aws.environment_variables import AWS_EXPORTER_SERVICE
from amazon.opentelemetry.distro.patches._pip_system_certs_patches import apply_pip_system_certs_compatibility_patch

_logger = logging.getLogger(__name__)


class AwsAuthSession(requests.Session):
    """
    A custom requests Session that adds AWS SigV4 authentication to HTTP requests.

    This class extends the standard requests.Session to automatically sign requests
    with AWS Signature Version 4 (SigV4) authentication. It is used by the
    OpenTelemetry HTTP exporters for logs, metrics, and traces sent to AWS OTLP endpoints.

    Botocore is required for signing. When no botocore session is supplied, one is
    created using the default AWS credential chain. Credentials are resolved on
    the first request.

    Usage:
        session = AwsAuthSession(aws_region="us-west-2", service="logs")
        response = session.request("POST", "https://logs.us-west-2.amazonaws.com/v1/logs",
                                    data=payload, headers=headers)

    Args:
        aws_region (str): Optional signing region. Falls back to ``AWS_REGION``,
            then ``AWS_DEFAULT_REGION``, then the endpoint hostname, then the
            botocore session's configured region.
        service (str): Optional signing service (e.g., "logs", "xray", or "osis").
            Falls back to ``AWS_EXPORTER_SERVICE``.
        session (Session): Optional botocore session; defaults to the AWS credential chain.
        endpoint (str): Optional resolved OTLP endpoint used to infer the region when
            no explicit region or AWS region environment variable is set. Region
            labels are detected within recognized AWS domain suffixes, including
            OSIS pipeline, VPC endpoint, and generic service hostnames.

    Raises:
        ValueError: If the signing service or region cannot be resolved, or no
            botocore session is available.
    """

    def __init__(
        self,
        aws_region: Optional[str] = None,
        service: Optional[str] = None,
        session: Optional[Session] = None,
        endpoint: Optional[str] = None,
    ):
        service = service or os.environ.get(AWS_EXPORTER_SERVICE)
        if not service:
            raise ValueError("AwsAuthSession requires a signing service or AWS_EXPORTER_SERVICE.")

        if session is None:
            session = get_aws_session()
        if session is None:
            raise ValueError("The otlp/sigv4 exporter requires botocore; install aws-opentelemetry-distro[patch].")

        aws_region = aws_region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if not aws_region:
            host = urlparse(endpoint or "").hostname or ""
            # Locate a region label independently of the service name and its position in the hostname.
            match = re.fullmatch(
                rf"(?:[a-z0-9-]+\.)*([a-z]+(?:-[a-z]+)+-\d+)(?:\.[a-z0-9-]+)*\.{AWS_DNS_SUFFIX_PATTERN}",
                host,
            )
            aws_region = match.group(1) if match else session.get_config_variable("region")

        if not aws_region:
            raise ValueError(
                "The otlp/sigv4 exporter requires an AWS endpoint region, AWS_REGION, or AWS_DEFAULT_REGION."
            )

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

        # Tracks whether the most recent credential resolution attempt already logged
        # an error (because ``get_credentials()`` raised). When True, ``request()``
        # skips its own "Failed to load AWS Credentials" log to avoid double-logging
        # the same failure. It stays False when ``get_credentials()`` returns ``None``
        # without raising, so that the "no credentials configured" case is still
        # surfaced exactly once by ``request()``.
        self._credentials_error_logged = False

        super().__init__()

    def _ensure_initialized(self) -> None:
        """Apply one-time, deferred initialization on the first ``request()`` call.

        This runs after sitecustomize has fully completed (i.e., after any ``.pth``
        based ``ssl.SSLContext`` injection from packages such as ``pip_system_certs``),
        which is the only point at which we can safely re-align stale ``SSLContext``
        references captured by ``botocore`` / ``urllib3`` during ADOT startup.

        Credentials are also resolved once here. ``RefreshableCredentials`` handles
        rotation internally on attribute access, so caching the reference is safe.

        On a transient credential resolution failure (e.g., IMDS timeout), the
        ``_credentials_resolved`` flag is left ``False`` so the next ``request()`` call
        will retry. A failure can surface as an exception or as ``None``: botocore's
        instance metadata provider returns ``None`` rather than raising when the
        metadata service times out. Only a successful resolution that yields
        credentials latches the flag, matching the original "retry every request"
        behavior for the failure path while keeping the SSL-context-construction cost
        amortized to once on the success path.

        Note: the read of ``_credentials_resolved`` outside the lock is safe because
        Python's GIL makes attribute reads/writes atomic. On free-threaded Python
        builds (3.13t+) this would need a memory barrier; revisit if/when we
        support those.
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
                _logger.warning("Failed to apply pip_system_certs compatibility patch: %s", patch_error)

            try:
                self._credentials = self._session.get_credentials()
            except Exception as cred_error:  # pylint: disable=broad-except
                # Don't latch _credentials_resolved on failure - leave it False so
                # the next request retries credential resolution. This preserves
                # self-healing behavior on transient errors (e.g., IMDS timeouts).
                _logger.error("Failed to load AWS Credentials: %s", cred_error)
                self._credentials = None
                self._credentials_error_logged = True
                return

            # Clear the error-logged flag so request() can surface a None result.
            self._credentials_error_logged = False

            # A None result is indistinguishable from a transient lookup failure (botocore
            # swallows IMDS timeouts and returns None), so only latch once credentials were
            # found. Otherwise one failed lookup would leave every later export unsigned.
            if self._credentials is not None:
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

                # Copy rather than mutate: the upstream OTLP HTTP client passes the same headers
                # dict on every request, so signing headers must not leak into it.
                headers = {**(headers or {}), **dict(request.headers)}

            except Exception as signing_error:  # pylint: disable=broad-except
                _logger.error("Failed to sign request: %s", signing_error)
        elif not self._credentials_error_logged:
            # Only log here when get_credentials() returned None without raising
            # (e.g., no credential provider configured). The exception path already
            # logged the failure with detail in _ensure_initialized, so we avoid
            # double-logging the same error.
            _logger.error("Failed to load AWS Credentials")

        return super().request(method=method, url=url, *args, data=data, headers=headers, **kwargs)
