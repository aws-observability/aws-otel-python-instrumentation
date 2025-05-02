# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from typing import Optional

import requests

from amazon.opentelemetry.distro._utils import is_installed

_logger = logging.getLogger(__name__)


class OTLPBaseAwsExporter(ABC):
    """
    Abstract base class providing shared functionality for AWS (OTLP) exporters authenticated with
    Sigv4.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        rsession: Optional[requests.Session] = None,
    ):

        self._aws_region = None
        self._has_required_dependencies = False
        self._endpoint = endpoint
        self._session = rsession

        # Requires botocore to be installed to sign the headers. However,
        # some users might not need to use this exporter. In order not conflict
        # with existing behavior, we check for botocore before initializing this exporter.

        if endpoint and is_installed("botocore"):
            # pylint: disable=import-outside-toplevel
            from botocore import auth, awsrequest, session

            self._boto_auth = auth
            self._boto_aws_request = awsrequest
            self._boto_session = session.Session()

            # Assumes only valid endpoints passed are of XRay OTLP format.
            # The only usecase for this class would be for ADOT Python Auto Instrumentation and that already validates
            # the endpoint to be an XRay OTLP endpoint.
            self._aws_region = endpoint.split(".")[1]
            self._has_required_dependencies = True

        else:
            _logger.error(
                "botocore is required to export to %s. Please install it using `pip install botocore`",
                endpoint,
            )

    @abstractmethod
    def get_service(self):
        pass

    def inject_sigv4_auth(self, serialized_data):
        """
        Injects Sigv4 authentication headers to this exporter's session object.
        Does nothing if obtaining or signing the credentials fails.
        """

        if self._has_required_dependencies:
            request = self._boto_aws_request.AWSRequest(
                method="POST",
                url=self._endpoint,
                data=serialized_data,
                headers={"Content-Type": "application/x-protobuf"},
            )

            credentials = self._boto_session.get_credentials()

            if credentials is not None:
                signer = self._boto_auth.SigV4Auth(credentials, self.get_service(), self._aws_region)

                try:
                    signer.add_auth(request)
                    self._session.headers.update(dict(request.headers))

                except Exception as signing_error:  # pylint: disable=broad-except
                    _logger.error("Failed to sign request: %s", signing_error)
        else:
            _logger.debug("botocore is not installed. Failed to sign request to export traces to: %s", self._endpoint)
