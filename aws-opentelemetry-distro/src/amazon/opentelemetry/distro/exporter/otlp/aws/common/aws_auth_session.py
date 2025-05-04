import logging

import requests

from amazon.opentelemetry.distro._utils import is_installed

_logger = logging.getLogger(__name__)


class AwsAuthSession(requests.Session):

    def __init__(self, aws_region, service):

        # Requires botocore to be installed to sign the headers. However,
        # some users might not need to use this exporter. In order not conflict
        # with existing behavior, we check for botocore before initializing this exporter.

        if aws_region and service and is_installed("botocore"):
            # pylint: disable=import-outside-toplevel
            from botocore import auth, awsrequest, session

            self._boto_auth = auth
            self._boto_aws_request = awsrequest
            self._boto_session = session.Session()

            self._aws_region = aws_region
            self._service = service
            self._has_required_dependencies = True

        else:
            print(
                "botocore is required to enable SigV4 Authentication. Please install it using `pip install botocore`",
            )

        super().__init__()

    def request(self, method, url, data=None, headers=None, *args, **kwargs):
        if self._has_required_dependencies:

            credentials = self._boto_session.get_credentials()

            if credentials is not None:
                signer = self._boto_auth.SigV4Auth(credentials, self._service, self._aws_region)

                request = self._boto_aws_request.AWSRequest(
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

        return super().request(method, url, data=data, headers=headers, *args, **kwargs)

    def close(self):
        super().close()
