# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict, List, Optional


class PresignedAwsUrl:
    """A parsed SigV4 presigned AWS URL.

    Carries the request context needed for attribution that a plain parsed URL cannot express: the
    HTTP method (which comes from the span, not the URL) and the parsed query parameters. The host
    and path come from the URL itself.

    The signing service is intentionally not carried here: it is derived from the SigV4 credential
    scope, which the agent's URL sanitization redacts. Service identity is instead determined from
    the endpoint hostname by the service-specific attributor.
    """

    def __init__(
        self,
        host: Optional[str],
        path: str,
        http_method: Optional[str],
        query_parameters: Dict[str, List[str]],
    ):
        self._host = host
        self._path = path if path else "/"
        self._http_method = http_method
        self._query_parameters = query_parameters

    def get_http_method(self) -> Optional[str]:
        return self._http_method

    def get_host(self) -> Optional[str]:
        return self._host

    def get_path(self) -> str:
        return self._path

    def get_first_query_parameter_value(self, name: str) -> Optional[str]:
        values = self._query_parameters.get(name)
        if not values:
            return None
        return values[0]
