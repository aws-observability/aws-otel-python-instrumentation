# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing_extensions import override

from amazon.serviceevents.serviceevents_contract_test_base import ServiceEventsContractTestBase

_APP_IMAGE = "aws-application-signals-tests-serviceevents-fastapi-app"


class FastApiServiceEventsTest(ServiceEventsContractTestBase):
    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Uvicorn running on"
