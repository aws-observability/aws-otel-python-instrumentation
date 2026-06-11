# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict

from typing_extensions import override

from amazon.serviceevents.serviceevents_contract_test_base import ServiceEventsContractTestBase

_APP_IMAGE = "aws-application-signals-tests-serviceevents-django-app"


class DjangoServiceEventsTest(ServiceEventsContractTestBase):
    __test__ = True

    @override
    @staticmethod
    def get_application_image_name() -> str:
        return _APP_IMAGE

    @override
    def get_application_wait_pattern(self) -> str:
        return "Quit the server with CONTROL-C."

    @override
    def get_application_extra_environment_variables(self) -> Dict[str, str]:
        return {"DJANGO_SETTINGS_MODULE": "serviceevents_django.settings"}
