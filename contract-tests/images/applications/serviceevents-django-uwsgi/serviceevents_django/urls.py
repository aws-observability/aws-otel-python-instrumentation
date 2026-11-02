# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from django.urls import path
from serviceevents_django.views import error, exception_endpoint, fault, health, success

urlpatterns = [
    path("health", health),
    path("success", success),
    path("error", error),
    path("fault", fault),
    path("exception", exception_endpoint),
]
