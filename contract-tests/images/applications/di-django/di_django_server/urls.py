# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# pylint: skip-file
"""URL configuration for the DI contract test Django app.

The `path()` calls below capture direct references to view functions defined
in the sibling `api.views` module. After DI replaces those functions on
`api.views` via setattr, the URLPattern.callback cache here still points to
the originals — which is exactly the bug `_patch_django_url_patterns` fixes.
"""
from api.views import (
    error_endpoint,
    fault_endpoint,
    health,
    limited_endpoint,
    limits_collection_endpoint,
    limits_string_endpoint,
    line_level_endpoint,
    probe_endpoint,
    shared_endpoint,
    success,
)
from django.urls import path

urlpatterns = [
    path("health", health),
    path("success", success),
    path("probe", probe_endpoint),
    path("line-level", line_level_endpoint),
    path("limited", limited_endpoint),
    path("shared", shared_endpoint),
    path("limits-string", limits_string_endpoint),
    path("limits-collection", limits_collection_endpoint),
    path("error", error_endpoint),
    path("fault", fault_endpoint),
]
