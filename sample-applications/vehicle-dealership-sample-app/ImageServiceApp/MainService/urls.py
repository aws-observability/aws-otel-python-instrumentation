# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("name/<str:image_name>", views.handle_image, name="image"),
    path("remote-image", views.get_remote_image, name="remote-image"),
    path("health-check/", views.health_check, name="health_check"),
]
