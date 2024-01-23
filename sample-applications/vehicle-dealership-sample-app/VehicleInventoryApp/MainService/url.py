# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from django.urls import path

from . import views

urlpatterns = [
    path("", views.vehicle, name="vehicle"),
    path("<int:vehicle_id>", views.get_vehicle_by_id, name="get_vehicle_by_id"),
    path("<int:vehicle_id>/image", views.get_vehicle_image, name="get_vehicle_image"),
]
