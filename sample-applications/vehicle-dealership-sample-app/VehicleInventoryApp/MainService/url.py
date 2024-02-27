# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from django.urls import path

from . import views

urlpatterns = [
    path("", views.vehicle, name="vehicle"),
    path("<int:vehicle_id>", views.get_vehicle_by_id, name="get_vehicle_by_id"),
    path("name/<str:vehicles_name>", views.get_vehicles_by_name, name="get_vehicles_by_name"),
    path("<int:vehicle_id>/image", views.get_vehicle_image, name="get_vehicle_image"),
    path("image/<str:image_name>", views.image, name="image"),
    path("history/", views.vehicle_purchase_history, name="purchase_history"),
    path(
        "history/<int:vehicle_purchase_history_id>",
        views.get_vehicle_purchase_history_by_id,
        name="get_vehicle_purchase_history_by_id",
    ),
    path("health-check/", views.health_check, name="health_check"),
]
