from django.urls import path, re_path

from . import views

urlpatterns = [
    path("vehicle-inventory/", views.vehicle, name="vehicle"),
    path('vehicle-inventory/<int:vehicle_id>', views.get_vehicle_by_id, name="get_vehicle_by_id"),
    path('vehicle-inventory/<int:vehicle_id>/image', views.get_vehicle_image, name="get_vehicle_image"),
    re_path(r'^images/', views.redirect, name="redirect")
]