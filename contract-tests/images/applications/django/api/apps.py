"""
This application is created by "python manage.py startapp api"
Modified views.py and ../django_server/urls.py to listen on specific endpoint.
Unused file have been removed.
"""

from django.apps import AppConfig


class ApiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "api"
