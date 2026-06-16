# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# pylint: skip-file
"""Minimal Django settings for the DI contract test app."""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "di-contract-test-not-a-secret"  # noqa: S105
DEBUG = False
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "api",
]

MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "di_django_server.urls"

TEMPLATES = []

USE_TZ = True
