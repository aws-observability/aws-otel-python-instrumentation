# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "serviceevents-test-secret"

DEBUG = True

ALLOWED_HOSTS = ["*"]

ROOT_URLCONF = "serviceevents_django.urls"

WSGI_APPLICATION = "serviceevents_django.wsgi.application"

MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
]
