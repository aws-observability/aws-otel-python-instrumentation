# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os

from django.core.wsgi import get_wsgi_application

# Importing get_wsgi_application above does not read settings; only the call below
# does, so the env default still takes effect before the application is built.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "serviceevents_django.settings")

application = get_wsgi_application()
