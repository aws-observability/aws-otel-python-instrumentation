#!/usr/bin/env python
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import sys

from django.core.management import execute_from_command_line

# Importing execute_from_command_line above does not read settings; only the call
# below does, so the env default still takes effect before any command runs.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "serviceevents_django.settings")

execute_from_command_line(sys.argv)
