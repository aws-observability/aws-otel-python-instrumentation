#!/usr/bin/env python
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# pylint: skip-file
"""Django's command-line utility for the DI contract test app."""
import os
import sys


def main():
    """Run administrative tasks."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "di_django_server.settings")
    # Start the mock DI API + register configs BEFORE Django boots so the DI
    # poller's first /list-instrumentation-configurations call (after Django
    # imports) finds them ready.
    from api.di_configs import (  # pylint: disable=import-outside-toplevel
        BREAKPOINT_CONFIGS,
        PROBE_CONFIGS,
    )
    from mock_di_api import (  # pylint: disable=import-outside-toplevel
        set_breakpoint_configs,
        set_probe_configs,
        start_mock_api,
    )

    set_breakpoint_configs(BREAKPOINT_CONFIGS)
    set_probe_configs(PROBE_CONFIGS)
    start_mock_api(port=3030)

    try:
        from django.core.management import execute_from_command_line  # pylint: disable=import-outside-toplevel
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
