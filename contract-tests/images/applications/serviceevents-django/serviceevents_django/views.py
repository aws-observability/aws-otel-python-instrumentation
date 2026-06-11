# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging

from django.http import HttpResponse, JsonResponse

logging.basicConfig(level=logging.INFO)


def health(request):
    return HttpResponse("Ready")


def success(request):
    # Deferred import: helpers must be imported after the AST hooks are active
    # so its functions get instrumented (see helpers.py module docstring).
    # pylint: disable-next=import-outside-toplevel
    from serviceevents_django.helpers import BusinessLogic, compute_result

    bl = BusinessLogic()
    result = bl.process("test_data")
    compute_result(42)
    return JsonResponse({"status": "ok", "result": result})


def error(request):
    return JsonResponse({"error": "bad request"}, status=400)


def fault(request):
    raise RuntimeError("Intentional server fault")


def exception_endpoint(request):
    # Deferred import (see success()): keep helpers import inside the view.
    # pylint: disable-next=import-outside-toplevel
    from serviceevents_django.helpers import validate_input

    validate_input(None)
