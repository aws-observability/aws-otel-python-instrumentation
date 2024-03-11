# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods


def success(request):
    return HttpResponse("Success", status=200)


def fault(request):
    return HttpResponse("Server Error", status=500)


def error(request):
    return HttpResponse("Bad Request", status=400)


def user_order(request, userId, orderId):
    request.GET.get("filter", None)
    return HttpResponse("Routed", status=200)


@csrf_exempt
@require_http_methods(["POST"])
def post_success(request):
    return HttpResponse("Create Success", status=201)
