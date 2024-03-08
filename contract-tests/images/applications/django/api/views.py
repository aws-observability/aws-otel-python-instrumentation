from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse


def success(request):
    return HttpResponse("Success", status=200)


def fault(request):
    return HttpResponse("Server Error", status=500)


def error(request):
    return HttpResponse("Bad Request", status=400)


def user_order(request, userId, orderId):
    request.GET.get('filter', None)
    return HttpResponse("Routed", status=200)
