from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse

def success(request):
    return HttpResponse("Success", status=200)

def fault(request):
    return HttpResponse("Server Error", status=500)

def error(request):
    print("hit", request)
    return HttpResponse("Bad Request", status=400)
