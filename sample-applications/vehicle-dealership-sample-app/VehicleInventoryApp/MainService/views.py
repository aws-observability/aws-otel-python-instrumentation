# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import os
import time

import requests
from django.http import HttpResponse, HttpResponseBadRequest, HttpResponseNotAllowed, HttpResponseNotFound
from django.views.decorators.csrf import csrf_exempt
from dotenv import load_dotenv
from MainService.models import Vehicle, VehiclePurchaseHistory

load_dotenv()


def health_check(request):
    return HttpResponse("Vehicle Inventory Service up and running!")


def get_image_endpoint():
    load_dotenv()
    return "http://" + os.environ.get("IMAGE_BACKEND_SERVICE_HOST") + ":" + os.environ.get("IMAGE_BACKEND_SERVICE_PORT")


@csrf_exempt
def vehicle(request):
    if request.method == "POST":
        body_unicode = request.body.decode("utf-8")
        body = json.loads(body_unicode)
        try:
            vehicle_object = Vehicle(
                make=body["make"], model=body["model"], year=body["year"], image_name=body["image_name"]
            )
            vehicle_object.save()
            requests.post(build_image_url(body["image_name"]), timeout=10)
            return HttpResponse("VehicleId = " + str(vehicle_object.id))
        except KeyError as exception:
            return HttpResponseBadRequest("Missing key: " + str(exception))
    elif request.method == "GET":
        vehicle_objects = Vehicle.objects.all().values()
        return HttpResponse(vehicle_objects)
    return HttpResponseNotAllowed("Only GET/POST requests are allowed!")


@csrf_exempt
def vehicle_by_id(request, vehicle_id):
    if request.method == "GET":
        throttle_time = request.GET.get("throttle")
        if throttle_time:
            print("going to throttle for " + throttle_time + " seconds")
            time.sleep(int(throttle_time))

        vehicle_objects = Vehicle.objects.filter(id=vehicle_id).values()
        if not vehicle_objects:
            return HttpResponseNotFound("Vehicle with id=" + str(vehicle_id) + " is not found")
        return HttpResponse(vehicle_objects)
    if request.method == "DELETE":
        vehicle_objects = Vehicle.objects.filter(id=vehicle_id)
        vehicle_objects_values = Vehicle.objects.filter(id=vehicle_id).values()
        if not vehicle_objects_values:
            return HttpResponseNotFound("Vehicle with id=" + str(vehicle_id) + " is not found")
        vehicle_objects.delete()
        return HttpResponse("Vehicle with id=" + str(vehicle_id) + " has been deleted")
    return HttpResponseNotAllowed("Only GET/DELETE requests are allowed!")


def get_vehicles_by_make(request, vehicles_make):
    if request.method == "GET":
        vehicles_objects = Vehicle.objects.filter(make=vehicles_make).values()
        if not vehicles_objects:
            return HttpResponseNotFound("Couldn't find any vehicle with make=" + str(vehicles_make))
        return HttpResponse(vehicles_objects)
    return HttpResponseNotAllowed("Only GET requests are allowed!")


def get_vehicle_image(request, vehicle_id):
    if request.method == "GET":
        vehicle_object = Vehicle.objects.filter(id=vehicle_id).first()
        if not vehicle_object:
            return HttpResponseNotFound("Vehicle with id=" + str(vehicle_id) + " is not found")
        image_name = getattr(vehicle_object, "image_name")
        return HttpResponse(requests.get(build_image_url(image_name), timeout=10))
    return HttpResponseNotAllowed("Only GET requests are allowed!")


@csrf_exempt
def image(request, image_name):
    print(image_name)
    if request.method == "GET":
        response = requests.get(build_image_url(image_name), timeout=10)
        if response.ok:
            return HttpResponse(response)
        return HttpResponseNotFound("Image with name: " + image_name + " is not found")
    if request.method == "POST":
        response = requests.post(build_image_url(image_name), timeout=10)
        if response.ok:
            return HttpResponse(response)
        return HttpResponseNotFound("Image with name: " + image_name + " failed to saved")
    return HttpResponseNotAllowed("Only GET/POST requests are allowed!")


@csrf_exempt
def vehicle_purchase_history(request):
    if request.method == "POST":
        body_unicode = request.body.decode("utf-8")
        body = json.loads(body_unicode)
        try:
            vehicle_purchase_history_object = VehiclePurchaseHistory(
                vehicle_id=body["vehicle_id"], purchase_price=body["purchase_price"]
            )
            vehicle_purchase_history_object.save()
            return HttpResponse("VehiclePurchaseHistoryId = " + str(vehicle_purchase_history_object.id))
        except KeyError as exception:
            return HttpResponseBadRequest("Missing key: " + str(exception))
    elif request.method == "GET":
        vehicle_purchase_history_object = VehiclePurchaseHistory.objects.all().values()
        return HttpResponse(vehicle_purchase_history_object)
    return HttpResponseNotAllowed("Only GET/POST requests are allowed!")


@csrf_exempt
def vehicle_purchase_history_by_id(request, vehicle_purchase_history_id):
    if request.method == "GET":
        vehicle_purchase_history_object = VehiclePurchaseHistory.objects.filter(id=vehicle_purchase_history_id).values()
        if not vehicle_purchase_history_object:
            return HttpResponseNotFound(
                "VehiclePurchaseHistory with id=" + str(vehicle_purchase_history_id) + " is not found"
            )
        return HttpResponse(vehicle_purchase_history_object)
    if request.method == "DELETE":
        vehicle_purchase_history_object = VehiclePurchaseHistory.objects.filter(id=vehicle_purchase_history_id)
        vehicle_purchase_history_object_values = Vehicle.objects.filter(id=vehicle_purchase_history_id).values()
        if not vehicle_purchase_history_object_values:
            return HttpResponseNotFound(
                "VehiclePurchaseHistory with id=" + str(vehicle_purchase_history_id) + " is not found"
            )
        vehicle_purchase_history_object.delete()
        return HttpResponse("VehiclePurchaseHistory with id=" + str(vehicle_purchase_history_id) + " has been deleted")
    return HttpResponseNotAllowed("Only GET/DELETE requests are allowed!")


def get_vehicle_from_vehicle_history(request, vehicle_purchase_history_id):
    if request.method == "GET":
        vehicle_purchase_history_object = VehiclePurchaseHistory.objects.filter(id=vehicle_purchase_history_id).first()
        if not vehicle_purchase_history_object:
            return HttpResponseNotFound(
                "VehiclePurchaseHistory with id=" + str(vehicle_purchase_history_id) + " is not found"
            )
        vehicle_id = getattr(vehicle_purchase_history_object, "vehicle")
        vehicle_objects = Vehicle.objects.filter(id=vehicle_id).values()
        if not vehicle_objects:
            return HttpResponseNotFound("Vehicle with id=" + str(vehicle_id) + " is not found")
        return HttpResponse(vehicle_objects)
    return HttpResponseNotAllowed("Only GET requests are allowed!")


def build_image_url(image_name):
    return get_image_endpoint() + "/images/name/" + image_name
