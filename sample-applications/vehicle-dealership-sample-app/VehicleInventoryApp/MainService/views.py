# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import os
import time

import requests
from django.http import HttpResponse, HttpResponseBadRequest, HttpResponseNotAllowed, HttpResponseNotFound
from django.views.decorators.csrf import csrf_exempt
from dotenv import load_dotenv
from MainService.models import Vehicle

load_dotenv()


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


def get_vehicle_by_id(request, vehicle_id):
    if request.method == "GET":
        throttle_time = request.GET.get("throttle")
        if throttle_time:
            print("going to throttle for " + throttle_time + " seconds")
            time.sleep(int(throttle_time))

        vehicle_objects = Vehicle.objects.filter(id=vehicle_id).values()
        if not vehicle_objects:
            return HttpResponseNotFound("Vehicle with id=" + str(vehicle_id) + " is not found")
        return HttpResponse(vehicle_objects)
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
def get_image_by_name(request, image_name):
    print(image_name)
    if request.method == "GET":
        response = requests.get(build_image_url(image_name), timeout=10);
        if response.ok:
            return HttpResponse(response)
        else:
            return HttpResponseNotFound("Image with name: " + image_name + " is not found")
    return HttpResponseNotAllowed("Only GET requests are allowed!")


def build_image_url(image_name):
    return get_image_endpoint() + "/images/name/" + image_name
