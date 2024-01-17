import json
import logging
import os
import traceback

import requests
from django.http import HttpResponse, HttpResponseBadRequest, HttpResponseNotAllowed
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
            print(get_image_endpoint() + "/images/name/" + body["image_name"])
            requests.post(get_image_endpoint() + "/images/name/" + body["image_name"])
            return HttpResponse("VehicleId = " + str(vehicle_object.id))
        except KeyError as e:
            return HttpResponseBadRequest("Missing key: " + str(e))
        except Exception as e:
            logging.error(traceback.format_exc())
            logging.error(str(e))
    elif request.method == "GET":
        vehicle_objects = Vehicle.objects.all().values()
        return HttpResponse(vehicle_objects)
    else:
        return HttpResponseNotAllowed()


def get_vehicle_by_id(request, vehicle_id):
    if request.method == "GET":
        vehicle_objects = Vehicle.objects.filter(id=vehicle_id).values()
        return HttpResponse(vehicle_objects)
    else:
        return HttpResponseNotAllowed()


def get_vehicle_image(request, vehicle_id):
    if request.method == "GET":
        vehicle_object = Vehicle.objects.filter(id=vehicle_id).first()
        image_name = getattr(vehicle_object, "image_name")
        return HttpResponse(requests.get(get_image_endpoint() + "/images/name/" + image_name))
    else:
        return HttpResponseNotAllowed()
