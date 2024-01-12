import json

import requests
from django.http import HttpResponse, HttpResponseBadRequest, HttpResponseNotAllowed, HttpResponseRedirect
from django.views.decorators.csrf import csrf_exempt
from MainService.models import Vehicle


@csrf_exempt
def vehicle(request):
    if request.method == "POST":
        body_unicode = request.body.decode("utf-8")
        body = json.loads(body_unicode)
        try:
            vehicle_object = Vehicle(
                make=body["make"], model=body["model"], year=body["year"], imageName=body["imageName"]
            )
            vehicle_object.save()
            requests.post("http://0.0.0.0:8000/images/name/" + body["imageName"])
            return HttpResponse("VehicleId = " + str(vehicle_object.id))
        except KeyError as e:
            return HttpResponseBadRequest("Missing key: " + str(e))
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
        image_name = getattr(vehicle_object, "imageName")
        return HttpResponse(requests.get("http://0.0.0.0:8000/images/name/" + image_name))
    else:
        return HttpResponseNotAllowed()


def redirect(request, **kwargs):
    print(request.path)
    return HttpResponseRedirect("http://0.0.0.0:8000" + request.path)


# add api to call other microservice

# Do we need api gateway? could just route from vehicle to image service.
