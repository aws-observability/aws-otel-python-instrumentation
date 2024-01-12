from threading import Thread
from time import sleep

import boto3
import requests
from django.http import HttpResponse, HttpResponseNotAllowed
from django.views.decorators.csrf import csrf_exempt

s3_client = boto3.client("s3")
sqs = boto3.resource("sqs", region_name="us-west-2")


def read_from_queue():
    while True:
        queue = sqs.get_queue_by_name(QueueName="imageQueue.fifo")
        for message in queue.receive_messages():
            image_name = message.body
            print("receiving " + image_name + " from the queue")
            s3_client.put_object(Bucket="image-service-test", Key=image_name)
            message.delete()
        sleep(10)


thread = Thread(target=read_from_queue)
thread.start()


def index(request):
    # return all images in s3?
    return HttpResponse("Hello, world LOL!")


@csrf_exempt
def handle_image(request, image_name):
    print(image_name)
    if request.method == "POST":
        put_image(image_name)
        return HttpResponse("Putting to queue")
    elif request.method == "GET":
        return HttpResponse(get_image(image_name))
    else:
        return HttpResponseNotAllowed()


def get_image(image_name):
    s3_object = s3_client.get_object(Bucket="image-service-test", Key=image_name)
    return str(s3_object)


def get_remote_image(request):
    api_url = "https://google.com"
    return HttpResponse(requests.get(api_url))


def put_image(image_name):
    queue = sqs.get_queue_by_name(QueueName="imageQueue.fifo")
    queue.send_message(MessageBody=image_name, MessageGroupId="1")
    print("adding " + image_name + " to the queue")
    return HttpResponse("added to the queue")
