import json
import os

import boto3
import requests

client = boto3.client("s3")


# lambda function
def lambda_handler(event, context):

    requests.get("https://aws.amazon.com/")

    client.list_buckets()

    return {"statusCode": 200, "body": json.dumps(os.environ.get("_X_AMZN_TRACE_ID"))}
