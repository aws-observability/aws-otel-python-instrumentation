# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import boto3
from flask import Flask, request

# Let's use Amazon S3
s3 = boto3.resource("s3")

app = Flask(__name__)


@app.route("/server_request")
def server_request():
    print(request.args.get("param"))
    for bucket in s3.buckets.all():
        print(bucket.name)
    return "served"


if __name__ == "__main__":
    app.run(port=8082)