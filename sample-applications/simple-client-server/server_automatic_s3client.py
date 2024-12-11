# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import boto3
import json
from flask import Flask

client = boto3.client(service_name="bedrock-runtime")
app = Flask(__name__)

@app.route("/server_request")
def server_request():
    messages = [
    {"role": "user", "content": [{"text": "Write a short poem"}]},
 ]

    model_response = client.converse(
        modelId="us.amazon.nova-lite-v1:0", 
        messages=messages
    )

    print("\n[Full Response]")
    print(json.dumps(model_response, indent=2))

    print("\n[Response Content Text]")
    print(model_response["output"]["message"]["content"][0]["text"])
    


if __name__ == "__main__":
    app.run(port=8082)
