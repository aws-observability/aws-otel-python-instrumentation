# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from sys import argv

import requests

try:
    requested = requests.get("http://localhost:8082/server_request", params={"param": argv[0]}, timeout=120)
    assert requested.status_code == 200
except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
except requests.exceptions.Timeout as timeout_err:
    print(f"Timeout error occurred: {timeout_err}")
except requests.exceptions.RequestException as req_err:
    print(f"Request error occurred: {req_err}")
