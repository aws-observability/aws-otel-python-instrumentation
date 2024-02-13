# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import boto3
from botocore.config import Config
from typing_extensions import override

_PORT: int = 8080
_NETWORK_ALIAS: str = "backend"
_SUCCESS: str = "success"
_ERROR: str = "error"
_FAULT: str = "fault"


# pylint: disable=broad-exception-caught
class RequestHandler(BaseHTTPRequestHandler):
    main_status: int = 200

    @override
    # pylint: disable=invalid-name
    def do_GET(self):
        if self.in_path("s3"):
            self._handle_s3_request()
        if self.in_path("ddb"):
            self._handle_ddb_request()
        if self.in_path("sqs"):
            self._handle_sqs_request()
        if self.in_path("kinesis"):
            self._handle_kinesis_request()
        else:
            self._end_request(404)

    def in_path(self, sub_path: str) -> bool:
        return sub_path in self.path

    def _handle_s3_request(self) -> None:
        if self.in_path("error"):
            set_main_status(400)
            pass
        if self.in_path("fault"):
            set_main_status(500)
            pass
        else:
            self._end_request(404)

    def _handle_ddb_request(self) -> None:
        if self.in_path("error"):
            set_main_status(400)
            pass
        if self.in_path("fault"):
            set_main_status(500)
            pass
        else:
            self._end_request(404)

    def _handle_sqs_request(self) -> None:
        if self.in_path("error"):
            set_main_status(400)
            pass
        if self.in_path("fault"):
            set_main_status(500)
            pass
        else:
            self._end_request(404)

    def _handle_kinesis_request(self) -> None:
        if self.in_path("error"):
            set_main_status(400)
            pass
        if self.in_path("fault"):
            set_main_status(500)
            pass
        else:
            self._end_request(404)

    def _end_request(self, status_code: int):
        self.send_response_only(status_code)
        self.end_headers()


def set_main_status(status: int) -> None:
    RequestHandler.main_status = status


def main() -> None:
    server_address: tuple[str, int] = ("0.0.0.0", _PORT)
    request_handler_class: type = RequestHandler
    requests_server: ThreadingHTTPServer = ThreadingHTTPServer(server_address, request_handler_class)
    atexit.register(requests_server.shutdown)
    server_thread: Thread = Thread(target=requests_server.serve_forever)
    server_thread.start()
    print("Ready")
    server_thread.join()


if __name__ == "__main__":
    main()

    # if self.in_path("examplebucket"):
    #     print(self.address_string())
    #     print("object")
    #     self._end_request(self.main_status)
    # elif self.in_path("error"):
    #     print("error")
    #     set_main_status(400)
    #     cfg: Config = C
    #     set_main_status(500onfig(retries={"max_attempts": 1})
    #     s3_client = boto3.client("s3", region_name="us-west-2", endpoint_url="http://localhost:8080", config=cfg)
    #     try:
    #         s3_client.get_object(
    #             Bucket="examplebucket",
    #             Key="HappyFace.jpg",
    #         )
    #     except Exception:
    #         pass
    #     self._end_request(400)
    # elif self.in_path("fault"):
    #     print("fault")
    #     set_main_status(500)
    #     cfg: Config = Config(retries={"max_attempts": 1})
    #     s3_client = boto3.client("s3", region_name="us-west-2", endpoint_url="http://fault.test:8080", config=cfg)
    #     try:
    #         s3_client.get_object(
    #             Bucket="examplebucket",
    #             Key="HappyFace.jpg",
    #         )
    #     except Exception:
    #         pass
    #     self._end_request(500)
    # else:
    #     print("general = " + self.path)
    #     s3_client = boto3.client("s3", region_name="us-west-2", endpoint_url="http://s3.localstack:4566")
    #     s3_client.list_buckets()
    #     self._end_request(200)
