# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import atexit
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Tuple

import psycopg2
from typing_extensions import override

_PORT: int = 8080
_SUCCESS: str = "success"
_ERROR: str = "error"
_FAULT: str = "fault"

_DB_HOST = os.getenv("DB_HOST")
_DB_USER = os.getenv("DB_USER")
_DB_PASS = os.getenv("DB_PASS")
_DB_NAME = os.getenv("DB_NAME")


class RequestHandler(BaseHTTPRequestHandler):
    @override
    # pylint: disable=invalid-name
    def do_GET(self):
        status_code: int = 200
        conn = psycopg2.connect(dbname=_DB_NAME, user=_DB_USER, password=_DB_PASS, host=_DB_HOST)
        if self.in_path(_SUCCESS):
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS test_table")
            cur.close()
            status_code = 200
        elif self.in_path(_FAULT):
            cur = conn.cursor()
            try:
                cur.execute("SELECT DISTINCT id, name FROM invalid_table")
            except psycopg2.ProgrammingError as exception:
                print("Expected Exception with Invalid SQL occurred:", exception)
                status_code = 500
            except Exception as exception:  # pylint: disable=broad-except
                print("Exception Occurred:", exception)
            else:
                status_code = 200
            finally:
                cur.close()
        else:
            status_code = 404
        conn.close()
        self.send_response_only(status_code)
        self.end_headers()

    def in_path(self, sub_path: str):
        return sub_path in self.path


def main() -> None:
    server_address: Tuple[str, int] = ("0.0.0.0", _PORT)
    request_handler_class: type = RequestHandler
    requests_server: ThreadingHTTPServer = ThreadingHTTPServer(server_address, request_handler_class)
    atexit.register(requests_server.shutdown)
    server_thread: Thread = Thread(target=requests_server.serve_forever)
    server_thread.start()
    print("Ready")
    server_thread.join()


if __name__ == "__main__":
    main()
