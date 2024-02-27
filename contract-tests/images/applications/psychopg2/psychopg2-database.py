import atexit
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Tuple

from requests import Response, request
from typing_extensions import override

import psycopg2

_PORT: int = 8080
_NETWORK_ALIAS: str = "backend"
_SUCCESS: str = "success"
_ERROR: str = "error"
_FAULT: str = "fault"


def prepare_database(db_host, db_user, db_pass, db_name):
    conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host=db_host)
    print("db connected")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
            )
        """)

    cur.execute("INSERT INTO test_table (name) VALUES (%s)", ("Alice",))
    cur.execute("INSERT INTO test_table (name) VALUES (%s)", ("Bob",))

    conn.commit()

    cur.close()
    conn.close()



class RequestHandler(BaseHTTPRequestHandler):

    @override
    # pylint: disable=invalid-name
    def do_GET(self):
        db_host = os.getenv('DB_HOST')
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASS')
        db_name = os.getenv('DB_NAME')
        self.handle_request("get", db_host, db_user, db_pass, db_name)

    # def handle_request(self, db_host, db_user, db_pass, db_name):
    #     conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host=db_host)
    #     if "success" in self.path:
    #         cur = conn.cursor()
    #         cur.execute("SELECT id, name FROM test_table")
    #         rows = cur.fetchall()
    #         cur.close()
    #         conn.close()
    #         if len(rows) == 2:
    #             print("sucess request triggered, responding")
    #             self.send_response_only(200, "success")
    #             self.end_headers()
    #         else:
    #             self.send_response_only(400, "failed")
    #             self.end_headers()
    #     elif "fault" in self.path:
    #         cur = conn.cursor()
    #         cur.execute("SELECT id, name FROM invalid_table")
    #         cur.close()
    #         conn.close()
    #         self.send_response_only(200, "success")
    #         self.end_headers()

    def handle_request(self, method: str, db_host, db_user, db_pass, db_name):
        status_code: int
        if self.in_path(_NETWORK_ALIAS):
            if self.in_path(_SUCCESS):
                status_code = 200
            elif self.in_path(_ERROR):
                status_code = 400
            elif self.in_path(_FAULT):
                status_code = 500
            else:
                status_code = 404
        else:
            url: str = f"http://{_NETWORK_ALIAS}:{_PORT}/{_NETWORK_ALIAS}{self.path}"
            response: Response = request(method, url, timeout=20)
            status_code = response.status_code
        print("received a " + method + " request")
        self.send_response_only(status_code)
        self.end_headers()

    def in_path(self, sub_path: str):
        return sub_path in self.path


def main() -> None:
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_name = os.getenv('DB_NAME')
    prepare_database(db_host, db_user, db_pass, db_name)
    server_address: Tuple[str, int] = ("0.0.0.0", _PORT)
    request_handler_class: type = RequestHandler
    requests_server: ThreadingHTTPServer = ThreadingHTTPServer(server_address, request_handler_class)
    atexit.register(requests_server.shutdown)
    server_thread: Thread = Thread(target=requests_server.serve_forever)
    server_thread.start()
    print("Psychopg2-Ready")
    server_thread.join()


if __name__ == "__main__":
    main()
