import atexit
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Tuple
from typing_extensions import override

import psycopg2

_PORT: int = 8080
_DBNAME = 'testdb'
_USER = 'user'
_PASSWORD = 'password'
_HOST = 'localhost'


def prepare_database() -> None:
    conn = psycopg2.connect(dbname=_DBNAME, user=_USER, password=_PASSWORD, host=_HOST)

    print("db connected")

    cur = conn.cursor()

    cur.execute("""
        CREATE TEMPORARY TABLE test_table (
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
        self.handle_request()

    def handle_request(self):
        conn = psycopg2.connect(dbname=_DBNAME, user=_USER, password=_PASSWORD, host=_HOST)
        if "success" in self.path:
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM test_table")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            if len(rows) == 2:
                self.send_response(200, "success")
            else:
                self.send_response(400, "failed")
        elif "fault" in self.path:
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM invalid_table")
            cur.close()
            conn.close()
            self.send_response(200, "success")


def main() -> None:
    prepare_database()
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
