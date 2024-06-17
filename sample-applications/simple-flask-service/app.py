import logging
import os
from flask import Flask
from requests import sessions

application = app = Flask(__name__)

# tying the logger with gunicorn logging
gunicorn_logger = logging.getLogger('gunicorn.error')
app.logger.handlers = gunicorn_logger.handlers
logging.getLogger().setLevel(logging.INFO)
session = sessions.Session()

@app.route('/health-check')
def call_health_check():
    return 200


@app.route('/dep')
def call_dep():
    session.request("GET", "http://simple-service:8081/health-check")
    return 200


if __name__ == "__main__":
    address = os.environ.get('LISTEN_ADDRESS')
    if address is None:
        host = '127.0.0.1'
        port = '5000'
    else:
        host, port = address.split(":")
    app.run(host=host, port=int(port), debug=False)
