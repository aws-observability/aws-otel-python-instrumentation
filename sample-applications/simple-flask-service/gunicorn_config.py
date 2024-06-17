import os

workers = int(os.environ.get('GUNICORN_PROCESSES', '4'))

threads = int(os.environ.get('GUNICORN_THREADS', '6'))

# timeout = int(os.environ.get('GUNICORN_TIMEOUT', '120'))

address = os.environ.get('LISTEN_ADDRESS')

bind = os.environ.get('GUNICORN_BIND', address)

forwarded_allow_ips = '*'

secure_scheme_headers = { 'X-Forwarded-Proto': 'https' }