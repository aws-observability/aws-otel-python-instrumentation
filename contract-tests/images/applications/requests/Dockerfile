FROM python:3.9-slim
WORKDIR /requests
COPY . /requests

RUN pip install -r requirements.txt && pip install aws_opentelemetry_distro-0.0.1-py3-none-any.whl --force-reinstall
RUN opentelemetry-bootstrap -a install

# Without `-u`, logs will be buffered and `wait_for_logs` will never return.
CMD ["opentelemetry-instrument", "python", "-u", "./requests_server.py"]