ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}
WORKDIR /di-fastapi
COPY ./dist/aws_opentelemetry_*.whl /di-fastapi/
COPY ./contract-tests/images/applications/di-fastapi /di-fastapi

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO
RUN pip install --upgrade pip && pip install -r requirements.txt && pip install "${DISTRO}[debugger]" --find-links . --force-reinstall
RUN opentelemetry-bootstrap -a install

CMD ["opentelemetry-instrument", "python", "-u", "./di_fastapi_server.py"]
