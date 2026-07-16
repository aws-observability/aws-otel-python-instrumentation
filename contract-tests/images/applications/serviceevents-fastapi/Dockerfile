ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}
WORKDIR /serviceevents-fastapi
COPY ./dist/aws_opentelemetry_*.whl /serviceevents-fastapi/
COPY ./contract-tests/images/applications/serviceevents-fastapi /serviceevents-fastapi

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO
RUN pip install --upgrade pip && pip install -r requirements.txt && pip install ${DISTRO} --find-links . --force-reinstall
RUN opentelemetry-bootstrap -a install

CMD ["opentelemetry-instrument", "python", "-u", "./serviceevents_fastapi_server.py"]
