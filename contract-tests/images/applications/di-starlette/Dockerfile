ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}
WORKDIR /di-starlette
COPY ./dist/$DISTRO /di-starlette
COPY ./contract-tests/images/applications/di-starlette /di-starlette

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO
RUN pip install --upgrade pip && pip install -r requirements.txt && pip install "${DISTRO}[debugger]" --force-reinstall
RUN opentelemetry-bootstrap -a install

CMD ["opentelemetry-instrument", "python", "-u", "./di_starlette_server.py"]
