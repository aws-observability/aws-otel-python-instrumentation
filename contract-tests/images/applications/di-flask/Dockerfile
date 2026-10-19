ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}
WORKDIR /di-flask
COPY ./dist/$DISTRO /di-flask
COPY ./contract-tests/images/applications/di-flask /di-flask

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO
RUN pip install --upgrade pip && pip install -r requirements.txt && pip install "${DISTRO}[debugger]" --force-reinstall
RUN opentelemetry-bootstrap -a install

CMD ["opentelemetry-instrument", "python", "-u", "./di_flask_server.py"]
