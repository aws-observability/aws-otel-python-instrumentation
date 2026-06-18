ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}
WORKDIR /serviceevents-flask
COPY ./dist/$DISTRO /serviceevents-flask
COPY ./contract-tests/images/applications/serviceevents-flask /serviceevents-flask

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO
RUN pip install --upgrade pip && pip install -r requirements.txt && pip install ${DISTRO} --force-reinstall
RUN opentelemetry-bootstrap -a install

CMD ["opentelemetry-instrument", "python", "-u", "./serviceevents_flask_server.py"]
