FROM python:3.10
WORKDIR /serviceevents-django
COPY ./dist/$DISTRO /serviceevents-django
COPY ./contract-tests/images/applications/serviceevents-django /serviceevents-django

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO
RUN pip install --upgrade pip && pip install -r requirements.txt && pip install ${DISTRO} --force-reinstall
RUN opentelemetry-bootstrap -a install

CMD ["opentelemetry-instrument", "python", "-u", "./manage.py", "runserver", "0.0.0.0:8080", "--noreload"]
