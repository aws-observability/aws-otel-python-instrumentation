ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}
WORKDIR /serviceevents-django-uwsgi
COPY ./dist/aws_opentelemetry_*.whl /serviceevents-django-uwsgi/
COPY ./contract-tests/images/applications/serviceevents-django-uwsgi /serviceevents-django-uwsgi

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO
RUN pip install --upgrade pip && pip install -r requirements.txt && pip install ${DISTRO} --find-links . --force-reinstall
RUN opentelemetry-bootstrap -a install

CMD ["opentelemetry-instrument", "uwsgi", \
     "--module", "serviceevents_django.wsgi:application", \
     "--http", "0.0.0.0:8080", \
     "--processes", "2", \
     "--lazy-apps", \
     "--enable-threads", \
     "--import", "opentelemetry.instrumentation.auto_instrumentation.sitecustomize", \
     "--master", \
     "--die-on-term"]
