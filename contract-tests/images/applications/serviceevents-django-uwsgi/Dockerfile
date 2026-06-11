FROM python:3.10
WORKDIR /serviceevents-django-uwsgi
COPY ./dist/$DISTRO /serviceevents-django-uwsgi
COPY ./contract-tests/images/applications/serviceevents-django-uwsgi /serviceevents-django-uwsgi

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO
RUN pip install --upgrade pip && pip install -r requirements.txt && pip install ${DISTRO} --force-reinstall
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
