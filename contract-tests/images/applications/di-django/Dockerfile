# Meant to be run from aws-otel-python-instrumentation/contract-tests.
# Assumes existence of dist/aws_opentelemetry_distro-<pkg_version>-py3-none-any.whl.
# Assumes filename of aws_opentelemetry_distro-<pkg_version>-py3-none-any.whl is passed in as "DISTRO" arg.
ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}
WORKDIR /di-django
COPY ./dist/$DISTRO /di-django
COPY ./contract-tests/images/applications/di-django /di-django

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO
RUN pip install --upgrade pip && pip install -r requirements.txt && pip install "${DISTRO}[debugger]" --force-reinstall
RUN opentelemetry-bootstrap -a install

# Without `-u`, logs will be buffered and `wait_for_logs` will never return.
CMD ["opentelemetry-instrument", "python", "-u", "./manage.py", "runserver", "0.0.0.0:8080", "--noreload"]
