# Meant to be run from aws-otel-python-instrumentation/contract-tests.
# Assumes existence of dist/aws_opentelemetry_distro-<pkg_version>-py3-none-any.whl.
# Assumes filename of aws_opentelemetry_distro-<pkg_version>-py3-none-any.whl is passed in as "DISTRO" arg.
ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}
WORKDIR /openai_agents
COPY ./dist/$DISTRO /openai_agents
COPY ./contract-tests/images/applications/gen_ai/openai_agents /openai_agents
COPY ./contract-tests/images/applications/gen_ai/mock_llm.py /openai_agents

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO
RUN pip install --upgrade pip && pip install -r requirements.txt && pip install ${DISTRO} --force-reinstall

# Without `-u`, logs will be buffered and `wait_for_logs` will never return.
CMD ["opentelemetry-instrument", "python", "-u", "./openai_agents_server.py"]
