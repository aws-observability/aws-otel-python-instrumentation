# Meant to be run from aws-otel-python-instrumentation/contract-tests.
# Assumes existence of dist/aws_opentelemetry_distro-<pkg_version>-py3-none-any.whl.
# Assumes filename of aws_opentelemetry_distro-<pkg_version>-py3-none-any.whl is passed in as "DISTRO" arg.
FROM python:3.10
WORKDIR /mcp
COPY ./dist/$DISTRO /mcp
COPY ./contract-tests/images/applications/mcp /mcp
COPY ./aws-opentelemetry-distro/src/amazon/opentelemetry/distro/mcpinstrumentor /mcp/mcpinstrumentor

ENV PIP_ROOT_USER_ACTION=ignore
ARG DISTRO

# Install your MCP instrumentor first (use -e for editable install)
RUN pip install --upgrade pip && pip install -e ./mcpinstrumentor/

# Then install other requirements and the main distro
RUN pip install -r requirements.txt && pip install ${DISTRO} --force-reinstall
RUN opentelemetry-bootstrap -a install

# Without `-u`, logs will be buffered and `wait_for_logs` will never return.
CMD ["opentelemetry-instrument", "python3", "-u", "./simple_client.py"]