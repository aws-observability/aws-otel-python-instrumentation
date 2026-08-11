# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}

WORKDIR /cloudwatch-span-metrics

ARG PLUGIN
COPY ./dist/${PLUGIN} .
COPY ./contract-tests/images/applications/cloudwatch-span-metrics .

ENV PIP_ROOT_USER_ACTION=ignore
RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install "${PLUGIN}" --force-reinstall

CMD ["opentelemetry-instrument", "python", "-u", "./span_metrics_server.py"]
