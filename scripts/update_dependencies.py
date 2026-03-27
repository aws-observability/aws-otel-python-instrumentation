#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import re
import sys

# Dependencies that use the first version number (opentelemetry-python)
PYTHON_CORE_DEPS = [
    "opentelemetry-api",
    "opentelemetry-sdk",
    "opentelemetry-proto",
    "opentelemetry-exporter-otlp-proto-grpc",
    "opentelemetry-exporter-otlp-proto-http",
    "opentelemetry-propagator-b3",
    "opentelemetry-propagator-jaeger",
    "opentelemetry-exporter-otlp-proto-common",
]

# Dependencies that use the second version number (opentelemetry-python-contrib)
CONTRIB_DEPS = [
    "opentelemetry-distro",
    "opentelemetry-processor-baggage",
    "opentelemetry-propagator-ot-trace",
    "opentelemetry-test-utils",
    "opentelemetry-instrumentation",
    "opentelemetry-instrumentation-aws-lambda",
    "opentelemetry-instrumentation-aio-pika",
    "opentelemetry-instrumentation-aiohttp-client",
    "opentelemetry-instrumentation-aiokafka",
    "opentelemetry-instrumentation-aiopg",
    "opentelemetry-instrumentation-asgi",
    "opentelemetry-instrumentation-asyncpg",
    "opentelemetry-instrumentation-boto",
    "opentelemetry-instrumentation-boto3sqs",
    "opentelemetry-instrumentation-botocore",
    "opentelemetry-instrumentation-celery",
    "opentelemetry-instrumentation-confluent-kafka",
    "opentelemetry-instrumentation-dbapi",
    "opentelemetry-instrumentation-django",
    "opentelemetry-instrumentation-elasticsearch",
    "opentelemetry-instrumentation-falcon",
    "opentelemetry-instrumentation-fastapi",
    "opentelemetry-instrumentation-flask",
    "opentelemetry-instrumentation-grpc",
    "opentelemetry-instrumentation-httpx",
    "opentelemetry-instrumentation-jinja2",
    "opentelemetry-instrumentation-kafka-python",
    "opentelemetry-instrumentation-logging",
    "opentelemetry-instrumentation-mysql",
    "opentelemetry-instrumentation-mysqlclient",
    "opentelemetry-instrumentation-pika",
    "opentelemetry-instrumentation-psycopg2",
    "opentelemetry-instrumentation-pymemcache",
    "opentelemetry-instrumentation-pymongo",
    "opentelemetry-instrumentation-pymysql",
    "opentelemetry-instrumentation-pyramid",
    "opentelemetry-instrumentation-redis",
    "opentelemetry-instrumentation-remoulade",
    "opentelemetry-instrumentation-requests",
    "opentelemetry-instrumentation-sqlalchemy",
    "opentelemetry-instrumentation-sqlite3",
    "opentelemetry-instrumentation-starlette",
    "opentelemetry-instrumentation-system-metrics",
    "opentelemetry-instrumentation-tornado",
    "opentelemetry-instrumentation-tortoiseorm",
    "opentelemetry-instrumentation-urllib",
    "opentelemetry-instrumentation-urllib3",
    "opentelemetry-instrumentation-wsgi",
    "opentelemetry-instrumentation-cassandra",
]

# AWS-specific packages with independent versioning
AWS_DEPS = [
    "opentelemetry-sdk-extension-aws",
    "opentelemetry-propagator-aws-xray",
]


def _replace_dep_versions(content, deps, version):
    """Replace version pins for a list of dependencies in content."""
    updated = False
    for dep in deps:
        pattern = rf'"?{re.escape(dep)}\s*==\s*[^\s,\]"]*"?'
        replacement = f'"{dep} == {version}"' if '"' in content else f"{dep}=={version}"
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            updated = True
    return content, updated


def _replace_tox_repo_branches(content, otel_python_version, otel_contrib_version):
    """Replace tox CORE_REPO/CONTRIB_REPO branch references."""
    core_match = re.match(r"(\d+\.\d+)", otel_python_version)
    contrib_match = re.match(r"(\d+\.\d+b?)", otel_contrib_version)
    if not (core_match and contrib_match):
        return content, False
    new_branch = f"release/v{core_match.group(1)}.x-{contrib_match.group(1)}x"
    pattern = (
        r"((?:CORE|CONTRIB)_REPO=git\+https://github\.com/open-telemetry/"
        r"opentelemetry-python(?:-contrib)?\.git@)release/v[\d.]+x-[\d.b]+x"
    )
    content, count = re.subn(pattern, rf"\g<1>{new_branch}", content)
    return content, bool(count)


def update_file_dependencies(file_path, otel_python_version, otel_contrib_version, aws_versions):
    """Update all Otel dependencies in a given file"""
    try:
        with open(file_path, "r", encoding="utf-8") as input_file:
            content = input_file.read()

        updated = False

        content, changed = _replace_dep_versions(content, PYTHON_CORE_DEPS, otel_python_version)
        updated = updated or changed

        content, changed = _replace_dep_versions(content, CONTRIB_DEPS, otel_contrib_version)
        updated = updated or changed

        for dep, version in aws_versions.items():
            if version:
                content, changed = _replace_dep_versions(content, [dep], version)
                updated = updated or changed

        content, changed = _replace_tox_repo_branches(content, otel_python_version, otel_contrib_version)
        updated = updated or changed

        if updated:
            with open(file_path, "w", encoding="utf-8") as output_file:
                output_file.write(content)
            print(f"Updated {file_path}")

        return updated
    except (OSError, IOError) as file_error:
        print(f"Error updating {file_path}: {file_error}")
        return False


def main():
    otel_python_version = os.environ.get("OTEL_PYTHON_VERSION")
    otel_contrib_version = os.environ.get("OTEL_CONTRIB_VERSION")
    aws_sdk_ext_version = os.environ.get("OPENTELEMETRY_SDK_EXTENSION_AWS_VERSION")
    aws_xray_prop_version = os.environ.get("OPENTELEMETRY_PROPAGATOR_AWS_XRAY_VERSION")

    if not otel_python_version or not otel_contrib_version:
        print("Error: OTEL_PYTHON_VERSION and OTEL_CONTRIB_VERSION environment variables required")
        sys.exit(1)

    if not aws_sdk_ext_version or not aws_xray_prop_version:
        print("Error: AWS dependency versions required")
        sys.exit(1)

    aws_versions = {
        "opentelemetry-sdk-extension-aws": aws_sdk_ext_version,
        "opentelemetry-propagator-aws-xray": aws_xray_prop_version,
    }

    # All files to update
    files_to_update = [
        "aws-opentelemetry-distro/pyproject.toml",
        "contract-tests/images/mock-collector/pyproject.toml",
        "contract-tests/images/mock-collector/requirements.txt",
        "contract-tests/tests/pyproject.toml",
        "lambda-layer/src/tests/requirements.txt",
        "tox.ini",
    ]

    any_updated = False
    for file_path in files_to_update:
        if update_file_dependencies(file_path, otel_python_version, otel_contrib_version, aws_versions):
            any_updated = True

    if any_updated:
        print(f"Dependencies updated to Python {otel_python_version} / Contrib {otel_contrib_version}")
    else:
        print("No OpenTelemetry dependencies found to update")


if __name__ == "__main__":
    main()
