#!/usr/bin/env python3

import sys
import os
import re
import requests

# Dependencies that use the first version number (opentelemetry-python)
PYTHON_CORE_DEPS = [
    'opentelemetry-api',
    'opentelemetry-sdk',
    'opentelemetry-exporter-otlp',
    'opentelemetry-exporter-otlp-proto-grpc',
    'opentelemetry-exporter-otlp-proto-http',
    'opentelemetry-propagator-b3',
    'opentelemetry-propagator-jaeger',
    'opentelemetry-exporter-otlp-proto-common',
]

# Dependencies that use the second version number (opentelemetry-python-contrib)
CONTRIB_DEPS = [
    'opentelemetry-distro',
    'opentelemetry-processor-baggage',
    'opentelemetry-propagator-ot-trace',
    'opentelemetry-instrumentation',
    'opentelemetry-instrumentation-aws-lambda',
    'opentelemetry-instrumentation-aio-pika',
    'opentelemetry-instrumentation-aiohttp-client',
    'opentelemetry-instrumentation-aiopg',
    'opentelemetry-instrumentation-asgi',
    'opentelemetry-instrumentation-asyncpg',
    'opentelemetry-instrumentation-boto',
    'opentelemetry-instrumentation-boto3sqs',
    'opentelemetry-instrumentation-botocore',
    'opentelemetry-instrumentation-celery',
    'opentelemetry-instrumentation-confluent-kafka',
    'opentelemetry-instrumentation-dbapi',
    'opentelemetry-instrumentation-django',
    'opentelemetry-instrumentation-elasticsearch',
    'opentelemetry-instrumentation-falcon',
    'opentelemetry-instrumentation-fastapi',
    'opentelemetry-instrumentation-flask',
    'opentelemetry-instrumentation-grpc',
    'opentelemetry-instrumentation-httpx',
    'opentelemetry-instrumentation-jinja2',
    'opentelemetry-instrumentation-kafka-python',
    'opentelemetry-instrumentation-logging',
    'opentelemetry-instrumentation-mysql',
    'opentelemetry-instrumentation-mysqlclient',
    'opentelemetry-instrumentation-pika',
    'opentelemetry-instrumentation-psycopg2',
    'opentelemetry-instrumentation-pymemcache',
    'opentelemetry-instrumentation-pymongo',
    'opentelemetry-instrumentation-pymysql',
    'opentelemetry-instrumentation-pyramid',
    'opentelemetry-instrumentation-redis',
    'opentelemetry-instrumentation-remoulade',
    'opentelemetry-instrumentation-requests',
    'opentelemetry-instrumentation-sqlalchemy',
    'opentelemetry-instrumentation-sqlite3',
    'opentelemetry-instrumentation-starlette',
    'opentelemetry-instrumentation-system-metrics',
    'opentelemetry-instrumentation-tornado',
    'opentelemetry-instrumentation-tortoiseorm',
    'opentelemetry-instrumentation-urllib',
    'opentelemetry-instrumentation-urllib3',
    'opentelemetry-instrumentation-wsgi',
    'opentelemetry-instrumentation-cassandra',
]

# AWS-specific packages with independent versioning
AWS_DEPS = [
    'opentelemetry-sdk-extension-aws',
    'opentelemetry-propagator-aws-xray',
]

def get_latest_version(package_name):
    """Get the latest version of a package from PyPI."""
    try:
        response = requests.get(f'https://pypi.org/pypi/{package_name}/json', timeout=30)
        response.raise_for_status()
        data = response.json()
        return data['info']['version']
    except requests.RequestException as request_error:
        print(f"Warning: Could not get latest version for {package_name}: {request_error}")
        return None

def main():
    otel_python_version = os.environ.get('OTEL_PYTHON_VERSION')
    otel_contrib_version = os.environ.get('OTEL_CONTRIB_VERSION')
    
    if not otel_python_version or not otel_contrib_version:
        print("Error: OTEL_PYTHON_VERSION and OTEL_CONTRIB_VERSION environment variables required")
        sys.exit(1)
    
    pyproject_path = 'aws-opentelemetry-distro/pyproject.toml'
    
    try:
        with open(pyproject_path, 'r', encoding='utf-8') as input_file:
            content = input_file.read()
        
        updated = False
        
        # Update opentelemetry-python dependencies
        for dep in PYTHON_CORE_DEPS:
            pattern = rf'"{re.escape(dep)} == [^"]*"'
            replacement = f'"{dep} == {otel_python_version}"'
            if re.search(pattern, content):
                content = re.sub(pattern, replacement, content)
                updated = True
        
        # Update opentelemetry-python-contrib dependencies  
        for dep in CONTRIB_DEPS:
            pattern = rf'"{re.escape(dep)} == [^"]*"'
            replacement = f'"{dep} == {otel_contrib_version}"'
            if re.search(pattern, content):
                content = re.sub(pattern, replacement, content)
                updated = True
        
        # Update dependencies with independent versioning
        for dep in AWS_DEPS:
            latest_version = get_latest_version(dep)
            if latest_version:
                pattern = rf'"{re.escape(dep)} == [^"]*"'
                replacement = f'"{dep} == {latest_version}"'
                if re.search(pattern, content):
                    content = re.sub(pattern, replacement, content)
                    updated = True
                    print(f"Updated {dep} to {latest_version}")
        
        if updated:
            with open(pyproject_path, 'w', encoding='utf-8') as output_file:
                output_file.write(content)
            print(f'Dependencies updated to Python {otel_python_version} / Contrib {otel_contrib_version}')
        else:
            print('No OpenTelemetry dependencies found to update')
            
    except (OSError, IOError) as file_error:
        print(f"Error updating dependencies: {file_error}")
        sys.exit(1)

if __name__ == '__main__':
    main()
