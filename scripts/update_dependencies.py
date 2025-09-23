#!/usr/bin/env python3

import sys
import os
import re

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

def main():
    otel_python_version = os.environ.get('OTEL_PYTHON_VERSION')
    otel_contrib_version = os.environ.get('OTEL_CONTRIB_VERSION')
    
    if not otel_python_version or not otel_contrib_version:
        print("Error: OTEL_PYTHON_VERSION and OTEL_CONTRIB_VERSION environment variables required")
        sys.exit(1)
    
    pyproject_path = 'aws-opentelemetry-distro/pyproject.toml'
    
    try:
        with open(pyproject_path, 'r') as f:
            content = f.read()
        
        updated = False
        
        # Update Python core dependencies
        for dep in PYTHON_CORE_DEPS:
            pattern = rf'"{re.escape(dep)} == [^"]*"'
            replacement = f'"{dep} == {otel_python_version}"'
            if re.search(pattern, content):
                content = re.sub(pattern, replacement, content)
                updated = True
        
        # Update contrib dependencies  
        for dep in CONTRIB_DEPS:
            pattern = rf'"{re.escape(dep)} == [^"]*"'
            replacement = f'"{dep} == {otel_contrib_version}"'
            if re.search(pattern, content):
                content = re.sub(pattern, replacement, content)
                updated = True
        
        if updated:
            with open(pyproject_path, 'w') as f:
                f.write(content)
            print(f'Dependencies updated to Python {otel_python_version} / Contrib {otel_contrib_version}')
        else:
            print('No OpenTelemetry dependencies found to update')
            
    except Exception as e:
        print(f"Error updating dependencies: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
