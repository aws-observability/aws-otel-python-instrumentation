import json
import logging
import os

import boto3
import requests

from opentelemetry._logs import LogRecord, SeverityNumber, get_logger

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
otel_logger = get_logger(__name__)

client = boto3.client("s3")


# lambda function
def lambda_handler(event, context):
    logger.debug("debug-level-test-message")
    logger.info("info-level-test-message")
    logger.warning("warn-level-test-message")
    logger.error("error-level-test-message")

    # Test all attribute data types via OTel Logs API directly
    otel_logger.emit(
        LogRecord(
            body="type-test-message",
            severity_number=SeverityNumber.INFO,
            attributes={
                "bool_attr": True,
                "float_attr": 3.14,
                "int_attr": 42,
                "string_attr": "hello",
            },
        )
    )

    requests.get("https://aws.amazon.com/")

    client.list_buckets()

    return {"statusCode": 200, "body": json.dumps(os.environ.get("_X_AMZN_TRACE_ID"))}
