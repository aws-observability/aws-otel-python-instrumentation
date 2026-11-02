import json
import logging
import os

import boto3
import requests

try:
    from opentelemetry._logs import LogRecord, SeverityNumber, get_logger
except Exception:  # pylint: disable=broad-except
    LogRecord = None
    SeverityNumber = None
    get_logger = None

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
otel_logger = get_logger(__name__) if get_logger else None

client = boto3.client("s3")


# lambda function
def lambda_handler(event, context):
    logger.debug("debug-level-test-message")
    logger.info("info-level-test-message")
    logger.warning("warn-level-test-message")
    logger.error("error-level-test-message")

    # Test all attribute data types via OTel Logs API directly
    try:
        if otel_logger is not None:
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
    except Exception:  # pylint: disable=broad-except
        logger.exception("Failed to emit OTel log record")

    requests.get("https://aws.amazon.com/")

    client.list_buckets()

    return {"statusCode": 200, "body": json.dumps(os.environ.get("_X_AMZN_TRACE_ID"))}
