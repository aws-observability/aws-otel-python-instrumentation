# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import Dict, Optional, Sequence

import re
import requests

from amazon.opentelemetry.distro._utils import is_installed
from amazon.opentelemetry.distro.llo_sender_client import LLOSenderClient
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import ReadableSpan, Event
from opentelemetry.sdk.trace.export import SpanExportResult

AWS_SERVICE = "xray"
_logger = logging.getLogger(__name__)


class OTLPAwsSpanExporter(OTLPSpanExporter):
    """
    This exporter extends the functionality of the OTLPSpanExporter to allow spans to be exported to the
    XRay OTLP endpoint https://xray.[AWSRegion].amazonaws.com/v1/traces. Utilizes the botocore
    library to sign and directly inject SigV4 Authentication to the exported request's headers.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        certificate_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_certificate_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        compression: Optional[Compression] = None,
        rsession: Optional[requests.Session] = None,
    ):

        self._aws_region = None
        self._has_required_dependencies = False
        self._llo_sender_client = LLOSenderClient()
        # Requires botocore to be installed to sign the headers. However,
        # some users might not need to use this exporter. In order not conflict
        # with existing behavior, we check for botocore before initializing this exporter.

        if endpoint and is_installed("botocore"):
            # pylint: disable=import-outside-toplevel
            from botocore import auth, awsrequest, session

            self.boto_auth = auth
            self.boto_aws_request = awsrequest
            self.boto_session = session.Session()

            # Assumes only valid endpoints passed are of XRay OTLP format.
            # The only usecase for this class would be for ADOT Python Auto Instrumentation and that already validates
            # the endpoint to be an XRay OTLP endpoint.
            self._aws_region = endpoint.split(".")[1]
            self._has_required_dependencies = True

        else:
            _logger.error(
                "botocore is required to export traces to %s. Please install it using `pip install botocore`",
                endpoint,
            )

        super().__init__(
            endpoint=endpoint,
            certificate_file=certificate_file,
            client_key_file=client_key_file,
            client_certificate_file=client_certificate_file,
            headers=headers,
            timeout=timeout,
            compression=compression,
            session=rsession,
        )

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        modified_spans = []

        for span in spans:
            # Process span attributes
            updated_attributes = {}

            # Copy all original attributes and handle LLO data
            for key, value in span.attributes.items():
                if self._should_offload(key):
                    metadata = {
                        "trace_id": format(span.context.trace_id, 'x'),
                        "span_id": format(span.context.span_id, 'x'),
                        "attribute_name": key,
                        "span_name": span.name
                    }

                    # Get S3 pointer from LLOSenderClient
                    s3_pointer = self._llo_sender_client.upload(value, metadata)

                    # Store the S3 pointer instead of original value to trim span
                    updated_attributes[key] = s3_pointer
                else:
                    # Keep original value if it is not LLO
                    updated_attributes[key] = value

            # Update span attributes
            if isinstance(span.attributes, BoundedAttributes):
                span._attributes = BoundedAttributes(
                    maxlen=span.attributes.maxlen,
                    attributes=updated_attributes,
                    immutable=span.attributes._immutable,
                    max_value_len=span.attributes.max_value_len
                )
            else:
                span._attributes = updated_attributes

            # Process span events
            if span.events:
                updated_events = []

                for event in span.events:
                    # Check if this event has any attributes to process
                    if not event.attributes:
                        updated_events.append(event) # Keep the original event
                        continue

                    # Process event attributes for LLO content
                    updated_event_attributes = {}
                    need_to_update = False

                    for key, value in event.attributes.items():
                        if self._should_offload(key):
                            metadata = {
                                "trace_id": format(span.context.trace_id, 'x'),
                                "span_id": format(span.context.span_id, 'x'),
                                "attribute_name": key,
                                "event_name": event.name,
                                "event_time": str(event.timestamp)
                            }

                            s3_pointer = self._llo_sender_client.upload(value, metadata)
                            updated_event_attributes[key] = s3_pointer
                            need_to_update = True
                        else:
                            updated_event_attributes[key] = value

                    if need_to_update:
                        # Create new Event with the updated attributes
                        limit = None
                        if isinstance(event.attributes, BoundedAttributes):
                            limit = event.attributes.maxlen

                        updated_event = Event(
                            name=event.name,
                            attributes=updated_event_attributes,
                            timestamp=event.timestamp,
                            limit=limit
                        )

                        updated_events.append(updated_event)
                    else:
                        # Keep the original event
                        updated_events.append(event)

                # Update the span's events with processed events
                span._events = updated_events

            modified_spans.append(span)

        # Export the modified spans
        return super().export(modified_spans)

    def _should_offload(self, key):
        """Determine if LLO based on the attribute key. Strict matching is enforced as to not introduce unintended behavior."""
        exact_match_patterns = [
            "traceloop.entity.input",
            "traceloop.entity.output",
            "message.content",
            "input.value",
            "output.value",
            "gen_ai.prompt",
            "gen_ai.completion",
            "gen_ai.content.revised_prompt",
        ]

        regex_match_patterns = [
            r"^gen_ai\.prompt\.\d+\.content$",
            r"^gen_ai\.completion\.\d+\.content$",
            r"^llm.input_messages\.\d+\.message.content$",
            r"^llm.output_messages\.\d+\.message.content$",
        ]

        return (
            any(pattern == key for pattern in exact_match_patterns) or
            any(re.match(pattern, key) for pattern in regex_match_patterns)
        )

    # Overrides upstream's private implementation of _export. All behaviors are
    # the same except if the endpoint is an XRay OTLP endpoint, we will sign the request
    # with SigV4 in headers before sending it to the endpoint. Otherwise, we will skip signing.
    def _export(self, serialized_data: bytes):
        if self._has_required_dependencies:
            request = self.boto_aws_request.AWSRequest(
                method="POST",
                url=self._endpoint,
                data=serialized_data,
                headers={"Content-Type": "application/x-protobuf"},
            )

            credentials = self.boto_session.get_credentials()

            if credentials is not None:
                signer = self.boto_auth.SigV4Auth(credentials, AWS_SERVICE, self._aws_region)

                try:
                    signer.add_auth(request)
                    self._session.headers.update(dict(request.headers))

                except Exception as signing_error:  # pylint: disable=broad-except
                    _logger.error("Failed to sign request: %s", signing_error)
        else:
            _logger.debug("botocore is not installed. Failed to sign request to export traces to: %s", self._endpoint)

        return super()._export(serialized_data)
