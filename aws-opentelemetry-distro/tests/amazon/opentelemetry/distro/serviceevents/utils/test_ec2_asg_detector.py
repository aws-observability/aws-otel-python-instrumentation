# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for the EC2 Auto Scaling group resource detector.

The custom detector fetches aws:autoscaling:groupName from IMDS instance tags (the
stock AwsEc2ResourceDetector does not), so the SDK resource carries the same ASG the
CloudWatch agent uses to resolve ec2:<asg>. A local HTTP server stands in for IMDS.
"""

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import TestCase

from amazon.opentelemetry.distro.serviceevents.utils.ec2_asg_detector import (
    EC2_ASG_ATTRIBUTE,
    Ec2AutoScalingGroupResourceDetector,
)

_TOKEN = "fake-token"
_ASG_PATH = "/latest/meta-data/tags/instance/aws:autoscaling:groupName"
_TOKEN_PATH = "/latest/api/token"


def _make_handler(asg_status: int, asg_body: str, captured: dict):
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silence test server logging
            pass

        def do_PUT(self):  # noqa: N802 - http.server API
            if self.path == _TOKEN_PATH:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(_TOKEN.encode("utf-8"))
            else:
                self.send_response(404)
                self.end_headers()

        def do_GET(self):  # noqa: N802 - http.server API
            if self.path == _ASG_PATH:
                captured["token"] = self.headers.get("X-aws-ec2-metadata-token")
                self.send_response(asg_status)
                self.end_headers()
                if asg_status == 200:
                    self.wfile.write(asg_body.encode("utf-8"))
            else:
                self.send_response(404)
                self.end_headers()

    return _Handler


class TestEc2AutoScalingGroupResourceDetector(TestCase):
    def _detect_with_server(self, asg_status: int, asg_body: str):
        captured = {}
        server = HTTPServer(("127.0.0.1", 0), _make_handler(asg_status, asg_body, captured))
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()
        try:
            host = f"127.0.0.1:{server.server_address[1]}"
            resource = Ec2AutoScalingGroupResourceDetector(host=host).detect()
            return resource, captured
        finally:
            server.shutdown()
            thread.join()

    def test_fetches_asg_from_imds_tags(self):
        resource, captured = self._detect_with_server(200, "my-asg")
        self.assertEqual(resource.attributes.get(EC2_ASG_ATTRIBUTE), "my-asg")
        # The token from the PUT must be forwarded on the tag GET.
        self.assertEqual(captured.get("token"), _TOKEN)

    def test_trims_whitespace_from_asg_value(self):
        resource, _ = self._detect_with_server(200, "  my-asg\n")
        self.assertEqual(resource.attributes.get(EC2_ASG_ATTRIBUTE), "my-asg")

    def test_empty_resource_when_tags_disabled(self):
        # 404 on the tag path mimics instance metadata tags not being enabled.
        resource, _ = self._detect_with_server(404, "")
        self.assertNotIn(EC2_ASG_ATTRIBUTE, resource.attributes)

    def test_empty_resource_when_asg_value_blank(self):
        resource, _ = self._detect_with_server(200, "   ")
        self.assertNotIn(EC2_ASG_ATTRIBUTE, resource.attributes)

    def test_empty_resource_when_not_on_ec2(self):
        # Point at a closed port — connection refused, mimicking "not on EC2".
        resource = Ec2AutoScalingGroupResourceDetector(host="127.0.0.1:1").detect()
        self.assertNotIn(EC2_ASG_ATTRIBUTE, resource.attributes)
