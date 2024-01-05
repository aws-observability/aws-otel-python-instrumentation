# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from sys import argv

from requests import get

from opentelemetry import trace
from opentelemetry.propagate import inject
import sys

sys.path.append("./aws-otel-distro/src/opentelemetry/distro")
from aws_distro import AWSDistro
from aws_distro import AWSTracerProvider
from aws_distro import RemoteAttributesSpanProcessor

trace.set_tracer_provider(AWSTracerProvider())
tracer = trace.get_tracer_provider().get_tracer(__name__)

trace.get_tracer_provider().add_span_processor(RemoteAttributesSpanProcessor())

print("==================== argv" + str(argv))

assert len(argv) == 2

with tracer.start_as_current_span("client"):
    with tracer.start_as_current_span("client-server"):
        headers = {}
        inject(headers)
        requested = get(
            "http://localhost:8082/server_request",
            params={"param": argv[1]},
            headers=headers,
        )
        print("==================== requested" + str(requested))

        assert requested.status_code == 200
