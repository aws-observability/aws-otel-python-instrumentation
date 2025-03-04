from flask import Flask, jsonify

from amazon.opentelemetry.exporters.otlp.udp import OTLPUdpSpanExporter
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

app = Flask(__name__)

# Set up tracer provider
tracer_provider = TracerProvider()
trace.set_tracer_provider(tracer_provider)

# Set up UDP exporter with batch processor
exporter = OTLPUdpSpanExporter(endpoint="127.0.0.1:2000")
span_processor = BatchSpanProcessor(exporter)
tracer_provider.add_span_processor(span_processor)

# Get tracer
tracer = trace.get_tracer(__name__)


@app.route("/test", methods=["GET"])
def create_trace():
    # Create a span for testing with various attributes
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("test_parent_span") as parent:
        parent.set_attribute("service.name", "validation-app")
        parent.set_attribute("test.attribute", "test_value")
        parent.add_event("test-event", {"event.data": "some data"})

        # Get the trace ID
        trace_id = format(parent.get_span_context().trace_id, "032x")

        # Add a child span
        with tracer.start_as_current_span("test_child_span") as child:
            child.set_attribute("child.attribute", "child_value")
            print("Created spans with attributes and events")

    # Force flush to ensure spans are exported immediately
    success = tracer_provider.force_flush()
    print(f"Force flush {'succeeded' if success else 'failed'}")

    return jsonify({"trace_id": trace_id})


if __name__ == "__main__":
    app.run(port=8080)
