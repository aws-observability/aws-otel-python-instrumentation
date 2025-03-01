import socket
import threading
import time
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from amazon.opentelemetry.exporters.otlp.udp import OTLPUdpSpanExporter

# Set up a UDP server to verify data is sent
def udp_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('127.0.0.1', 2000))
    sock.settimeout(5)
    print("UDP server listening on 127.0.0.1:2000")
    try:
        data, addr = sock.recvfrom(4096)
        print(f"Received data from {addr}")
        if data:
            print("✅ Successfully received exported span data")
            return True
    except socket.timeout:
        print("❌ No data received within timeout period")
        return False
    finally:
        sock.close()

# Start UDP server in a separate thread
server_thread = threading.Thread(target=udp_server)
server_thread.daemon = True
server_thread.start()

# Set up tracer provider
tracer_provider = TracerProvider()
trace.set_tracer_provider(tracer_provider)

# Set up UDP exporter with batch processor (more realistic usage)
exporter = OTLPUdpSpanExporter(endpoint="127.0.0.1:2000")
span_processor = BatchSpanProcessor(exporter)
tracer_provider.add_span_processor(span_processor)

# Create a span for testing with various attributes
tracer = trace.get_tracer(__name__)
with tracer.start_as_current_span("test_parent_span") as parent:
    parent.set_attribute("service.name", "validation-app")
    parent.set_attribute("test.attribute", "test_value")
    parent.add_event("test-event", {"event.data": "some data"})
    
    # Add a child span
    with tracer.start_as_current_span("test_child_span") as child:
        child.set_attribute("child.attribute", "child_value")
        print("Created spans with attributes and events")

# Force flush to ensure spans are exported immediately
success = tracer_provider.force_flush()
print(f"Force flush {'succeeded' if success else 'failed'}")

# Give some time for the UDP packet to be processed
time.sleep(2)

# Shutdown
tracer_provider.shutdown()
print("Test completed")
