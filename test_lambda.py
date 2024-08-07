# test script for appsignals lambda behavior

from time import sleep

from opentelemetry import trace

if __name__ == "__main__":
    tracer = trace.get_tracer("test-tracer")
    with tracer.start_as_current_span("parent"):
        # Attach a new child and update the current span
        with tracer.start_as_current_span("child"):
            # sleep for 1s
            sleep(1)
        # Close child span, set parent as current
    # Close parent span, set default span as current
