# AWS Distro For OpenTelemetry Python Distro

## Installation

```
pip install aws-opentelemetry-distro
```

This package provides Amazon Web Services distribution of the OpenTelemetry Python Instrumentation, which allows for auto-instrumentation of Python applications.

## Experimental Features

**⚠️ Note: Experimental features and their behavior may change in future versions.**

### Code Attributes

Code attributes is an experimental feature that automatically captures source code context information when trace data is generated. This feature enhances observability by providing precise code-level details that help developers quickly identify and debug issues.

**What Code Attributes Capture:**
* **Source file path** - The exact file where the telemetry originated
* **Function name** - The method or function fully-qualified name, including class/namespace context when applicable (e.g., `com.example.MyService.processRequest`, `MyClass.my_method`)
* **Line numbers** - Precise code location for pinpoint debugging

**How to Enable:**
To enable code attributes, set the following environment variable:
```bash
export OTEL_AWS_EXPERIMENTAL_CODE_ATTRIBUTES=true
```


## References

* [OpenTelemetry Project](https://opentelemetry.io/)
* [Example using opentelemetry-distro](https://opentelemetry.io/docs/instrumentation/python/distro/)
