# AWS Distro for OpenTelemetry - Instrumentation for Python

## Introduction

This project is a redistribution of the [OpenTelemetry Distro for Python](https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/opentelemetry-distro),
preconfigured for use with AWS services. Please check out that project too to get a better
understanding of the underlying internals. You won't see much code in this repository since we only
apply some small configuration changes, and our OpenTelemetry friends takes care of the rest. The 
exception to this is support for Application Signals.

We provided a Python agent that can be attached to any application using a supported Python version and dynamically injects
bytecode to capture telemetry from a number of popular libraries and frameworks. The telemetry data
can be exported in a variety of formats. In addition, the agent and exporter can be configured via
command line arguments or environment variables. The net result is the ability to gather telemetry
data from a Python application without any code changes.

## Getting Started

Check out the [getting started documentation](https://aws-otel.github.io/docs/getting-started/python-sdk/auto-instr).

## Supported Python libraries and frameworks
For the complete list of supported frameworks, please refer to the [OpenTelemetry for Python documentation](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/README.md).

## Support

Please note that as per policy, we're providing support via GitHub on a best effort basis. However, if you have AWS Enterprise Support you can create a ticket and we will provide direct support within the respective SLAs.

## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.

## License

This project is licensed under the Apache-2.0 License.

## Notices

### Python Version Support
This project ensures compatibility with the following supported Python versions: 3.8, 3.9, 3.10, 3.11