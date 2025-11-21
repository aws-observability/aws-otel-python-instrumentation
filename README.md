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

## Experimental Features

This distribution includes experimental features that are under active development. For detailed information about experimental features including code attributes, please refer to the [aws-opentelemetry-distro README](aws-opentelemetry-distro/README.md).


## Support

Please note that as per policy, we're providing support via GitHub on a best effort basis. However, if you have AWS Enterprise Support you can create a ticket and we will provide direct support within the respective SLAs.

## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.

## License

This project is licensed under the Apache-2.0 License.

## Notices

### Python Version Support
This project ensures compatibility with the following supported Python versions: 3.9, 3.10, 3.11, 3.12, 3.13

### Note on Amazon CloudWatch Application Signals
[Amazon CloudWatch Application Signals](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Application-Monitoring-Sections.html) components are designed to seamlessly work with all library instrumentations offered by [OpenTelemetry Python auto-instrumentation](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/README.md). While upstream OpenTelemetry Python instrumentations are in beta, Application Signals components are stable, production ready and have also been tested for popular libraries/frameworks such as [Django, Boto3, and others](https://github.com/aws-observability/aws-otel-python-instrumentation/tree/main/contract-tests/images/applications). We will prioritize backward compatibility for Application Signals components, striving to ensure that they remain functional even in the face of potential breaking changes introduced by OpenTelemetry upstream libraries. Please [raise an issue](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/main/CONTRIBUTING.md#reporting-bugsfeature-requests) if you notice Application Signals doesn't work for a particular OpenTelemetry supported library.

## Checksum Verification
Artifacts released will include a `.sha256` file for checksum verification starting from v0.7.0
To verify, run the command `shasum -a 256 -c <artifact_name>.sha256` 
It should return the output `<artifact_name>: OK` if the validation is successful
