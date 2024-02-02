# Introduction

This directory contain contract tests that exist to prevent regressions. They cover:
* [OpenTelemetry semantic conventions](https://github.com/open-telemetry/semantic-conventions/).
* Application Signals-specific attributes.

# How it works?

The tests present here rely on the auto-instrumentation of a sample application which will send telemetry signals to a mock collector. The tests will use the data collected by the mock collector to perform assertions and validate that the contracts are being respected.

# Types of tested frameworks

The frameworks and libraries that are tested in the contract tests should fall in the following categories (more can be added on demand):
* http-servers - applications meant to test http servers (e.g. Django).
* http-clients - applications meant to test http clients (e.g. requests).
* aws-sdk - Applications meant to test the AWS SDK (e.g. botocore).
* database-clients - Applications meant to test database clients (e.g. asycnpg).

When testing a framework, we will create a sample application. The sample applications are stored following this convention: `contract-tests/images/<framework-name>`.

# Adding tests for a new library or framework

The steps to add a new test for a library or framework are:
* Create a sample application.
    * The sample application should be created in `contract-tests/images/applications/<framework-name>`.
* Add a test class for the sample application.
    * The test class should be created in `contract-tests/tests/amazon/<framework-name>`.

# How to run the tests locally?

Pre-requirements:
* Have `docker` installed and running
* Copy the `aws_opentelemetry_distro` wheel file to each application folder under `images` (e.g. to `requests`, but not `mock-collector`)

From `aws-otel-python-instrumentation/contract-tests` execute:

```
./create-images.sh
mkdir dist
cd images/mock-collector
python3 -m build --outdir ../../dist
pip wheel --no-deps mock_collector-1.0.0.tar.gz
pip install mock_collector-1.0.0-py3-none-any.whl --force-reinstall
<TODO>
```