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
* database-clients - Applications meant to test database clients (e.g. psychopg2).

When testing a framework, we will create a sample application. The sample applications are stored following this convention: `contract-tests/images/applications/<framework-name>`.

# Adding tests for a new library or framework

The steps to add a new test for a library or framework are:
* Create a sample application.
    * The sample application should be created in `contract-tests/images/applications/<framework-name>`.
    * Implement a `pyproject.toml` (to ensure code style checks run), `Dockerfile`, and `requirements.txt` file. See the `requests` application for an example of this.
* Add a test class for the sample application.
    * The test class should be created in `contract-tests/tests/amazon/<framework-name>`.
    * The test class should extend `contract_test_base.py`

Note: For botocore applications, when creating new resources in [prepare_aws_server()](https://github.com/aws-observability/aws-otel-python-instrumentation/blob/166c4cb36da6634cb070df5a312a62f6b0136a9c/contract-tests/images/applications/botocore/botocore_server.py#L215), make sure to check if the resource already exist before creation. 
This is because each test pull the "aws-application-signals-tests-botocore-app" image and start a new container running `prepare_aws_server()` once, only the first attempt can succeeds, all subsequent attempts will fail due to the resources already existing. 

# How to run the tests locally?

Pre-requirements:
* Have `docker` installed and running - verify by running the `docker` command.

Steps:
* From `aws-otel-python-instrumentation` dir, execute:
```sh
./scripts/build_and_install_distro.sh
./scripts/set-up-contract-tests.sh
pytest contract-tests/tests
```