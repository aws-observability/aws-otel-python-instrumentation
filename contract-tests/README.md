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

# How to run the tests locally?

Pre-requirements:
* Have `docker` (with `buildx`) installed and running - verify by running the `docker` command.

Steps:
* From `aws-otel-python-instrumentation` dir, execute:
```sh
./scripts/build_and_install_distro.sh
./scripts/set-up-contract-tests.sh
pytest contract-tests/tests
```

`set-up-contract-tests.sh` is a thin wrapper that builds the application images
(`build-contract-test-images.sh`, via `docker buildx bake`) and then prepares the host
(`run-contract-tests.sh` - installs test deps and the `mock_collector`/`contract_tests` wheels).
You can run those two steps directly, and you can build/run a subset:

```sh
# Build only some images (bake targets/groups), optionally against a specific Python base:
./scripts/build-contract-test-images.sh 3.13 botocore requests   # build on python:3.13
./scripts/build-contract-test-images.sh "" serviceevents          # group, default Python bases
./scripts/run-contract-tests.sh
pytest contract-tests/tests/test/amazon/botocore -v
```

Passing a Python version builds every selected application image against `python:<version>`
(the default keeps each Dockerfile's own base). Available bake targets are the application names
(e.g. `botocore`, `django`, `crewai`) plus the `di` and `serviceevents` groups; see
`contract-tests/images/docker-bake.hcl`.