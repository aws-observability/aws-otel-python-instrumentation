# AWS Distro for OpenTelemetry Python Instrumentation

## Introduction

This project provide AWS Distro base on [OpenTelemetry Python Contrib](https://github.com/open-telemetry/opentelemetry-python-contrib),
preconfigured for use with AWS services. Please check out that project too to get a better
understanding of the underlying internals.

## Notices

### Python Version Support
This project ensures compatibility with the following supported Python versions: 3.8, 3.9, 3.10, 3.11

## Code Style Check

This package applies code style check automatically when created a push/pull request to the project repository. You can apply style check locally before submitting the PR by following:
1. Install related packages:
```sh
pip install isort pylint black flake8 codespell readme_renderer
```
2. Check code style errors using codespell and lint:
```sh
codespell
python scripts/eachdist.py lint --check-only
```
3. Apply the fix for the errors automatically:
```sh
codespell . --write-changes
python scripts/eachdist.py lint
```

## Unit test
This package detects all the unit tests defined in folder with naming "tests"/"test" under the same directory as pyproject.toml file. Please make sure to add unit test every time a new feature added. 
The workflow will run the test tox environment automatically whenever there is a push/pull request. Please make sure you install the related package needed for the unit tests in `commands_pre`.

If you want to test a specific component/feature, please add a new environment in tox.ini file, and add related workflow as needed.

## License

This project is licensed under the Apache-2.0 License.

