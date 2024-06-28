# Contributing Guidelines

Thanks a lot for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we really appreciate your help. Take a look through this document to make sure we can give you a great
experience with your contribution.


## Reporting Bugs/Feature Requests

We're always happy to hear about any bugs or features requests using GitHub issues.

When filing an issue, please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment


## Contributing via Pull Requests
Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.
4. You are not mixing substantial refactoring changes in with functional changes.
   1. If refactoring is desirable, publish a separate refactoring PR first, followed by a functional change PR. This will ensure safe and efficient reviews.
   2. PRs that do not meet these expectations will be rejected.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Ensure local tests pass.
4. Commit to your fork using clear commit messages.
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.
7. Please do not squash commits between revisions, this makes review challenging, as the diff between revisions is harder to find and review.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).

The following sections provide some guidance that will help you make contributions.

### Build and install distro locally
From `aws-otel-python-instrumentation` dir, execute:
```sh
./scripts/build_and_install_distro.sh
```

### Test a sample App
1. Setup env and install project dependencies
```sh
mkdir auto_instrumentation
virtualenv auto_instrumentation
source auto_instrumentation/bin/activate
pip install flask requests boto3 opentelemetry-instrumentation-flask opentelemetry-instrumentation-botocore opentelemetry-instrumentation
```
2. Install the project pkg by following "Build a Wheel file locally" step above. Please make sure to install “aws-opentelemetry-distro” by following steps instead of install "opentelemetry-distro” directly.
3. Add AWS test account credential into the terminal, setup environment variable and run sample server:
```sh
export OTEL_PYTHON_DISTRO="aws_distro"
export OTEL_PYTHON_CONFIGURATOR="aws_configurator"
opentelemetry-instrument python ./sample-applications/simple-client-server/server_automatic_s3client.py
```
4. Prepare a client.py, an example is `./tests/client.py`, open a new terminal and run sample client:
```sh
python ./sample-applications/simple-client-server/client.py testing
```
The span content will be output into terminal console

### Code Style Check

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

### Unit test
This package detects all the unit tests defined in folder with naming "tests"/"test" under the same directory as pyproject.toml file. Please make sure to add unit test every time a new feature added.
The workflow will run the test tox environment automatically whenever there is a push/pull request. Please make sure you install the related package needed for the unit tests in `commands_pre`.

If you want to test a specific component/feature, please add a new environment in tox.ini file, and add related workflow as needed.


## Finding contributions to work on
Looking at the existing issues is a great way to find something to contribute on. As this is a repository for experimenting
and trying out new integrations, there may be few open issues filed but we're always happy to add more test apps for
different frameworks.


## Code of Conduct
This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.


## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. When contributing code, make sure there are no copyright
headers - the code is available for users to copy into their own apps.
