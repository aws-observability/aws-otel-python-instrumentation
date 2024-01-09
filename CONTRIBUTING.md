# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.

## Build a Wheel file locally
1. Get the latest versions of packaging tools
```sh
pip install --upgrade pip setuptools wheel packaging
```
2. Create a dist directory to save build output
```sh
mkdir -p ./dist
rm -rf ./dist/*
```
3. cd to project folder
```sh
cd ./opentelemetry-distro
```
4. Build the project, and save the output into dist folder
```sh
python3 -m build --outdir ../dist
```
5. Check the project `${pkg_version}` which can be found in `./opentelemetry-distro/src/amazon/opentelemetry/distro/version.py`
6. Build a wheel for the project distribution
```sh
cd ../dist
pip wheel --no-deps aws_opentelemetry_distro-${pkg_version}.tar.gz
```
The `*.whl` can be found under `dist` folder, and the pkg can be built by `pip install`

## Test a sample App
1. Setup env and install project dependencies
```sh
mkdir auto_instrumentation
virtualenv auto_instrumentation
source auto_instrumentation/bin/activate
pip install flask
pip install requests
pip install boto3
pip install opentelemetry-instrumentation-flask
pip install opentelemetry-instrumentation-botocore
pip install opentelemetry-instrumentation
```
2. Install the project pkg by following "Build a Wheel file locally" step above.
3. Add AWS test account credential into the terminal, setup environment variable and run sample server:
```sh
export OTEL_PYTHON_DISTRO="aws_distro"
export OTEL_PYTHON_CONFIGURATOR="aws_configurator"
opentelemetry-instrument python ./tests/server_automatic_s3client.py
```
4. Prepare a client.py, an example is `./tests/client.py`, open a new terminal and run sample client:
```sh
python client.py testing
```
The span content will be output into terminal console

## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check existing open, or recently closed, issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment


## Contributing via Pull Requests
Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Ensure local tests pass.
4. Commit to your fork using clear commit messages.
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).


## Finding contributions to work on
Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any 'help wanted' issues is a great place to start.


## Code of Conduct
This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.


## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.
