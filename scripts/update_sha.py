import argparse

import requests
from ruamel.yaml import YAML

CORE_API_URL = (
    "https://api.github.com/repos/open-telemetry/opentelemetry-python/commits/"
)
CONTRIB_API_URL = "https://api.github.com/repos/open-telemetry/opentelemetry-python-contrib/commits/"
WORKFLOW_FILE = ".github/workflows/test.yml"


def get_core_sha(branch):
    url = CORE_API_URL + branch
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()["sha"]


def get_contrib_sha(branch):
    url = CONTRIB_API_URL + branch
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()["sha"]


def update_core_sha(sha):
    yaml = YAML()
    yaml.preserve_quotes = True
    with open(WORKFLOW_FILE, "r", encoding="utf-8") as file:
        workflow = yaml.load(file)
    workflow["env"]["CORE_REPO_SHA"] = sha
    with open(WORKFLOW_FILE, "w", encoding="utf-8") as file:
        yaml.dump(workflow, file)


def update_contrib_sha(sha):
    yaml = YAML()
    yaml.preserve_quotes = True
    with open(WORKFLOW_FILE, "r", encoding="utf-8") as file:
        workflow = yaml.load(file)
    workflow["env"]["CONTRIB_REPO_SHA"] = sha
    with open(WORKFLOW_FILE, "w", encoding="utf-8") as file:
        yaml.dump(workflow, file)


def main():
    args = parse_args()
    core_sha = get_core_sha(args.core_branch)
    contrib_sha = get_contrib_sha(args.contrib_branch)
    update_core_sha(core_sha)
    update_contrib_sha(contrib_sha)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Updates the SHA in the workflow file"
    )
    parser.add_argument("-cb", "--core-branch", help="core branch to use")
    parser.add_argument(
        "-tb", "--contrib-branch", help="contrib branch to use"
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
