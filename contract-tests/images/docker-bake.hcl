# docker-bake.hcl — builds the contract-test application images and the mock collector.
#
# Invoke from the repository root (so build contexts resolve like the previous
# `docker build .` calls), e.g.:
#
#   DISTRO=aws_opentelemetry_distro-x.y.z-py3-none-any.whl \
#     docker buildx bake -f contract-tests/images/docker-bake.hcl --load
#
# Variables are auto-populated from environment variables of the same name:
#   DISTRO         filename of the prebuilt distro wheel under ./dist (required by app images).
#   PYTHON_VERSION python base image tag used for the per-(app, version) cache scope. The actual
#                  build-arg is injected by build-contract-test-images.sh via `--set` only when a
#                  version is requested; when empty, each Dockerfile keeps its own default base.
#   CACHE_BACKEND  set to "gha" in CI to enable GitHub Actions layer caching; empty disables it
#                  (local builds have no Actions cache backend, so caching stays off by default).

variable "DISTRO" {
  default = ""
}

variable "PYTHON_VERSION" {
  default = ""
}

variable "CACHE_BACKEND" {
  default = ""
}

# Per-(app, version) GitHub Actions cache. Scoping by app and python version keeps a 3.13 build
# from colliding with a 3.10 build and lets the heavy ~60-dependency distro install layer persist
# across runs. Returns an empty list (no cache) unless CACHE_BACKEND=gha.
function "cache_from" {
  params = [scope]
  result = CACHE_BACKEND == "gha" ? ["type=gha,scope=${scope}"] : []
}

function "cache_to" {
  params = [scope]
  result = CACHE_BACKEND == "gha" ? ["type=gha,mode=max,scope=${scope}"] : []
}

# Shared config for application images. PYTHON_VERSION is NOT set here as a build-arg; the build
# script injects it with `--set '*.args.PYTHON_VERSION=...'` only when a version is requested, so
# the default (no-version) build reproduces each Dockerfile's native base exactly.
target "_app_common" {
  context   = "."
  platforms = ["linux/amd64"]
  args = {
    DISTRO = DISTRO
  }
}

# Flat application images: contract-tests/images/applications/<app>/Dockerfile
target "flat" {
  name     = app
  inherits = ["_app_common"]
  matrix = {
    app = [
      "botocore", "requests", "django",
      "psycopg2", "mysql-connector", "mysqlclient", "pymysql",
      "di-django", "di-fastapi", "di-flask",
      "serviceevents-django", "serviceevents-django-uwsgi",
      "serviceevents-fastapi", "serviceevents-flask",
    ]
  }
  dockerfile = "contract-tests/images/applications/${app}/Dockerfile"
  tags       = ["aws-application-signals-tests-${app}-app"]
  cache-from = cache_from("contract-${app}-py${PYTHON_VERSION}")
  cache-to   = cache_to("contract-${app}-py${PYTHON_VERSION}")
}

# GenAI images live one directory deeper: applications/gen_ai/<app>/Dockerfile
target "genai" {
  name     = app
  inherits = ["_app_common"]
  matrix = {
    app = ["crewai", "langchain", "llamaindex"]
  }
  dockerfile = "contract-tests/images/applications/gen_ai/${app}/Dockerfile"
  tags       = ["aws-application-signals-tests-${app}-app"]
  cache-from = cache_from("contract-${app}-py${PYTHON_VERSION}")
  cache-to   = cache_to("contract-${app}-py${PYTHON_VERSION}")
}

# Mock collector: fixed python base, does not install the distro, so no DISTRO/PYTHON_VERSION args.
target "mock-collector" {
  context    = "contract-tests/images/mock-collector"
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64"]
  tags       = ["aws-application-signals-mock-collector-python"]
  cache-from = cache_from("contract-mock-collector")
  cache-to   = cache_to("contract-mock-collector")
}

# Convenience groups for the dedicated DI / ServiceEvents workflows.
group "di" {
  targets = ["di-django", "di-fastapi", "di-flask"]
}

group "serviceevents" {
  targets = [
    "serviceevents-django", "serviceevents-django-uwsgi",
    "serviceevents-fastapi", "serviceevents-flask",
  ]
}

# Default = everything (used by the full-suite local/main-build path).
group "default" {
  targets = [
    "mock-collector",
    "botocore", "requests", "django",
    "psycopg2", "mysql-connector", "mysqlclient", "pymysql",
    "di-django", "di-fastapi", "di-flask",
    "serviceevents-django", "serviceevents-django-uwsgi",
    "serviceevents-fastapi", "serviceevents-flask",
    "crewai", "langchain", "llamaindex",
  ]
}
