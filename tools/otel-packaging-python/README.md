# Auto-instrumentation bootstrap for the operator image

The ADOT Python image the CloudWatch agent operator injects decides, in every Python process it reaches, whether
auto-instrumentation can start. When it cannot (for example on a Python older than the payload supports), it removes
itself from `sys.path` and `PYTHONPATH` and the application starts with its own packages, without telemetry, instead of
crashing on the payload's.

That decision is made by the [OpenTelemetry Packaging](https://github.com/open-telemetry/opentelemetry-packaging)
Python bootstrap (`packaging/common/python/sitecustomize.py`), vendored here unchanged and patched for the ADOT payload,
so that we track upstream instead of maintaining our own.

## Files

| File | Purpose |
|---|---|
| `UPSTREAM` | Upstream commit and SHA-256 of the vendored file, and the Python versions the payload supports |
| `vendor/sitecustomize.py`, `vendor/NOTICE` | The upstream file and its repository's NOTICE, unchanged |
| `patches/*.patch` | ADOT changes to the upstream file, applied in order |
| `adot_sitecustomize.py` | Entry point the operator's `PYTHONPATH` loads; runs the bootstrap |
| `build.sh` | Installs all of this into a payload; called by the root `Dockerfile` |
| `tests/` | Tests for `build.sh`, the patched bootstrap, and the entry point |

## How it runs

The operator sets `PYTHONPATH=<payload>/opentelemetry/instrumentation/auto_instrumentation:<customer entries>:<payload>`.

1. Python imports `<payload>/opentelemetry/instrumentation/auto_instrumentation/sitecustomize.py`, which is
   `adot_sitecustomize.py`. It replaces upstream's, which only calls `initialize()`, and keeps ours ahead of any
   `sitecustomize` the customer has on `PYTHONPATH`.
2. It runs `<payload>/_otel_packaging_sitecustomize.py`, the patched upstream bootstrap. The file is not named
   `sitecustomize.py`: upstream `initialize()` stops child processes it starts from re-running auto-instrumentation by
   removing the `auto_instrumentation` directory from `PYTHONPATH`, which only works if no other `sitecustomize.py` is
   on it.
3. The bootstrap checks the Python version, the OTLP protocol, and the payload's OpenTelemetry distributions
   (`<payload>/cp3<minor>/all-dependencies.txt`), then calls `initialize(swallow_exceptions=False)`, which loads the ADOT
   distro. On a failed check or an initialization error it removes the payload from `sys.path` and `PYTHONPATH`, clears
   `PYTHON_AUTO_INSTRUMENTATION_AGENT_PATH_PREFIX`, and logs one line to stderr.
4. When the bootstrap deactivates, `adot_sitecustomize.py` also removes every other payload directory from `sys.path` and
   `PYTHONPATH`: its own, which only the operator adds, and `cp3<minor>`, which the bootstrap inserts.

`all-dependencies.txt` lists only the payload's OpenTelemetry distributions. The SDK pins the exact API version it was
built with, so these must be the payload's copies; a different version earlier on `sys.path` (for example from the
customer's `PYTHONPATH`) deactivates auto-instrumentation instead of letting it start half-broken. General-purpose
libraries are not listed, so the check does not change which applications are instrumented.

## Patches

| Patch | Change | Why |
|---|---|---|
| `0001` | The double-instrumentation check never deactivates; it reports at debug level (`OTEL_INJECTOR_LOG_LEVEL=debug`) | Applications commonly depend on OpenTelemetry packages without instrumenting themselves; Apache Airflow depends on `opentelemetry-exporter-otlp` |
| `0002` | The payload goes back to its position on `sys.path` instead of the end | The ADOT image has always kept its packages ahead of the application's `site-packages`; moving it behind them changes which versions applications run with |
| `0003` | `OTEL_{TRACES,METRICS,LOGS}_EXPORTER` are not set | Upstream selects its own vendored exporters, which the ADOT payload does not contain; the ADOT distro sets its own defaults |

Each patch is a candidate for an upstream option; drop it here once upstream has one.

## Updating upstream

1. Copy `packaging/common/python/sitecustomize.py` and `NOTICE` at the new commit to `vendor/`.
2. Update `UPSTREAM_COMMIT` and `UPSTREAM_SHA256` in `UPSTREAM`.
3. Re-apply `patches/` and refresh any that no longer apply. Drop those upstream has absorbed.
4. Run the tests.

## Tests

```sh
tox -e 3.11-otel-packaging-python
```

`RUN_DOCKER_TESTS=1` also runs the old-interpreter tests, which start `python:2.7-slim` to `python:3.9-slim` containers.
