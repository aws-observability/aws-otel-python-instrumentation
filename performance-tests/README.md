# Overhead tests

- [Process](#process)
- [What do we measure?](#what-do-we-measure)
- [Config](#config)
- [DistroConfigs](#distroConfigs)
- [Automation](#automation)
- [Setup and Usage](#setup-and-usage)
- [Visualization](#visualization)

This directory will contain tools and utilities that help us to measure the performance overhead introduced by the distro and to measure how this overhead changes over time.

The overhead tests here should be considered a "macro" benchmark. They serve to measure high-level overhead as perceived by the operator of a "typical" application. Tests are performed on Python 3.10.

## Process

There is one dynamic test here called OverheadTests. The `@TestFactory` method creates a test pass for each of the defined configurations.  Before the tests run, a single collector instance is started. Each test pass has one or more distroConfigs and those are tested in series. For each distro defined in a configuration, the test runner (using [testcontainers](https://www.testcontainers.org/)) will:

1. create a fresh postgres instance and populate it with initial data.
2. create a fresh instance of vehicle inventory service instrumented with the specified distroConfig, and image service (not currently instrumented)
3. measure the time until the app is marked "healthy" and then write it to a file.
4. if configured, perform a warmup phase. During the warmup phase, a bit of traffic is generated in order to get the application into a steady state (primarily helping facilitate jit compilations).
5. start a profiling recording by running a script that relies on psutils inside the application container
6. run a k6 test script with the configured number of iterations through the file and the configured number of concurrent virtual users (VUs).
7. after k6 completes, application is shut down
8. after application is shut down, postgres is shut down

And this repeats for every distro configured in each test configuration.

After all the tests are complete, the results are collected and committed back to the `/results` subdirectory as csv and summary text files.

## What do we measure?

For each test pass, we record the following metrics in order to compare distroConfigs and determine relative overhead.

// WIP: This list will change once we finalize the profiling script.

| metric name              | units  | description                                                                  |
|--------------------------| ------ |------------------------------------------------------------------------------|
| Startup time             | ms     | How long it takes for the spring app to report "healthy"                     |
| Total allocated mem      | bytes  | Across the life of the application                                           |
| Heap (min)               | bytes  | Smallest observed heap size                                                  |
| Heap (max)               | bytes  | Largest observed heap size                                                   |
| Thread switch rate       | # / s  | Max observed thread context switch rate                                      |
| GC time                  | ms     | Total amount of time spent paused for garbage collection                     |
| Request mean             | ms     | Average time to handle a single web request (measured at the caller)         |
| Request p95              | ms     | 95th percentile time to handle a single web requ4st (measured at the caller) |
| Iteration mean           | ms     | average time to do a single pass through the k6 test script                  |
| Iteration p95            | ms     | 95th percentile time to do a single pass through the k6 test script          |
| Peak threads             | #      | Highest number of running threads in the VM, including distroConfig threads  |
| Network read mean        | bits/s | Average network read rate                                                    |
| Network write mean       | bits/s | Average network write rate                                                   |
| Average user CPU         | %      | Average observed user CPU (range 0.0-1.0)                                    |
| Max user CPU             | %      | Max observed user CPU used (range 0.0-1.0)                                   |
| Average machine tot. CPU | %      | Average percentage of machine CPU used (range 0.0-1.0)                       |
| Total GC pause nanos     | ns     |  time spent paused due to GC                                                 |
| Run duration ms          | ms     | Duration of the test run, in ms                                              |

## Config

Each config contains the following:

- name
- description
- list of distroConfigs (see below)
- maxRequestRate (optional, used to throttle traffic)
- concurrentConnections (number of concurrent virtual users [VUs])
- totalIterations - the number of passes to make through the k6 test script
- warmupSeconds - how long to wait before starting conducting measurements

Additional configurations can be created by submitting a PR against the `Configs` class.

### DistroConfigs

An distroConfig is defined in code as a name, description, flag for instrumentation. and optional additional arguments to be passed to the application container. New distroConfigs may be defined by creating new instances of the `Distro` class. The `AgentResolver` is used to download the relevant distroConfig jar for an `Distro` definition.

## Setup and Usage

Pre-requirements:
* Have `docker` installed and running - verify by running the `docker` command.
* Export AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, and S3_BUCKET environment variables.

Steps:
* From `aws-otel-python-instrumentation` dir, execute:
```sh
./scripts/build_and_install_distro.sh
./scripts/set-up-performance-tests.sh
cd performance-tests
./gradlew test
```

The last step can be run or you can run from IDE (after setting environment variables appropriately).
