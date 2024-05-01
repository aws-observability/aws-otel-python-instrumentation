/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import io.opentelemetry.config.Configs;
import io.opentelemetry.config.TestConfig;
import io.opentelemetry.containers.*;
import io.opentelemetry.distros.DistroConfig;
import io.opentelemetry.results.AppPerfResults;
import io.opentelemetry.results.MainResultsPersister;
import io.opentelemetry.results.ResultsCollector;
import io.opentelemetry.util.NamingConventions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

public class OverheadTests {

  private static final Logger logger = LoggerFactory.getLogger(OverheadTests.class);
  private static final Network NETWORK = Network.newNetwork();
  private static GenericContainer<?> collector;
  private final NamingConventions namingConventions = new NamingConventions();
  private final Map<String, Long> runDurations = new HashMap<>();

  @BeforeAll
  static void setUp() {
    // Ensure results folder exists, several intermediary results stored here, in addition to final
    // results.
    new File("./results").mkdirs();

    collector = CollectorContainer.build(NETWORK);
    collector.start();
  }

  @AfterAll
  static void tearDown() {
    collector.close();
  }

  @TestFactory
  Stream<DynamicTest> runAllTestConfigurations() {
    return Configs.all().map(config -> dynamicTest(config.getName(), () -> runTestConfig(config)));
  }

  void runTestConfig(TestConfig config) {
    logger.warn(
        String.format("Running test config %s: %s", config.getName(), config.getDescription()));
    runDurations.clear();
    config
        .getDistroConfigs()
        .forEach(
            distroConfig -> {
              logger.warn(
                  String.format(
                      "Running distro config %s: %s (%s)",
                      distroConfig.getName(), distroConfig.getDescription(), config.getName()));
              try {
                runAppOnce(config, distroConfig);
              } catch (Exception e) {
                fail("Unhandled exception in " + config.getName(), e);
              }
            });
    List<AppPerfResults> results =
        new ResultsCollector(namingConventions.local, runDurations).collect(config);
    new MainResultsPersister(config).write(results);
  }

  void runAppOnce(TestConfig config, DistroConfig distroConfig) throws Exception {
    GenericContainer<?> simpleRequestsService =
        new SimpleRequestsServiceContainer(NETWORK, collector, distroConfig, namingConventions)
            .build();
    long start = System.currentTimeMillis();
    simpleRequestsService.start();
    writeStartupTimeFile(distroConfig, start);

    long testStart = System.currentTimeMillis();
    startRecording(distroConfig, simpleRequestsService);

    GenericContainer<?> k6 =
        new K6Container(NETWORK, distroConfig, config, namingConventions).build();
    k6.start();

    long runDuration = System.currentTimeMillis() - testStart;
    runDurations.put(distroConfig.getName(), runDuration);

    int counter = 0;
    while(counter++ < 120) {
      if (simpleRequestsService.getLogs().contains("Wrote flamegraph data")) {
        logger.info("Flamegraph done.");
        break;
      } else {
          logger.info("Waiting for flamegraph.");
          sleep(1);
      }
    }

    simpleRequestsService.stop();
  }

  private void startRecording(DistroConfig distroConfig, GenericContainer<?> service)
      throws Exception {
    String[] command = {
      "sh",
      "executeProfiler.sh",
      namingConventions.container.performanceMetricsFileWithoutPath(distroConfig),
      namingConventions.container.root()
    };
    service.execInContainer(command);
  }

  private void writeStartupTimeFile(DistroConfig distroConfig, long start) throws IOException {
    long delta = System.currentTimeMillis() - start;
    Path startupPath = namingConventions.local.startupDurationFile(distroConfig);
    Files.writeString(startupPath, String.valueOf(delta));
  }

  private static void sleep(int seconds) {
    try {
      Thread.sleep(seconds * 1000L);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
