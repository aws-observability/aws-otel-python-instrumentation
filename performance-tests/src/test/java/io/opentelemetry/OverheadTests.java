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
import io.opentelemetry.containers.CollectorContainer;
import io.opentelemetry.containers.ImageServiceContainer;
import io.opentelemetry.containers.K6Container;
import io.opentelemetry.containers.PostgresContainer;
import io.opentelemetry.containers.VehicleInventoryServiceContainer;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class OverheadTests {

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
    runDurations.clear();
    config
        .getDistroConfigs()
        .forEach(
            distroConfig -> {
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
    GenericContainer<?> postgres = new PostgresContainer(NETWORK).build();
    postgres.start();

    GenericContainer<?> imageService = new ImageServiceContainer(NETWORK).build();
    imageService.start();

    GenericContainer<?> vehicleInventoryService =
        new VehicleInventoryServiceContainer(NETWORK, collector, distroConfig).build();
    long start = System.currentTimeMillis();
    vehicleInventoryService.start();
    writeStartupTimeFile(distroConfig, start);

    populateDatabase();

    if (config.getWarmupSeconds() > 0) {
      // doWarmupPhase(config, vehicleInventoryService);
    }

    long testStart = System.currentTimeMillis();
    // startRecording(distroConfig, vehicleInventoryService);

    GenericContainer<?> k6 =
        new K6Container(NETWORK, distroConfig, config, namingConventions).build();
    k6.start();

    long runDuration = System.currentTimeMillis() - testStart;
    runDurations.put(distroConfig.getName(), runDuration);

    vehicleInventoryService.stop();
    imageService.stop();
    postgres.stop();
  }

  private void startRecording(
      DistroConfig distroConfig, GenericContainer<?> vehicleInventoryService) throws Exception {
    Path outFile = namingConventions.container.jfrFile(distroConfig);
    String[] command = {
      "jcmd",
      "1",
      "JFR.start",
      "settings=/app/overhead.jfc",
      "dumponexit=true",
      "name=petclinic",
      "filename=" + outFile
    };
    vehicleInventoryService.execInContainer(command);
  }

  private void doWarmupPhase(TestConfig testConfig, GenericContainer<?> vehicleInventoryService)
      throws IOException, InterruptedException {
    System.out.println(
        "Performing startup warming phase for " + testConfig.getWarmupSeconds() + " seconds...");

    // excluding the JFR recording from the warmup causes strange inconsistencies in the results
    System.out.println("Starting disposable JFR warmup recording...");
    String[] startCommand = {
      "jcmd",
      "1",
      "JFR.start",
      "settings=/app/overhead.jfc",
      "dumponexit=true",
      "name=warmup",
      "filename=warmup.jfr"
    };
    vehicleInventoryService.execInContainer(startCommand);

    long deadline =
        System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(testConfig.getWarmupSeconds());
    while (System.currentTimeMillis() < deadline) {
      GenericContainer<?> k6 =
          new GenericContainer<>(DockerImageName.parse("loadimpact/k6"))
              .withNetwork(NETWORK)
              .withCopyFileToContainer(MountableFile.forHostPath("./k6"), "/app")
              .withCommand("run", "-u", "5", "-i", "200", "/app/basic.js")
              .withStartupCheckStrategy(new OneShotStartupCheckStrategy());
      k6.start();
    }

    System.out.println("Stopping disposable JFR warmup recording...");
    String[] stopCommand = {"jcmd", "1", "JFR.stop", "name=warmup"};
    vehicleInventoryService.execInContainer(stopCommand);

    System.out.println("Warmup complete.");
  }

  private void populateDatabase() {
    GenericContainer<?> k6 =
        new GenericContainer<>(DockerImageName.parse("loadimpact/k6"))
            .withNetwork(NETWORK)
            .withCopyFileToContainer(MountableFile.forHostPath("./k6"), "/app")
            .withCommand("run", "/app/setUp.js")
            .withStartupCheckStrategy(new OneShotStartupCheckStrategy());
    k6.start();
  }

  private void writeStartupTimeFile(DistroConfig distroConfig, long start) throws IOException {
    long delta = System.currentTimeMillis() - start;
    Path startupPath = namingConventions.local.startupDurationFile(distroConfig);
    Files.writeString(startupPath, String.valueOf(delta));
  }
}
