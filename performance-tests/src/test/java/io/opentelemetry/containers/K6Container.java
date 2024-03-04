/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry.containers;

import io.opentelemetry.config.TestConfig;
import io.opentelemetry.distros.DistroConfig;
import io.opentelemetry.util.NamingConventions;
import io.opentelemetry.util.RuntimeUtil;
import java.nio.file.Path;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class K6Container {
  private static final Logger logger = LoggerFactory.getLogger(K6Container.class);
  private final Network network;
  private final DistroConfig distroConfig;
  private final TestConfig config;
  private final NamingConventions namingConventions;

  public K6Container(
      Network network,
      DistroConfig distroConfig,
      TestConfig config,
      NamingConventions namingConvention) {
    this.network = network;
    this.distroConfig = distroConfig;
    this.config = config;
    this.namingConventions = namingConvention;
  }

  public GenericContainer<?> build() {
    Path k6OutputFile = namingConventions.container.k6Results(distroConfig);
    return new GenericContainer<>(DockerImageName.parse("loadimpact/k6"))
        .withNetwork(network)
        .withNetworkAliases("k6")
        .withLogConsumer(new Slf4jLogConsumer(logger))
        .withCopyFileToContainer(MountableFile.forHostPath("./k6"), "/app")
        .withFileSystemBind(namingConventions.localResults(), namingConventions.containerResults())
        .withCreateContainerCmdModifier(cmd -> cmd.withUser("root"))
        .withCommand(
            "run",
            "--vus",
            String.valueOf(config.getConcurrentConnections()),
            "--duration",
            String.valueOf(config.getDuration()),
            "--rps",
            String.valueOf(config.getMaxRequestRate()),
            "--summary-export",
            k6OutputFile.toString(),
            "--summary-trend-stats",
            "avg,p(0),p(50),p(90),p(99),p(100),count",
            "/app/performanceTest.js")
        .withCreateContainerCmdModifier(
            cmd -> cmd.getHostConfig().withCpusetCpus(RuntimeUtil.getNonApplicationCores()))
        .withStartupCheckStrategy(
            new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(15)));
  }
}
