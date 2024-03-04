/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry.containers;

import io.opentelemetry.util.RuntimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class ImageServiceContainer {

  private static final Logger logger = LoggerFactory.getLogger(ImageServiceContainer.class);
  public static final int PORT = 8000;
  public static final String NETWORK_ALIAS = "image-service";

  private final Network network;

  public ImageServiceContainer(Network network) {
    this.network = network;
  }

  public GenericContainer<?> build() {
    return new GenericContainer<>(DockerImageName.parse("performance-test/image-service"))
        .withNetwork(network)
        .withNetworkAliases(NETWORK_ALIAS)
        .withLogConsumer(new Slf4jLogConsumer(logger))
        .withExposedPorts(PORT)
        .waitingFor(Wait.forHttp("/images/health-check").forPort(PORT))
        .withEnv("DJANGO_SETTINGS_MODULE", "ImageServiceApp.settings")
        .withEnv("PORT", Integer.toString(PORT))
        .withEnv("POSTGRES_DATABASE", PostgresContainer.DATABASE_NAME)
        .withEnv("POSTGRES_USER", PostgresContainer.USERNAME)
        .withEnv("POSTGRES_PASSWORD", PostgresContainer.PASSWORD)
        .withEnv("DB_SERVICE_HOST", PostgresContainer.NETWORK_ALIAS)
        .withEnv("DB_SERVICE_PORT", Integer.toString(PostgreSQLContainer.POSTGRESQL_PORT))
        .withEnv("AWS_ACCESS_KEY_ID", System.getenv("AWS_ACCESS_KEY_ID"))
        .withEnv("AWS_SECRET_ACCESS_KEY", System.getenv("AWS_SECRET_ACCESS_KEY"))
        .withEnv("AWS_SESSION_TOKEN", System.getenv("AWS_SESSION_TOKEN"))
        .withEnv("S3_BUCKET", System.getenv("S3_BUCKET"))
        .withCreateContainerCmdModifier(
            cmd -> cmd.getHostConfig().withCpusetCpus(RuntimeUtil.getNonApplicationCores()))
        .withCommand(String.format("python3 manage.py runserver 0.0.0.0:%s --noreload", PORT));
  }
}
