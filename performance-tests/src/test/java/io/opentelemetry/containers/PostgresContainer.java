/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry.containers;

import io.opentelemetry.util.RuntimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class PostgresContainer {

  private static final Logger logger = LoggerFactory.getLogger(PostgresContainer.class);
  public static final String PASSWORD = "password";
  public static final String USERNAME = "djangouser";
  public static final String DATABASE_NAME = "vehicle_inventory";
  public static final String NETWORK_ALIAS = "postgres";

  private final Network network;

  public PostgresContainer(Network network) {
    this.network = network;
  }

  public PostgreSQLContainer<?> build() throws Exception {
    return new PostgreSQLContainer<>("postgres:14.0")
        .withNetwork(network)
        .withNetworkAliases(NETWORK_ALIAS)
        .withLogConsumer(new Slf4jLogConsumer(logger))
        .withUsername(USERNAME)
        .withPassword(PASSWORD)
        .withDatabaseName(DATABASE_NAME)
        .withCreateContainerCmdModifier(
            cmd -> cmd.getHostConfig().withCpusetCpus(RuntimeUtil.getNonApplicationCores()))
        .withReuse(false);
  }
}
