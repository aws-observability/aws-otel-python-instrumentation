/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.containers;

import io.opentelemetry.distros.DistroConfig;
import io.opentelemetry.util.NamingConventions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class VehicleInventoryServiceContainer {

  private static final Logger logger =
      LoggerFactory.getLogger(VehicleInventoryServiceContainer.class);
  private static final int PORT = 8001;

  private final Network network;
  private final Startable collector;
  private final DistroConfig distroConfig;
  private final NamingConventions namingConventions;

  public VehicleInventoryServiceContainer(
      Network network,
      Startable collector,
      DistroConfig distroConfig,
      NamingConventions namingConventions) {
    this.network = network;
    this.collector = collector;
    this.distroConfig = distroConfig;
    this.namingConventions = namingConventions;
  }

  public GenericContainer<?> build() {
    GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse("performance-test/vehicle-inventory-service"))
            .withNetwork(network)
            .withNetworkAliases("vehicle-service")
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .withExposedPorts(PORT)
            .waitingFor(Wait.forHttp("/vehicle-inventory/health-check").forPort(PORT))
            .withFileSystemBind(
                namingConventions.localResults(), namingConventions.containerResults())
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("runVehicleInventory.sh"),
                "vehicle-inventory-app/run.sh")
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("profiler.py"),
                "vehicle-inventory-app/profiler.py")
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("executeProfiler.sh"),
                "vehicle-inventory-app/executeProfiler.sh")
            .withEnv("DJANGO_SETTINGS_MODULE", "VehicleInventoryApp.settings")
            .withEnv("PORT", Integer.toString(PORT))
            .withEnv("POSTGRES_DATABASE", PostgresContainer.DATABASE_NAME)
            .withEnv("POSTGRES_USER", PostgresContainer.USERNAME)
            .withEnv("POSTGRES_PASSWORD", PostgresContainer.PASSWORD)
            .withEnv("DB_SERVICE_HOST", PostgresContainer.NETWORK_ALIAS)
            .withEnv("DB_SERVICE_PORT", Integer.toString(PostgreSQLContainer.POSTGRESQL_PORT))
            .withEnv("IMAGE_BACKEND_SERVICE_HOST", ImageServiceContainer.NETWORK_ALIAS)
            .withEnv("IMAGE_BACKEND_SERVICE_PORT", Integer.toString(ImageServiceContainer.PORT))
            .withEnv(distroConfig.getAdditionalEnvVars())
            .dependsOn(collector)
            .withCommand("bash run.sh");

    if (distroConfig.doInstrument()) {
      container
          .withEnv("DO_INSTRUMENT", "true")
          .withEnv("OTEL_TRACES_EXPORTER", "otlp")
          .withEnv("OTEL_METRICS_EXPORTER", "none")
          .withEnv("OTEL_IMR_EXPORT_INTERVAL", "5000")
          .withEnv("OTEL_EXPORTER_OTLP_INSECURE", "true")
          .withEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")
          .withEnv("OTEL_RESOURCE_ATTRIBUTES", "service.name=vehicle_inventory_service");
    }
    return container;
  }
}
