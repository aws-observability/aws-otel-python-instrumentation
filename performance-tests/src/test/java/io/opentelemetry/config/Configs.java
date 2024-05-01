/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry.config;

import io.opentelemetry.distros.DistroConfig;
import java.util.Arrays;
import java.util.stream.Stream;

/** Defines all test configurations */
public enum Configs {
  ALL_TPS(buildConfig(System.getenv("TPS")));

  public final TestConfig config;

  public static Stream<TestConfig> all() {
    return Arrays.stream(Configs.values()).map(x -> x.config);
  }

  private static TestConfig buildConfig(String tps) {
    return TestConfig.builder()
        .name(String.format("%s-tps", tps))
        .description(String.format("Compares all DistroConfigs (%sTPS test)", tps))
        .withDistroConfigs(DistroConfig.values())
        .warmupSeconds(10)
        .maxRequestRate(tps)
        .duration(System.getenv("DURATION") + "s")
        .concurrentConnections(System.getenv("CONCURRENCY"))
        .build();
  }

  Configs(TestConfig config) {
    this.config = config;
  }
}
