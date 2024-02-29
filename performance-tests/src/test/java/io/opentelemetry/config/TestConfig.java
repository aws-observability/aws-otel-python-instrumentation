/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry.config;

import io.opentelemetry.distros.DistroConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Defines a test config. */
public class TestConfig {

  private static final int DEFAULT_MAX_REQUEST_RATE = 0; // none
  private static final int DEFAULT_CONCURRENT_CONNECTIONS = 5;
  private static final int DEFAULT_TOTAL_ITERATIONS = 5000;

  private final String name;
  private final String description;
  private final List<DistroConfig> distroConfigs;
  private final int maxRequestRate;
  private final int concurrentConnections;
  private final int totalIterations;
  private final int warmupSeconds;

  public TestConfig(Builder builder) {
    this.name = builder.name;
    this.description = builder.description;
    this.distroConfigs = Collections.unmodifiableList(builder.distroConfigs);
    this.maxRequestRate = builder.maxRequestRate;
    this.concurrentConnections = builder.concurrentConnections;
    this.totalIterations = builder.totalIterations;
    this.warmupSeconds = builder.warmupSeconds;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public List<DistroConfig> getDistroConfigs() {
    return Collections.unmodifiableList(distroConfigs);
  }

  public int getMaxRequestRate() {
    return maxRequestRate;
  }

  public int getConcurrentConnections() {
    return concurrentConnections;
  }

  public int getTotalIterations() {
    return totalIterations;
  }

  public int getWarmupSeconds() {
    return warmupSeconds;
  }

  public static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private String name;
    private String description;
    private List<DistroConfig> distroConfigs = new ArrayList<>();
    private int maxRequestRate = DEFAULT_MAX_REQUEST_RATE;
    private int concurrentConnections = DEFAULT_CONCURRENT_CONNECTIONS;
    private int totalIterations = DEFAULT_TOTAL_ITERATIONS;
    public int warmupSeconds = 0;

    Builder name(String name) {
      this.name = name;
      return this;
    }

    Builder description(String description) {
      this.description = description;
      return this;
    }

    Builder withDistroConfigs(DistroConfig... distroConfigs) {
      this.distroConfigs.addAll(Arrays.asList(distroConfigs));
      return this;
    }

    Builder maxRequestRate(int maxRequestRate) {
      this.maxRequestRate = maxRequestRate;
      return this;
    }

    Builder concurrentConnections(int concurrentConnections) {
      this.concurrentConnections = concurrentConnections;
      return this;
    }

    Builder totalIterations(int totalIterations) {
      this.totalIterations = totalIterations;
      return this;
    }

    Builder warmupSeconds(int warmupSeconds) {
      this.warmupSeconds = warmupSeconds;
      return this;
    }

    TestConfig build() {
      return new TestConfig(this);
    }
  }
}
