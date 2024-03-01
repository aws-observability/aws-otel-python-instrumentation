/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.util;

import io.opentelemetry.distros.DistroConfig;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This utility class provides the standard file naming conventions, primarily for files that are
 * shared between containers and the test runner. It consolidates the naming logic into one place to
 * ensure consistency, reduce duplication, and decrease errors.
 */
public class NamingConvention {

  private final String dir;

  public NamingConvention(String dir) {
    this.dir = dir;
  }

  /**
   * Returns a path to the location of the k6 results json file.
   *
   * @param distroConfig The distroConfig to get results file path for
   */
  public Path k6Results(DistroConfig distroConfig) {
    return Paths.get(dir, "k6_out_" + distroConfig.getName() + ".json");
  }

  /**
   * Returns a path to the location of the jfr output file for a given distroConfig run.
   *
   * @param distroConfig The distroConfig to get the jfr file path for.
   */
  // TODO: Clean up
  public Path jfrFile(DistroConfig distroConfig) {
    return Paths.get(dir, "petclinic-" + distroConfig.getName() + ".jfr");
  }

  /**
   * Returns a path to the location of the jfr output file for a given distroConfig run.
   *
   * @param distroConfig The distroConfig to get the jfr file path for.
   */
  public Path performanceMetricsFile(DistroConfig distroConfig) {
    return Paths.get(dir, "performance-metrics-" + distroConfig.getName() + ".json");
  }

  /**
   * Returns the path to the file that contains the startup duration for a given distroConfig run.
   *
   * @param distroConfig The distroConfig to get the startup duration for.
   */
  public Path startupDurationFile(DistroConfig distroConfig) {
    return Paths.get(dir, "startup-time-" + distroConfig.getName() + ".txt");
  }

  /** Returns the root path that this naming convention was configured with. */
  public String root() {
    return dir;
  }
}
