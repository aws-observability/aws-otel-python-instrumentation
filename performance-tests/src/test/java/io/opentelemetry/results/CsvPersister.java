/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry.results;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

class CsvPersister implements ResultsPersister {

  // The fields as they are output, in order, but spread across distroConfigs
  private static final List<FieldSpec> FIELDS =
      Arrays.asList(
          FieldSpec.of("startupDurationMs", r -> r.startupDurationMs),
          FieldSpec.of("requestCount", r -> r.requestCount),
          FieldSpec.of("requestRate", r -> r.requestRate),
          FieldSpec.of("requestLatencyAvg", r -> r.requestLatencyAvg),
          FieldSpec.of("requestLatencyP0", r -> r.requestLatencyP0),
          FieldSpec.of("requestLatencyP50", r -> r.requestLatencyP50),
          FieldSpec.of("requestLatencyP90", r -> r.requestLatencyP90),
          FieldSpec.of("requestLatencyP91", r -> r.requestLatencyP91),
          FieldSpec.of("requestLatencyP92", r -> r.requestLatencyP92),
          FieldSpec.of("requestLatencyP93", r -> r.requestLatencyP93),
          FieldSpec.of("requestLatencyP94", r -> r.requestLatencyP94),
          FieldSpec.of("requestLatencyP95", r -> r.requestLatencyP95),
          FieldSpec.of("requestLatencyP96", r -> r.requestLatencyP96),
          FieldSpec.of("requestLatencyP97", r -> r.requestLatencyP97),
          FieldSpec.of("requestLatencyP98", r -> r.requestLatencyP98),
          FieldSpec.of("requestLatencyP99", r -> r.requestLatencyP99),
          FieldSpec.of("requestLatencyP99.9", r -> r.requestLatencyP999),
          FieldSpec.of("requestLatencyP100", r -> r.requestLatencyP100),
          FieldSpec.of("networkBytesSentAvg", r -> r.networkBytesSentAvg),
          FieldSpec.of("networkBytesSentP0", r -> r.networkBytesSentP0),
          FieldSpec.of("networkBytesSentP50", r -> r.networkBytesSentP50),
          FieldSpec.of("networkBytesSentP90", r -> r.networkBytesSentP90),
          FieldSpec.of("networkBytesSentP99", r -> r.networkBytesSentP99),
          FieldSpec.of("networkBytesSentP100", r -> r.networkBytesSentP100),
          FieldSpec.of("networkBytesRecvAvg", r -> r.networkBytesRecvAvg),
          FieldSpec.of("networkBytesRecvP0", r -> r.networkBytesRecvP0),
          FieldSpec.of("networkBytesRecvP50", r -> r.networkBytesRecvP50),
          FieldSpec.of("networkBytesRecvP90", r -> r.networkBytesRecvP90),
          FieldSpec.of("networkBytesRecvP99", r -> r.networkBytesRecvP99),
          FieldSpec.of("networkBytesRecvP100", r -> r.networkBytesRecvP100),
          FieldSpec.of("cpuAvg", r -> r.cpuAvg),
          FieldSpec.of("cpuP0", r -> r.cpuP0),
          FieldSpec.of("cpuP50", r -> r.cpuP50),
          FieldSpec.of("cpuP90", r -> r.cpuP90),
          FieldSpec.of("cpuP99", r -> r.cpuP99),
          FieldSpec.of("cpuP100", r -> r.cpuP100),
          FieldSpec.of("cpuAvg", r -> r.cpuAvg),
          FieldSpec.of("cpuP0", r -> r.cpuP0),
          FieldSpec.of("cpuP50", r -> r.cpuP50),
          FieldSpec.of("cpuP90", r -> r.cpuP90),
          FieldSpec.of("cpuP99", r -> r.cpuP99),
          FieldSpec.of("cpuP100", r -> r.cpuP100),
          FieldSpec.of("rssMemAvg", r -> r.rssMemAvg),
          FieldSpec.of("rssMemP0", r -> r.rssMemP0),
          FieldSpec.of("rssMemP50", r -> r.rssMemP50),
          FieldSpec.of("rssMemP90", r -> r.rssMemP90),
          FieldSpec.of("rssMemP99", r -> r.rssMemP99),
          FieldSpec.of("rssMemP100", r -> r.rssMemP100),
          FieldSpec.of("vmsMemAvg", r -> r.vmsMemAvg),
          FieldSpec.of("vmsMemP0", r -> r.vmsMemP0),
          FieldSpec.of("vmsMemP50", r -> r.vmsMemP50),
          FieldSpec.of("vmsMemP90", r -> r.vmsMemP90),
          FieldSpec.of("vmsMemP99", r -> r.vmsMemP99),
          FieldSpec.of("vmsMemP100", r -> r.vmsMemP100),
          FieldSpec.of("peakThreadCount", r -> r.peakThreadCount),
          FieldSpec.of("runDurationMs", r -> r.runDurationMs));

  private final Path resultsFile;

  public CsvPersister(Path resultsFile) {
    this.resultsFile = resultsFile;
  }

  @Override
  public void write(List<AppPerfResults> results) {

    ensureFileCreated(results);

    StringBuilder sb = new StringBuilder().append(System.currentTimeMillis() / 1000);
    // Don't be confused by the loop -- This generates a single long csv line.
    // Each result is for a given distroConfig run, and we want all the fields for all distroConfigs
    // on the same
    // line so that we can create a columnar structure that allows us to more easily compare
    // distroConfig
    // to distroConfig for a given run.
    for (FieldSpec field : FIELDS) {
      for (AppPerfResults result : results) {
        sb.append(",").append(field.getter.apply(result));
      }
    }
    sb.append("\n");
    try {
      Files.writeString(resultsFile, sb.toString(), StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new RuntimeException("Error writing csv content", e);
    }
  }

  private void ensureFileCreated(List<AppPerfResults> results) {
    if (Files.exists(resultsFile)) {
      return;
    }
    try {
      String headerLine = createHeaderLine(results);
      Files.writeString(resultsFile, headerLine);
    } catch (IOException e) {
      throw new RuntimeException("Error creating csv output stub", e);
    }
  }

  private String createHeaderLine(List<AppPerfResults> results) {
    StringBuilder sb = new StringBuilder("timestamp");
    // Don't be confused by the loop -- This generates a single long csv line.
    // Each result is for a given distroConfig run, and we want all the fields for all distroConfigs
    // on the same
    // line so that we can create a columnar structure that allows us to more easily compare
    // distroConfig
    // to distroConfig for a given run.

    List<String> distroConfigs =
        results.stream().map(r -> r.distroConfig.getName()).collect(Collectors.toList());
    for (FieldSpec field : FIELDS) {
      for (String distroConfig : distroConfigs) {
        sb.append(",").append(distroConfig).append(':').append(field.name);
      }
    }

    sb.append("\n");
    return sb.toString();
  }

  static class FieldSpec {
    private final String name;
    private final Function<AppPerfResults, Object> getter;

    public FieldSpec(String name, Function<AppPerfResults, Object> getter) {
      this.name = name;
      this.getter = getter;
    }

    static FieldSpec of(String name, Function<AppPerfResults, Object> getter) {
      return new FieldSpec(name, getter);
    }
  }
}
