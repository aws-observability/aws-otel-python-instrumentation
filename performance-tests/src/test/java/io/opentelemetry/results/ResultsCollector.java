/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.results;

import com.jayway.jsonpath.JsonPath;
import io.opentelemetry.config.TestConfig;
import io.opentelemetry.distros.DistroConfig;
import io.opentelemetry.results.AppPerfResults.MinMax;
import io.opentelemetry.util.JFRUtils;
import io.opentelemetry.util.NamingConvention;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ResultsCollector {

  private final NamingConvention namingConvention;
  private final Map<String, Long> runDurations;

  public ResultsCollector(NamingConvention namingConvention, Map<String, Long> runDurations) {
    this.namingConvention = namingConvention;
    this.runDurations = runDurations;
  }

  public List<AppPerfResults> collect(TestConfig config) {
    return config.getDistroConfigs().stream()
        .map(a -> readDistroConfigResults(a, config))
        .collect(Collectors.toList());
  }

  private AppPerfResults readDistroConfigResults(DistroConfig distroConfig, TestConfig config) {
    try {
      AppPerfResults.Builder builder =
          AppPerfResults.builder()
              .distroConfig(distroConfig)
              .runDurationMs(runDurations.get(distroConfig.getName()))
              .config(config);

      builder = addStartupTime(builder, distroConfig);
      builder = addK6Results(builder, distroConfig);
      builder = addProfilerResults(builder, distroConfig);

      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException("Error reading results", e);
    }
  }

  private AppPerfResults.Builder addStartupTime(
      AppPerfResults.Builder builder, DistroConfig distroConfig) throws IOException {
    Path file = namingConvention.startupDurationFile(distroConfig);
    long startupDuration = Long.parseLong(new String(Files.readAllBytes(file)).trim());
    return builder.startupDurationMs(startupDuration);
  }

  private AppPerfResults.Builder addK6Results(
      AppPerfResults.Builder builder, DistroConfig distroConfig) throws IOException {
    Path k6File = namingConvention.k6Results(distroConfig);
    String json = new String(Files.readAllBytes(k6File));
    double iterationAvg = read(json, "$.metrics.iteration_duration.avg");
    double iterationP95 = read(json, "$.metrics.iteration_duration['p(95)']");
    double requestAvg = read(json, "$.metrics.http_req_duration.avg");
    double requestP95 = read(json, "$.metrics.http_req_duration['p(95)']");
    return builder
        .iterationAvg(iterationAvg)
        .iterationP95(iterationP95)
        .requestAvg(requestAvg)
        .requestP95(requestP95);
  }

  private static double read(String json, String jsonPath) {
    // JsonPath.read returns either Double or BigDecimal
    Number result = JsonPath.read(json, jsonPath);
    return result.doubleValue();
  }

  // TODO: Clean Up
  private AppPerfResults.Builder addJfrResults(
      AppPerfResults.Builder builder, DistroConfig distroConfig) throws IOException {
    Path jfrFile = namingConvention.jfrFile(distroConfig);
    return builder
        .totalGCTime(readTotalGCTime(jfrFile))
        .totalAllocated(readTotalAllocated(jfrFile))
        .heapUsed(readHeapUsed(jfrFile))
        .maxThreadContextSwitchRate(readMaxThreadContextSwitchRate(jfrFile))
        .peakThreadCount(readPeakThreadCount(jfrFile))
        .averageNetworkRead(computeAverageNetworkRead(jfrFile))
        .averageNetworkWrite(computeAverageNetworkWrite(jfrFile))
        .averageJvmUserCpu(computeAverageJvmUserCpu(jfrFile))
        .maxJvmUserCpu(computeMaxJvmUserCpu(jfrFile))
        .averageMachineCpuTotal(computeAverageMachineCpuTotal(jfrFile))
        .totalGcPauseNanos(computeTotalGcPauseNanos(jfrFile));
  }

  private AppPerfResults.Builder addProfilerResults(
          AppPerfResults.Builder builder, DistroConfig distroConfig) throws IOException {
    Path performanceMetricsFile = namingConvention.performanceMetricsFile(distroConfig);
    String json = new String(Files.readAllBytes(performanceMetricsFile));
    double peakThreads = read(json, "$.peak_threads");
    double minRSSMem = read(json, "$.min_rss_mem");
    double maxRSSMem = read(json, "$.max_rss_mem");
    double minVMSMem = read(json, "$.min_vms_mem");
    double maxVMSMem = read(json, "$.max_vms_mem");
    double averageCpu = read(json, "$.avg_cpu");
    double maxCPU = read(json, "$.max_cpu");

    return builder
        .peakThreadCount((long) peakThreads)
        .minRSSMem((long) minRSSMem)
        .maxRSSMem((long) maxRSSMem)
        .minVMSMem((long) minVMSMem)
        .maxVMSMem((long) maxVMSMem)
        .averageCpu(averageCpu)
        .maxCpu(maxCPU);
  }

  // TODO: Clean up.
  private long readTotalGCTime(Path jfrFile) throws IOException {
    return JFRUtils.sumLongEventValues(jfrFile, "jdk.G1GarbageCollection", "duration");
  }

  private long readTotalAllocated(Path jfrFile) throws IOException {
    return JFRUtils.sumLongEventValues(jfrFile, "jdk.ThreadAllocationStatistics", "allocated");
  }

  private MinMax readHeapUsed(Path jfrFile) throws IOException {
    return JFRUtils.findMinMax(jfrFile, "jdk.GCHeapSummary", "heapUsed");
  }

  private float readMaxThreadContextSwitchRate(Path jfrFile) throws IOException {
    return JFRUtils.findMaxFloat(jfrFile, "jdk.ThreadContextSwitchRate", "switchRate");
  }

  private long readPeakThreadCount(Path jfrFile) throws IOException {
    MinMax minMax = JFRUtils.findMinMax(jfrFile, "jdk.JavaThreadStatistics", "peakCount");
    return minMax.max;
  }

  private long computeAverageNetworkRead(Path jfrFile) throws IOException {
    return JFRUtils.findAverageLong(jfrFile, "jdk.NetworkUtilization", "readRate");
  }

  private long computeAverageNetworkWrite(Path jfrFile) throws IOException {
    return JFRUtils.findAverageLong(jfrFile, "jdk.NetworkUtilization", "writeRate");
  }

  private float computeAverageJvmUserCpu(Path jfrFile) throws IOException {
    return JFRUtils.computeAverageFloat(jfrFile, "jdk.CPULoad", "jvmUser");
  }

  private float computeMaxJvmUserCpu(Path jfrFile) throws IOException {
    return JFRUtils.findMaxFloat(jfrFile, "jdk.CPULoad", "jvmUser");
  }

  private float computeAverageMachineCpuTotal(Path jfrFile) throws IOException {
    return JFRUtils.computeAverageFloat(jfrFile, "jdk.CPULoad", "machineTotal");
  }

  private long computeTotalGcPauseNanos(Path jfrFile) throws IOException {
    return JFRUtils.sumLongEventValues(jfrFile, "jdk.GCPhasePause", "duration");
  }
}
