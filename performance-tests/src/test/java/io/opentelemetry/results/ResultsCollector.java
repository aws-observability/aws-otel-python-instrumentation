/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry.results;

import com.jayway.jsonpath.JsonPath;
import io.opentelemetry.config.TestConfig;
import io.opentelemetry.distros.DistroConfig;
import io.opentelemetry.results.AppPerfResults.MinMax;
import io.opentelemetry.util.JFRUtils;
import io.opentelemetry.util.NamingConvention;
import io.opentelemetry.util.ProfilerUtils;
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
    double requestCount = read(json, "$.metrics.http_reqs.count");
    double requestRate = read(json, "$.metrics.http_reqs.rate");
    double requestLatencyAvg = read(json, "$.metrics.http_req_duration.avg");
    double requestLatencyP0 = read(json, "$.metrics.http_req_duration['p(0)']");
    double requestLatencyP50 = read(json, "$.metrics.http_req_duration['p(50)']");
    double requestLatencyP90 = read(json, "$.metrics.http_req_duration['p(90)']");
    double requestLatencyP99 = read(json, "$.metrics.http_req_duration['p(99)']");
    double requestLatencyP100 = read(json, "$.metrics.http_req_duration['p(100)']");
    builder.requestCount = requestCount;
    builder.requestRate = requestRate;
    builder.requestLatencyAvg = requestLatencyAvg;
    builder.requestLatencyP0 = requestLatencyP0;
    builder.requestLatencyP50 = requestLatencyP50;
    builder.requestLatencyP90 = requestLatencyP90;
    builder.requestLatencyP99 = requestLatencyP99;
    builder.requestLatencyP100 = requestLatencyP100;
    return builder;
  }

  private static double read(String json, String jsonPath) {
    // JsonPath.read returns either Double or BigDecimal
    Number result = JsonPath.read(json, jsonPath);
    return result.doubleValue();
  }

  private static List<Number> readArray(String json, String jsonPath) {
    List<Number> result = JsonPath.read(json, jsonPath);
    return result;
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
    long peakThreads = (long) read(json, "$.peak_threads");
    List<Number> networkBytesSent = readArray(json, "$.network_bytes_sent");
    List<Number> networkBytesRecv = readArray(json, "$.network_bytes_recv");
    List<Number> cpuUsage = readArray(json, "$.cpu_usage");
    List<Number> rssMem = readArray(json, "$.rss_mem");
    List<Number> vmsMem = readArray(json, "$.vms_mem");

    double[] percentiles = new double[] {0, 50, 90, 99, 100};
    long[] networkBytesSentPercentile =
        ProfilerUtils.computeLongPercentiles(networkBytesSent, percentiles);
    long[] networkBytesRecvPercentile =
        ProfilerUtils.computeLongPercentiles(networkBytesRecv, percentiles);
    double[] cpuUsagePercentile = ProfilerUtils.computeDoublePercentiles(cpuUsage, percentiles);
    long[] rssMemPercentile = ProfilerUtils.computeLongPercentiles(rssMem, percentiles);
    long[] vmsMemPercentile = ProfilerUtils.computeLongPercentiles(vmsMem, percentiles);

    builder.networkBytesSentAvg = ProfilerUtils.computeLongAverage(networkBytesSent);
    builder.networkBytesSentP0 = networkBytesSentPercentile[0];
    builder.networkBytesSentP50 = networkBytesSentPercentile[1];
    builder.networkBytesSentP90 = networkBytesSentPercentile[2];
    builder.networkBytesSentP99 = networkBytesSentPercentile[3];
    builder.networkBytesSentP100 = networkBytesSentPercentile[4];
    builder.networkBytesRecvAvg = ProfilerUtils.computeLongAverage(networkBytesRecv);
    builder.networkBytesRecvP0 = networkBytesRecvPercentile[0];
    builder.networkBytesRecvP50 = networkBytesRecvPercentile[1];
    builder.networkBytesRecvP90 = networkBytesRecvPercentile[2];
    builder.networkBytesRecvP99 = networkBytesRecvPercentile[3];
    builder.networkBytesRecvP100 = networkBytesRecvPercentile[4];
    builder.cpuAvg = ProfilerUtils.computeDoubleAverage(cpuUsage);
    builder.cpuP0 = cpuUsagePercentile[0];
    builder.cpuP50 = cpuUsagePercentile[1];
    builder.cpuP90 = cpuUsagePercentile[2];
    builder.cpuP99 = cpuUsagePercentile[3];
    builder.cpuP100 = cpuUsagePercentile[4];
    builder.rssMemAvg = ProfilerUtils.computeLongAverage(rssMem);
    ;
    builder.rssMemP0 = rssMemPercentile[0];
    builder.rssMemP50 = rssMemPercentile[1];
    builder.rssMemP90 = rssMemPercentile[2];
    builder.rssMemP99 = rssMemPercentile[3];
    builder.rssMemP100 = rssMemPercentile[4];
    builder.vmsMemAvg = ProfilerUtils.computeLongAverage(vmsMem);
    builder.vmsMemP0 = vmsMemPercentile[0];
    builder.vmsMemP50 = vmsMemPercentile[1];
    builder.vmsMemP90 = vmsMemPercentile[2];
    builder.vmsMemP99 = vmsMemPercentile[3];
    builder.vmsMemP100 = vmsMemPercentile[4];
    builder.peakThreadCount = peakThreads;
    return builder;
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
