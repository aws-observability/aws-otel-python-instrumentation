/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry.results;

import io.opentelemetry.config.TestConfig;
import io.opentelemetry.distros.DistroConfig;

public class AppPerfResults {

  final DistroConfig distroConfig;
  final TestConfig config;
  final double requestCount;
  final double requestRate;
  final double requestLatencyAvg;
  final double requestLatencyP0;
  final double requestLatencyP50;
  final double requestLatencyP90;
  final double requestLatencyP99;
  final double requestLatencyP100;
  final long networkBytesSentAvg;
  final long networkBytesSentP0;
  final long networkBytesSentP50;
  final long networkBytesSentP90;
  final long networkBytesSentP99;
  final long networkBytesSentP100;
  final long networkBytesRecvAvg;
  final long networkBytesRecvP0;
  final long networkBytesRecvP50;
  final long networkBytesRecvP90;
  final long networkBytesRecvP99;
  final long networkBytesRecvP100;
  final double cpuAvg;
  final double cpuP0;
  final double cpuP50;
  final double cpuP90;
  final double cpuP99;
  final double cpuP100;
  final long rssMemAvg;
  final long rssMemP0;
  final long rssMemP50;
  final long rssMemP90;
  final long rssMemP99;
  final long rssMemP100;
  final long vmsMemAvg;
  final long vmsMemP0;
  final long vmsMemP50;
  final long vmsMemP90;
  final long vmsMemP99;
  final long vmsMemP100;
  final long peakThreadCount;
  final long startupDurationMs;
  final long runDurationMs;
  // TODO: cleanup
  final long totalGCTime;
  final long totalAllocated;
  final MinMax heapUsed;
  final float maxThreadContextSwitchRate;
  final long averageNetworkRead;
  final long averageNetworkWrite;
  final float averageJvmUserCpu;
  final float maxJvmUserCpu;
  final float averageMachineCpuTotal;
  final long totalGcPauseNanos;

  private AppPerfResults(Builder builder) {
    this.distroConfig = builder.distroConfig;
    this.config = builder.config;
    this.requestCount = builder.requestCount;
    this.requestRate = builder.requestRate;
    this.requestLatencyAvg = builder.requestLatencyAvg;
    this.requestLatencyP0 = builder.requestLatencyP0;
    this.requestLatencyP50 = builder.requestLatencyP50;
    this.requestLatencyP90 = builder.requestLatencyP90;
    this.requestLatencyP99 = builder.requestLatencyP99;
    this.requestLatencyP100 = builder.requestLatencyP100;
    this.networkBytesSentAvg = builder.networkBytesSentAvg;
    this.networkBytesSentP0 = builder.networkBytesSentP0;
    this.networkBytesSentP50 = builder.networkBytesSentP50;
    this.networkBytesSentP90 = builder.networkBytesSentP90;
    this.networkBytesSentP99 = builder.networkBytesSentP99;
    this.networkBytesSentP100 = builder.networkBytesSentP100;
    this.networkBytesRecvAvg = builder.networkBytesRecvAvg;
    this.networkBytesRecvP0 = builder.networkBytesRecvP0;
    this.networkBytesRecvP50 = builder.networkBytesRecvP50;
    this.networkBytesRecvP90 = builder.networkBytesRecvP90;
    this.networkBytesRecvP99 = builder.networkBytesRecvP99;
    this.networkBytesRecvP100 = builder.networkBytesRecvP100;
    this.cpuAvg = builder.cpuAvg;
    this.cpuP0 = builder.cpuP0;
    this.cpuP50 = builder.cpuP50;
    this.cpuP90 = builder.cpuP90;
    this.cpuP99 = builder.cpuP99;
    this.cpuP100 = builder.cpuP100;
    this.rssMemAvg = builder.rssMemAvg;
    this.rssMemP0 = builder.rssMemP0;
    this.rssMemP50 = builder.rssMemP50;
    this.rssMemP90 = builder.rssMemP90;
    this.rssMemP99 = builder.rssMemP99;
    this.rssMemP100 = builder.rssMemP100;
    this.vmsMemAvg = builder.vmsMemAvg;
    this.vmsMemP0 = builder.vmsMemP0;
    this.vmsMemP50 = builder.vmsMemP50;
    this.vmsMemP90 = builder.vmsMemP90;
    this.vmsMemP99 = builder.vmsMemP99;
    this.vmsMemP100 = builder.vmsMemP100;
    this.peakThreadCount = builder.peakThreadCount;
    this.startupDurationMs = builder.startupDurationMs;
    this.runDurationMs = builder.runDurationMs;
    this.totalGCTime = builder.totalGCTime;
    this.totalAllocated = builder.totalAllocated;
    this.heapUsed = builder.heapUsed;
    this.maxThreadContextSwitchRate = builder.maxThreadContextSwitchRate;
    this.averageNetworkRead = builder.averageNetworkRead;
    this.averageNetworkWrite = builder.averageNetworkWrite;
    this.averageJvmUserCpu = builder.averageJvmUserCpu;
    this.maxJvmUserCpu = builder.maxJvmUserCpu;
    this.averageMachineCpuTotal = builder.averageMachineCpuTotal;
    this.totalGcPauseNanos = builder.totalGcPauseNanos;
  }

  double getTotalAllocatedMB() {
    return bytesToMegs(this.totalAllocated);
  }

  double getMinHeapUsedMB() {
    return bytesToMegs(this.heapUsed.min);
  }

  double getMaxHeapUsedMB() {
    return bytesToMegs(this.heapUsed.max);
  }

  public double bytesToMegs(long x) {
    return x / (1024.0 * 1024.0);
  }

  String getDistroConfigName() {
    return distroConfig.getName();
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private long startupDurationMs;
    private DistroConfig distroConfig;
    private TestConfig config;
    public double requestCount;
    public double requestRate;
    public double requestLatencyAvg;
    public double requestLatencyP0;
    public double requestLatencyP50;
    public double requestLatencyP90;
    public double requestLatencyP99;
    public double requestLatencyP100;
    public long networkBytesSentAvg;
    public long networkBytesSentP0;
    public long networkBytesSentP50;
    public long networkBytesSentP90;
    public long networkBytesSentP99;
    public long networkBytesSentP100;
    public long networkBytesRecvAvg;
    public long networkBytesRecvP0;
    public long networkBytesRecvP50;
    public long networkBytesRecvP90;
    public long networkBytesRecvP99;
    public long networkBytesRecvP100;
    public double cpuAvg;
    public double cpuP0;
    public double cpuP50;
    public double cpuP90;
    public double cpuP99;
    public double cpuP100;
    public long rssMemAvg;
    public long rssMemP0;
    public long rssMemP50;
    public long rssMemP90;
    public long rssMemP99;
    public long rssMemP100;
    public long vmsMemAvg;
    public long vmsMemP0;
    public long vmsMemP50;
    public long vmsMemP90;
    public long vmsMemP99;
    public long vmsMemP100;
    public long peakThreadCount;
    public long runDurationMs;
    // TODO: cleanup
    private long totalGCTime;
    private long totalAllocated;
    private MinMax heapUsed;
    private float maxThreadContextSwitchRate;
    public long averageNetworkRead;
    public long averageNetworkWrite;
    public float averageJvmUserCpu;
    public float maxJvmUserCpu;
    public float averageMachineCpuTotal;
    public long totalGcPauseNanos;

    AppPerfResults build() {
      return new AppPerfResults(this);
    }

    Builder distroConfig(DistroConfig distroConfig) {
      this.distroConfig = distroConfig;
      return this;
    }

    Builder config(TestConfig config) {
      this.config = config;
      return this;
    }

    // TODO: cleanup
    Builder totalGCTime(long totalGCTime) {
      this.totalGCTime = totalGCTime;
      return this;
    }

    Builder totalAllocated(long totalAllocated) {
      this.totalAllocated = totalAllocated;
      return this;
    }

    Builder heapUsed(MinMax heapUsed) {
      this.heapUsed = heapUsed;
      return this;
    }

    Builder maxThreadContextSwitchRate(float maxThreadContextSwitchRate) {
      this.maxThreadContextSwitchRate = maxThreadContextSwitchRate;
      return this;
    }

    Builder startupDurationMs(long startupDurationMs) {
      this.startupDurationMs = startupDurationMs;
      return this;
    }

    Builder peakThreadCount(long peakThreadCount) {
      this.peakThreadCount = peakThreadCount;
      return this;
    }

    Builder averageNetworkRead(long averageNetworkRead) {
      this.averageNetworkRead = averageNetworkRead;
      return this;
    }

    Builder averageNetworkWrite(long averageNetworkWrite) {
      this.averageNetworkWrite = averageNetworkWrite;
      return this;
    }

    Builder averageJvmUserCpu(float averageJvmUserCpu) {
      this.averageJvmUserCpu = averageJvmUserCpu;
      return this;
    }

    Builder maxJvmUserCpu(float maxJvmUserCpu) {
      this.maxJvmUserCpu = maxJvmUserCpu;
      return this;
    }

    Builder averageMachineCpuTotal(float averageMachineCpuTotal) {
      this.averageMachineCpuTotal = averageMachineCpuTotal;
      return this;
    }

    Builder runDurationMs(long runDurationMs) {
      this.runDurationMs = runDurationMs;
      return this;
    }

    Builder totalGcPauseNanos(long totalGcPauseNanos) {
      this.totalGcPauseNanos = totalGcPauseNanos;
      return this;
    }
  }

  public static class MinMax {
    public final long min;
    public final long max;

    public MinMax() {
      this(Long.MAX_VALUE, Long.MIN_VALUE);
    }

    public MinMax(long min, long max) {
      this.min = min;
      this.max = max;
    }

    public MinMax withMin(long min) {
      return new MinMax(min, max);
    }

    public MinMax withMax(long max) {
      return new MinMax(min, max);
    }
  }
}
