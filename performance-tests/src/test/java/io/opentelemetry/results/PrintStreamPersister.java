/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry.results;

import io.opentelemetry.config.TestConfig;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

class PrintStreamPersister implements ResultsPersister {

  private final PrintStream out;

  public PrintStreamPersister(PrintStream out) {
    this.out = out;
  }

  @Override
  public void write(List<AppPerfResults> results) {
    TestConfig config = results.stream().findFirst().get().config;
    out.println("----------------------------------------------------------");
    out.println(" Run at " + new Date());
    out.printf(" %s : %s\n", config.getName(), config.getDescription());
    out.printf(" %d users, %s duration\n", config.getConcurrentConnections(), config.getDuration());
    out.println("----------------------------------------------------------");

    display(results, "DistroConfig", appPerfResults -> appPerfResults.distroConfig.getName());
    display(
        results,
        "Run duration",
        res -> {
          Duration duration = Duration.ofMillis(res.runDurationMs);
          return String.format(
              "%02d:%02d:%02d",
              duration.toHours(), duration.toMinutesPart(), duration.toSecondsPart());
        });
    display(results, "Startup time (ms)", res -> String.valueOf(res.startupDurationMs));
    display(results, "Req. Count", res -> format(res.requestCount));
    display(results, "Req. Rate", res -> format(res.requestRate));
    display(results, "Req. Lat. mean  (ms)", res -> format(res.requestLatencyAvg));
    display(results, "Req. Lat. p0    (ms)", res -> format(res.requestLatencyP0));
    display(results, "Req. Lat. p50   (ms)", res -> format(res.requestLatencyP50));
    display(results, "Req. Lat. p90   (ms)", res -> format(res.requestLatencyP90));
    display(results, "Req. Lat. p91   (ms)", res -> format(res.requestLatencyP91));
    display(results, "Req. Lat. p92   (ms)", res -> format(res.requestLatencyP92));
    display(results, "Req. Lat. p93   (ms)", res -> format(res.requestLatencyP93));
    display(results, "Req. Lat. p94   (ms)", res -> format(res.requestLatencyP94));
    display(results, "Req. Lat. p95   (ms)", res -> format(res.requestLatencyP95));
    display(results, "Req. Lat. p96   (ms)", res -> format(res.requestLatencyP96));
    display(results, "Req. Lat. p97   (ms)", res -> format(res.requestLatencyP97));
    display(results, "Req. Lat. p98   (ms)", res -> format(res.requestLatencyP98));
    display(results, "Req. Lat. p99   (ms)", res -> format(res.requestLatencyP99));
    display(results, "Req. Lat. p99.9 (ms)", res -> format(res.requestLatencyP999));
    display(results, "Req. Lat. p100  (ms)", res -> format(res.requestLatencyP100));
    display(results, "Net Sent mean (B)", res -> format(res.networkBytesSentAvg));
    display(results, "Net Sent p0 (B)", res -> format(res.networkBytesSentP0));
    display(results, "Net Sent p50 (B)", res -> format(res.networkBytesSentP50));
    display(results, "Net Sent p90 (B)", res -> format(res.networkBytesSentP90));
    display(results, "Net Sent p99 (B)", res -> format(res.networkBytesSentP99));
    display(results, "Net Sent p100 (B)", res -> format(res.networkBytesSentP100));
    display(results, "Net Recv mean (B)", res -> format(res.networkBytesRecvAvg));
    display(results, "Net Recv p0 (B)", res -> format(res.networkBytesRecvP0));
    display(results, "Net Recv p50 (B)", res -> format(res.networkBytesRecvP50));
    display(results, "Net Recv p90 (B)", res -> format(res.networkBytesRecvP90));
    display(results, "Net Recv p99 (B)", res -> format(res.networkBytesRecvP99));
    display(results, "Net Recv p100 (B)", res -> format(res.networkBytesRecvP100));
    display(results, "CPU Usage mean %", res -> format(res.cpuAvg));
    display(results, "CPU Usage p0 %", res -> format(res.cpuP0));
    display(results, "CPU Usage p50 %", res -> format(res.cpuP50));
    display(results, "CPU Usage p90 %", res -> format(res.cpuP90));
    display(results, "CPU Usage p99 %", res -> format(res.cpuP99));
    display(results, "CPU Usage p100 %", res -> format(res.cpuP100));
    display(results, "RSS Mem mean (MB)", res -> format(res.bytesToMegs(res.rssMemAvg)));
    display(results, "RSS Mem p0 (MB)", res -> format(res.bytesToMegs(res.rssMemP0)));
    display(results, "RSS Mem p50 (MB)", res -> format(res.bytesToMegs(res.rssMemP50)));
    display(results, "RSS Mem p90 (MB)", res -> format(res.bytesToMegs(res.rssMemP90)));
    display(results, "RSS Mem p99 (MB)", res -> format(res.bytesToMegs(res.rssMemP99)));
    display(results, "RSS Mem p100 (MB)", res -> format(res.bytesToMegs(res.rssMemP100)));
    display(results, "VMS Mem mean (MB)", res -> format(res.bytesToMegs(res.vmsMemAvg)));
    display(results, "VMS Mem p0 (MB)", res -> format(res.bytesToMegs(res.vmsMemP0)));
    display(results, "VMS Mem p50 (MB)", res -> format(res.bytesToMegs(res.vmsMemP50)));
    display(results, "VMS Mem p90 (MB)", res -> format(res.bytesToMegs(res.vmsMemP90)));
    display(results, "VMS Mem p99 (MB)", res -> format(res.bytesToMegs(res.vmsMemP99)));
    display(results, "VMS Mem p100 (MB)", res -> format(res.bytesToMegs(res.vmsMemP100)));
    display(results, "Peak threads", res -> String.valueOf(res.peakThreadCount));
  }

  private void display(
      List<AppPerfResults> results, String pref, Function<AppPerfResults, String> vs) {
    out.printf("%-30s: ", pref);
    results.forEach(
        result -> {
          out.printf("%25s", vs.apply(result));
        });
    out.println();
  }

  private String format(double d) {
    return String.format("%.2f", d);
  }
}
