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
    out.printf(
        " %d users, %d iterations\n",
        config.getConcurrentConnections(), config.getTotalIterations());
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
    display(results, "Avg. CPU %", res -> format(res.averageCpu));
    display(results, "Max. CPU %", res -> format(res.maxCpu));
    display(results, "Startup time (ms)", res -> String.valueOf(res.startupDurationMs));
    display(results, "Min Resident Mem (MB)", res -> format(res.getMinRSSMemMB()));
    display(results, "Max Resident Mem (MB)", res -> format(res.getMaxRSSMemMB()));
    display(results, "Min Virtual Mem (MB)", res -> format(res.getMinVMSMemMB()));
    display(results, "Max Virtual Mem (MB)", res -> format(res.getMaxVMSMemMB()));
    display(results, "Req. mean (ms)", res -> format(res.requestAvg));
    display(results, "Req. p95 (ms)", res -> format(res.requestP95));
    display(results, "Iter. mean (ms)", res -> format(res.iterationAvg));
    display(results, "Iter. p95 (ms)", res -> format(res.iterationP95));
    display(results, "Peak threads", res -> String.valueOf(res.peakThreadCount));
  }

  private void display(
      List<AppPerfResults> results, String pref, Function<AppPerfResults, String> vs) {
    out.printf("%-22s: ", pref);
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
