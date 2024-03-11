/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ProfilerUtils {

  public static long computeLongAverage(List<Number> values) {
    AverageSupport support = new AverageSupport();
    values.forEach(value -> support.add(value.longValue()));
    return support.average();
  }

  public static double computeDoubleAverage(List<Number> values) {
    AverageDoubleSupport support = new AverageDoubleSupport();
    values.forEach(value -> support.add(value.doubleValue()));
    return support.average();
  }

  public static long[] computeLongPercentiles(List<Number> values, double[] percentiles) {
    PercentileSupport support = new PercentileSupport();
    values.forEach(value -> support.add(value.longValue()));
    support.precompute();
    long[] percentileValues = new long[percentiles.length];
    for (int i = 0; i < percentiles.length; i++) {
      percentileValues[i] = support.percentile(percentiles[i]);
    }
    return percentileValues;
  }

  public static double[] computeDoublePercentiles(List<Number> values, double[] percentiles) {
    PercentileDoubleSupport support = new PercentileDoubleSupport();
    values.forEach(value -> support.add(value.doubleValue()));
    support.precompute();
    double[] percentileValues = new double[percentiles.length];
    for (int i = 0; i < percentiles.length; i++) {
      percentileValues[i] = support.percentile(percentiles[i]);
    }
    return percentileValues;
  }

  static class AverageSupport {
    long count;
    long total;

    AverageSupport add(long value) {
      count++;
      total += value;
      return this;
    }

    long average() {
      if (count == 0) return -1;
      return total / count;
    }
  }

  static class AverageDoubleSupport {
    double count;
    double total;

    AverageDoubleSupport add(double value) {
      count++;
      total += value;
      return this;
    }

    double average() {
      if (count == 0) return -1;
      return total / count;
    }
  }

  static class PercentileSupport {
    List<Long> longList = new ArrayList<>();

    PercentileSupport add(long value) {
      longList.add(value);
      return this;
    }

    void precompute() {
      Collections.sort(longList);
    }

    long percentile(double percentile) {
      int index = (int) Math.max(0, Math.ceil(percentile / 100.0 * longList.size()) - 1);
      return longList.get(index);
    }
  }

  static class PercentileDoubleSupport {
    List<Double> doubleList = new ArrayList<>();

    PercentileDoubleSupport add(double value) {
      doubleList.add(value);
      return this;
    }

    void precompute() {
      Collections.sort(doubleList);
    }

    double percentile(double percentile) {
      int index = (int) Math.max(0, Math.ceil(percentile / 100.0 * doubleList.size()) - 1);
      return doubleList.get(index);
    }
  }
}
