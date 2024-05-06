/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package io.opentelemetry.distros;

import java.util.Collections;
import java.util.Map;

public enum DistroConfig {
  NONE(
      "no distro at all",
      false,
      Collections.EMPTY_MAP,
      "performance-test/simple-requests-service-adot"),
//  OTEL_100(
//      "OTEL distro with 100% sampling",
//      true,
//      Map.of("OTEL_TRACES_SAMPLER", "traceidratio", "OTEL_TRACES_SAMPLER_ARG", "1"),
//      "performance-test/simple-requests-service-otel"),
  ADOT_100(
      "ADOT distro with Application Signals disabled, 100% sampling",
      true,
      Map.of(
          "OTEL_TRACES_SAMPLER",
          "traceidratio",
          "OTEL_TRACES_SAMPLER_ARG",
          "1",
          "OTEL_PYTHON_DISTRO",
          "aws_distro",
          "OTEL_PYTHON_CONFIGURATOR",
          "aws_configurator"),
      "performance-test/simple-requests-service-adot"),
  AS_100(
      "ADOT distro with Application Signals enabled, 100% sampling",
      true,
      Map.of(
          "OTEL_TRACES_SAMPLER",
          "traceidratio",
          "OTEL_TRACES_SAMPLER_ARG",
          "1",
          "OTEL_PYTHON_DISTRO",
          "aws_distro",
          "OTEL_PYTHON_CONFIGURATOR",
          "aws_configurator",
          "OTEL_AWS_APPLICATION_SIGNALS_ENABLED",
          "true",
          "OTEL_AWS_APPLICATION_SIGNALS_EXPORTER_ENDPOINT",
          "http://collector:4318/v1/metrics"),
      "performance-test/simple-requests-service-adot");

  private final String description;
  private final boolean doInstrument;
  private final Map<String, String> additionalEnvVars;
  private final String imageName;

  DistroConfig(
      String description,
      boolean doInstrument,
      Map<String, String> additionalEnvVars,
      String imageName) {
    this.description = description;
    this.doInstrument = doInstrument;
    this.additionalEnvVars = additionalEnvVars;
    this.imageName = imageName;
  }

  public String getName() {
    return this.name();
  }

  public String getDescription() {
    return description;
  }

  public boolean doInstrument() {
    return doInstrument;
  }

  public Map<String, String> getAdditionalEnvVars() {
    return Collections.unmodifiableMap(additionalEnvVars);
  }

  public String getImageName() {
    return imageName;
  }
}
