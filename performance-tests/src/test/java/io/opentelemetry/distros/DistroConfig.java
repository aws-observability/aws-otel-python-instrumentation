/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.distros;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DistroConfig {

  static final String OTEL_LATEST =
      "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar";

  public static final DistroConfig NONE = new DistroConfig("none", "no distro at all");
  public static final DistroConfig LATEST_RELEASE =
      new DistroConfig("latest", "latest mainstream release", OTEL_LATEST);
  public static final DistroConfig LATEST_SNAPSHOT =
      new DistroConfig("snapshot", "latest available snapshot version from main");

  private final String name;
  private final String description;
  private final URL url;
  private final List<String> additionalJvmArgs;

  public DistroConfig(String name, String description) {
    this(name, description, null);
  }

  public DistroConfig(String name, String description, String url) {
    this(name, description, url, Collections.emptyList());
  }

  public DistroConfig(String name, String description, String url, List<String> additionalJvmArgs) {
    this.name = name;
    this.description = description;
    this.url = makeUrl(url);
    this.additionalJvmArgs = new ArrayList<>(additionalJvmArgs);
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public boolean hasUrl() {
    return url != null;
  }

  public URL getUrl() {
    return url;
  }

  public List<String> getAdditionalJvmArgs() {
    return Collections.unmodifiableList(additionalJvmArgs);
  }

  private static URL makeUrl(String url) {
    try {
      if (url == null) {
        return null;
      }
      return URI.create(url).toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException("Error parsing url", e);
    }
  }
}
