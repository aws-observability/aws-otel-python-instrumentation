/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.distros;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class AgentResolver {

  private final LatestAgentSnapshotResolver snapshotResolver = new LatestAgentSnapshotResolver();

  public Optional<Path> resolve(DistroConfig distroConfig) throws Exception {
    if (DistroConfig.NONE.equals(distroConfig)) {
      return Optional.empty();
    }
    if (DistroConfig.LATEST_SNAPSHOT.equals(distroConfig)) {
      return snapshotResolver.resolve();
    }
    if (distroConfig.hasUrl()) {
      return Optional.of(downloadAgent(distroConfig.getUrl()));
    }
    throw new IllegalArgumentException("Unknown distroConfig: " + distroConfig);
  }

  private Path downloadAgent(URL agentUrl) throws Exception {
    if (agentUrl.getProtocol().equals("file")) {
      Path source = Path.of(agentUrl.toURI());
      Path result = Paths.get(".", source.getFileName().toString());
      Files.copy(source, result, StandardCopyOption.REPLACE_EXISTING);
      return result;
    }
    Request request = new Request.Builder().url(agentUrl).build();
    OkHttpClient client = new OkHttpClient();
    Response response = client.newCall(request).execute();
    byte[] raw = response.body().bytes();
    Path path = Paths.get(".", "opentelemetry-javaagent.jar");
    Files.write(
        path,
        raw,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING);
    return path;
  }
}
