/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuntimeUtil {
  private static final Logger logger = LoggerFactory.getLogger(RuntimeUtil.class);
  private static final int CORE_COUNT = getCpuCoreCount();

  /**
   * Get cores dedicated for application container being performance tested, to reduce chances of
   * CPU resource contention causing application slowdown. E.g. if 6 core system, "3-5"
   */
  public static String getApplicationCores() {
    String cpus = String.format("%s-%s", CORE_COUNT / 2, CORE_COUNT - 1);
    logger.info(String.format("App Cores: %s.", cpus));
    return cpus;
  }

  /** Get cores for all other containers. E.g. if 6 core system, "0-2" */
  public static String getNonApplicationCores() {
    String cpus = String.format("0-%s", CORE_COUNT / 2 - 1);
    logger.info(String.format("Non-App Cores: %s.", cpus));
    return cpus;
  }

  private static int getCpuCoreCount() {
    String os = System.getProperty("os.name").toLowerCase();
    try {
      if (os.contains("mac")) {
        logger.info("Detected running on MacOS.");
        return getCoreCountMac();
      } else {
        logger.info("Assume running on Linux.");
        return getCoreCountLinux();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static int getCoreCountMac() throws Exception {
    String command = "sysctl -n machdep.cpu.core_count";
    String[] cmd = {"/bin/sh", "-c", command};
    Process process = Runtime.getRuntime().exec(cmd);
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line = reader.readLine();
    return !line.isEmpty() ? Integer.parseInt(line) : 0;
  }

  private static int getCoreCountLinux() throws Exception {
    String command = "lscpu";
    Process process = Runtime.getRuntime().exec(command);
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line = reader.readLine();
    int threadsPerCore = 0;
    int coresPerSocket = 0;
    int sockets = 0;
    while (line != null) {
      if (line.contains("Thread(s) per core:")) {
        threadsPerCore = Integer.parseInt(line.split("\\s+")[line.split("\\s+").length - 1]);
      }
      if (line.contains("Core(s) per socket:")) {
        coresPerSocket = Integer.parseInt(line.split("\\s+")[line.split("\\s+").length - 1]);
      }
      if (line.contains("Socket(s):")) {
        sockets = Integer.parseInt(line.split("\\s+")[line.split("\\s+").length - 1]);
      }
      line = reader.readLine();
    }
    return threadsPerCore * coresPerSocket * sockets;
  }
}
