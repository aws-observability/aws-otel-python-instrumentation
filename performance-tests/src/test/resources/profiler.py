# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import os
import sys
import time

import psutil

# This profiler is used to get performance metrics for python3 application during load testing to
# measure performance under different scenarios. It writes a JSON dictionary to a file. Currently, these
# are the perf metrics that are being recorded: Network Bytes Sent/Received, Peak Threads, CPU Usage,
# Resident Memory and Virtual Memory.
# Usage: python3 profiler.py <file_name.json> <dir_to_save_in>


def get_pid(process_name_substring: str) -> int:
    pid = None
    proc_name = None
    for proc in psutil.process_iter(["pid", "name"]):
        if process_name_substring in proc.name():
            pid = proc.pid
            proc_name = proc.name()
            break
    print("Pid:", pid)
    print("Process Name: ", proc_name)
    return pid


if len(sys.argv) < 2:
    sys.exit("Please provide file performance file suffix!")

pid = get_pid("python3")

file_name = sys.argv[1]
results_dir = sys.argv[2]

tmp_file_name = "tmp-" + file_name
tmp_file_name = os.path.join(results_dir, tmp_file_name)

file_name = os.path.join(results_dir, file_name)

process = psutil.Process(int(pid))

peak_threads = process.num_threads()

rss_mem = []
vms_mem = []
cpu_usage = []
network_bytes_sent = []
network_bytes_recv = []

# These counters return the cumulative network bytes so need to get difference during each iteration by logging
# the counters before and after the 1 sec sleep and then getting the difference between them.
net_io_counters = psutil.net_io_counters()
base_network_bytes_sent = net_io_counters.bytes_sent
base_network_bytes_recv = net_io_counters.bytes_recv

# first call returns 0 so will be ignored.
# 0/None is a special value that will return the CPU usage averaged from the last call of
# cpu_percent. The first call will always return 0 CPU usage as per the spec. Then we sleep for one sec and will call
# cpu_percent() which would then return the avg CPU used over the last second (since the previous call).
process.cpu_percent()

print("this is cpu count: " + str(psutil.cpu_count()))

while True:
    with open(tmp_file_name, "w") as tmp_file:
        first_time = time.time()
        time.sleep(1)

        curr_rss_mem = process.memory_info().rss  # in bytes
        curr_vms_mem = process.memory_info().vms  # in bytes
        curr_threads = process.num_threads()
        curr_cpu_perc = process.cpu_percent() / psutil.cpu_count()

        net_io_counters = psutil.net_io_counters()
        curr_network_bytes_sent = net_io_counters.bytes_sent - base_network_bytes_sent
        curr_network_bytes_recv = net_io_counters.bytes_recv - base_network_bytes_recv
        base_network_bytes_sent = net_io_counters.bytes_sent
        base_network_bytes_recv = net_io_counters.bytes_recv

        rss_mem.append(curr_rss_mem)
        vms_mem.append(curr_vms_mem)
        cpu_usage.append(curr_cpu_perc)
        network_bytes_sent.append(curr_network_bytes_sent)
        network_bytes_recv.append(curr_network_bytes_recv)

        peak_threads = max(peak_threads, process.num_threads())

        dictionary = {
            "peak_threads": peak_threads,
            "rss_mem": rss_mem,
            "vms_mem": vms_mem,
            "cpu_usage": cpu_usage,
            "network_bytes_sent": network_bytes_sent,
            "network_bytes_recv": network_bytes_recv,
        }

        json_object = json.dumps(dictionary, indent=4)

        tmp_file.write(json_object)
        tmp_file.flush()
        os.fsync(tmp_file.fileno())
        os.replace(tmp_file_name, file_name)
        later_time = time.time()
        difference = later_time - first_time
        print(f'Performance sampling took {difference * 1000} seconds')
