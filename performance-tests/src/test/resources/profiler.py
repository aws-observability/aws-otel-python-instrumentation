import json
import os
import sys
import time

import psutil


def get_pid(process_name):
    pid = 1
    for proc in psutil.process_iter(["pid", "name"]):
        print(proc.name())
        if process_name in proc.name():
            pid = proc.pid
            break
    print("Pid:", pid)
    return pid


if len(sys.argv) < 2:
    sys.exit("Please provide file performance file suffix!")

pid = get_pid("python3")

file_suffix = sys.argv[1]

results_dir = "/results/"
tmp_file_name = "performance-metrics-tmp-" + file_suffix + ".json"
tmp_file_name = os.path.join(results_dir, tmp_file_name)

file_name = "performance-metrics-" + file_suffix + ".json"
file_name = os.path.join(results_dir, file_name)

process = psutil.Process(int(pid))

peak_threads = process.num_threads()

min_rss_mem = process.memory_info().rss
max_rss_mem = min_rss_mem

min_vms_mem = process.memory_info().vms
max_vms_mem = min_vms_mem

avg_cpu = process.cpu_percent(0.1) / psutil.cpu_count()
max_cpu = avg_cpu

print("this is cpu count: " + str(psutil.cpu_count()))

while True:
    with open(tmp_file_name, "w") as tmp_file:
        time.sleep(1)

        curr_rss_mem = process.memory_info().rss  # in bytes
        curr_vms_mem = process.memory_info().vms  # in bytes
        curr_threads = process.num_threads()
        curr_cpu_perc = process.cpu_percent(0.1) / psutil.cpu_count()

        peak_threads = max(peak_threads, process.num_threads())

        min_rss_mem = min(min_rss_mem, curr_rss_mem)
        max_rss_mem = max(max_rss_mem, curr_rss_mem)

        min_vms_mem = min(min_vms_mem, curr_vms_mem)
        max_vms_mem = max(max_vms_mem, curr_vms_mem)

        avg_cpu = (curr_cpu_perc + avg_cpu) / 2
        max_cpu = max(max_cpu, curr_cpu_perc)

        dictionary = {
            "peak_threads": peak_threads,
            "min_rss_mem": min_rss_mem,
            "max_rss_mem": max_rss_mem,
            "min_vms_mem": min_vms_mem,
            "max_vms_mem": max_vms_mem,
            "avg_cpu": avg_cpu,
            "max_cpu": max_cpu,
        }

        json_object = json.dumps(dictionary, indent=4)

        tmp_file.write(json_object)
        tmp_file.flush()
        os.fsync(tmp_file.fileno())
        os.replace(tmp_file_name, file_name)
