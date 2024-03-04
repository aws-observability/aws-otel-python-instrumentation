#!/bin/sh
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# The reason this script exists compared to just running the python3 command in
# vehicleInventoryService.execInContainer(command);
# is because execInContainer command is blocking. Profiler is running in an infinite while loop so it never returns.
# The way around this would be to run it in the background with &. However, running with & directly, it runs
# the command once and quits. This is just a hacky workaround. Also, reason we are
# outputting to output.txt is that output logs of the script can be checked during the run inside the container.

file_name=$1
results_path=$2
python3 profiler.py ${file_name} ${results_path} >> output.txt &
