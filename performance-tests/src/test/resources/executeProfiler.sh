#!/bin/sh

suffix=$1
python3 profiler.py ${suffix} >> output.txt &
