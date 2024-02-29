#!/bin/sh

suffix=$1
python3 collect-metrics.py ${suffix} >> output.txt &
