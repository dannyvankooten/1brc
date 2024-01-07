#!/usr/bin/bash

make 1 2 3 4 5 6 7
export TIMEFORMAT='%2R'
DESCRIPTIONS=(
    ""
    "linear-search by city name (baseline)"
    "hashmap with linear probing"
    "custom temperature float parser instead of strod"
    "fread with 64MB chunks instead of line-by-line"
    "unroll parsing of city name and generating hash"
    "parallelize across 16 threads"
    "mmap entire file instead of fread in chunks"
)

for PROGRAM in {1..7}; do
    # run program 5 times, capturing time output
    TIMES=""
    for n in {1..5}; do
        TIMES+="$({ time ./$PROGRAM measurements-100M.txt > /dev/null; } 2>&1 ) "
        sleep 1
    done

    echo -e $TIMES | awk "BEGIN { RS = \" \"; sum = 0.0; } {  sum += \$1; } END { sum /= 5.0; printf \"$PROGRAM.c runtime=[ $TIMES] average=%.2fs\t${DESCRIPTIONS[$PROGRAM]}\n\", sum }"
done
