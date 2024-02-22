# 1️⃣🐝🏎️ The One Billion Row Challenge

- Challenge blog post: https://www.morling.dev/blog/one-billion-row-challenge/
- Challenge repository: https://github.com/gunnarmorling/1brc

The challenge: **compute simple floating-point math over 1 billion rows. As fast as possible, without dependencies.**

Implemented in standard C11 with POSIX threads (however, no SIMD). `analyze.c` contains the fastest implementation, while `{1..7}.c` contain slower versions of the same program.

I wrote up some implmentation details on my blog here: https://www.dannyvankooten.com/blog/2024/1brc/

## Running the challenge

First, compile the two programs using any capable C-compiler.

```
make
```

### Create the measurements file with 1B rows

```
bin/create-sample 1000000000
```

This will create a 12 GB file with 1B rows named `measurements.txt` in your current working directory. The program to create this sample file will take a minute or two, but you only need to run it once.

### Run the challenge:

```
time bin/analyze measurements.txt >/dev/null

real	0m1.392s
user	0m0.000s
sys	    0m0.010sys
```

**Note:** the performance difference between a warm and a hot pagecache is quite extreme. Run `echo 3 > /proc/sys/vm/drop_caches` to drop your pagecache, then run the program twice in a row. It's not uncommon for the second run to be well over twice as fast.


### Benchmarks

Since I don't have access to a Hetzner CCX33 box, here are the reference times for the currently leading Java implementations from the official challenge when I run them on my machine.

| # | Result (m:s.ms) | Implementation     | Language | Submitter     |
|---|-----------------|--------------------|-----|---------------|
| ? |        00:01.590 | [link](https://github.com/dannyvankooten/1brc/blob/main/analyze.c)| C | [Danny van Kooten](https://github.com/dannyvankooten)|
| 1.|        00:06.131 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_spullara.java)| 21.0.1-graalce| [Sam Pullara](https://github.com/spullara)|
| 2.|        00:06.421 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_royvanrijn.java)| 21.0.1-graalce   | [Roy van Rijn](https://github.com/royvanrijn)|


### Progressions

You can find the average runtime (across 5 consecutive runs) for the various states of the program below, from baseline to the final and fully optimized version. Because I have no patience, this was run on a measurements file with only 100M rows.

```
1.c runtime=[ 55.86 59.09 64.28 63.63 56.08 ] average=59.79s   linear-search by city name (baseline)
2.c runtime=[ 9.14 9.31 9.35 9.05 9.30 ] average=9.23s hashmap with linear probing
3.c runtime=[ 4.27 4.51 4.47 4.28 4.25 ] average=4.36s custom temperature float parser instead of strod
4.c runtime=[ 2.38 2.41 2.46 2.40 2.39 ] average=2.41s fread with 64MB chunks instead of line-by-line
5.c runtime=[ 2.13 1.99 1.99 2.00 2.05 ] average=2.03s unroll parsing of city name and generating hash
6.c runtime=[ 0.49 0.49 0.49 0.50 0.50 ] average=0.49s parallelize across 16 threads
7.c runtime=[ 0.30 0.25 0.23 0.24 0.24 ] average=0.25s mmap entire file instead of fread in chunks
```

You can run the benchmark script for all progressions by executing `./run-progressions.sh` (needs `bash`, `make`, `time` and `awk`).
