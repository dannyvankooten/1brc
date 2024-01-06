# 1️⃣🐝🏎️ The One Billion Row Challenge

- Challenge blog post: https://www.morling.dev/blog/one-billion-row-challenge/
- Challenge repository: https://github.com/gunnarmorling/1brc

The challenge: **compute simple floating-point math over 1 billion rows. As fast as possible, without dependencies.**

Implemented in standard C99 with POSIX threads (however, no SIMD).

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

real    0m1.949s
user    0m26.654s
sys     0m0.868s
```

### Benchmarks

Since I don't have access to a Hetzner CCX33 box, here are the reference times for the currently leading Java implementations from the official challenge when I run them on my machine.

| # | Result (m:s.ms) | Implementation     | Language | Submitter     |
|---|-----------------|--------------------|-----|---------------|
| ? |        00:01.949 | [link](https://github.com/dannyvankooten/1brc/blob/main/analyze.c)| C | [Danny van Kooten](https://github.com/dannyvankooten)|
| 1.|        00:06.131 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_spullara.java)| 21.0.1-graalce| [Sam Pullara](https://github.com/spullara)|
| 2.|        00:06.421 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_royvanrijn.java)| 21.0.1-graalce   | [Roy van Rijn](https://github.com/royvanrijn)|
