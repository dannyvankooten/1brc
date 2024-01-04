# 1️⃣🐝🏎️ The One Billion Row Challenge

https://www.morling.dev/blog/one-billion-row-challenge/

The challenge: compute simple floating-point math over 1 billion rows. As fast as possible, without dependencies.

Implemented in standard C99 with POSIX threads (however, no SIMD).

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

real    0m3.598s
user    0m34.354s
sys     0m8.663s
```
