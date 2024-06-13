ifndef NTHREADS
NTHREADS=$(shell nproc --all 2>/dev/null || sysctl -n hw.logicalcpu)
endif

CFLAGS=-std=c11 -O2 -m64 -march=native -mtune=native -flto
CFLAGS+=-Wall -Wextra -Wconversion -Wformat -Wformat=2 -Wimplicit-fallthrough -Wvla
CFLAGS+=-DNTHREADS=$(NTHREADS)

ifdef DEBUG
CFLAGS+=-g -fno-omit-frame-pointer -fsanitize=address,undefined -fstack-protector-strong -fstack-clash-protection
CFLAGS+=-D_FORTIFY_SOURCE=3
endif

all: bin/ bin/create-sample bin/analyze

bin/:
	mkdir -p bin/

.PHONY: profile
profile: bin/analyze
	perf record --call-graph dwarf bin/analyze measurements-1M.txt

bin/create-sample: create-sample.c
	$(CC) $(CFLAGS) $^ -lm -o $@

bin/analyze: analyze.c
	$(CC) $(CFLAGS) $^ -o $@

.PHONY: clean
clean:
	rm -r bin/
