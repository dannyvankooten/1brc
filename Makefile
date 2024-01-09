NTHREADS=$(shell nproc --all 2>/dev/null || sysctl -n hw.logicalcpu)
CFLAGS+=-O2 -march=native -Wall -Wextra -Wpedantic \
-Wformat=2 -Wconversion -Wtrampolines -Wimplicit-fallthrough -DNTHREADS=$(NTHREADS)

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
