CFLAGS+=-g -O2 -march=native -Wall -Wextra -Wpedantic -std=c99 -Wformat=2 -Wconversion -Wtrampolines \
-Wimplicit-fallthrough -D_XOPEN_SOURCE=500
LIBS+=-lm

all: bin/ bin/create-sample bin/analyze

bin/:
	mkdir -p bin/

.PHONY: profile
profile: bin/analyze
	perf record --call-graph dwarf bin/analyze measurements-1M.txt

bin/create-sample: create-sample.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $@

bin/analyze: analyze.c
	$(CC) $(CFLAGS) $^ $(LIBS) -o $@

.PHONY: clean
clean:
	rm -r bin/
