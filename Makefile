ifndef NTHREADS
NTHREADS=$(shell nproc --all 2>/dev/null || sysctl -n hw.logicalcpu)
endif

CFLAGS+= -std=c11 -O3 -march=native -mtune=native -flto -Wall -Wextra -Wpedantic \
-Wformat=2 -Wconversion -Wundef -Winline -Wimplicit-fallthrough -DNTHREADS=$(NTHREADS)

ifdef DEBUG
CFLAGS+= -D_FORTIFY_SOURCE=2 -D_GLIBCXX_ASSERTIONS 	\
-fsanitize=address -fsanitize=undefined -g 									\
-fstack-protector-strong
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
