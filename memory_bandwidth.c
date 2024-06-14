// Program to get an estimation of the theoretical maximum
// by simply summing up a chunk of memory using a various # of threads

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>

#define N 1000000000

#ifndef NTHREADS
#define NTHREADS 4
#endif

struct thread_ctx {
  int *begin;
  int *end;
  int64_t sum;
};

int data[N];

// Worker function.  Sums a chunk of memory.
static int64_t
sum(const int *const begin, const int *const end) {
    int64_t sum = 0;
    for (const int *p = begin; p < end; p++) {
        sum += *p;
    }
    return sum;
}

static void* thread_routine(void *data) {
  struct thread_ctx* ctx = data;
  ctx->sum = sum(ctx->begin, ctx->end);
  return NULL;
}

int main(void) {
  // fill array with N random integers
  for (unsigned int i = 0; i < N; i++) {
    data[i] = (int) i;

    if (i % 1000 == 0) {
      data[i] = rand();
    }
  }

  // test from 1 up to NTHREADS
  for (unsigned int t = 1; t <= NTHREADS; t++) {
    struct timespec start, finish;
    clock_gettime(CLOCK_MONOTONIC, &start);

    // spin up t threads
    uint64_t chunk_size = N / t;
    struct thread_ctx thread_ctx[NTHREADS];
    pthread_t workers[NTHREADS];
    for (unsigned int i = 0; i < t; i++) {
      thread_ctx[i].begin = data + i * chunk_size;
      thread_ctx[i].end = i == t-1 ? data + N : data + (i + 1) * chunk_size;
      thread_ctx[i].sum = 0;
      pthread_create(&workers[i], NULL, thread_routine, &thread_ctx[i]);
    }

    // wait for all threads to finish
    int64_t sum = 0;
    for (unsigned int i = 0; i < t; i++) {
      pthread_join(workers[i], NULL);
      sum += thread_ctx[i].sum;
    }

    // output memory bandwidth in GiB / s
    clock_gettime(CLOCK_MONOTONIC, &finish);
    double elapsed = (double) (finish.tv_sec - start.tv_sec);
    elapsed += (double) (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
    double bytes_per_sec = sizeof(int) * N / elapsed;
    printf("%2d threads: %5.0f GiB / s\t%ld\n", t, bytes_per_sec / (1024*1024*1024), sum);
  }

  return EXIT_SUCCESS;
}
