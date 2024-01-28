#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_DISTINCT_GROUPS 10000
#define MAX_GROUPBY_KEY_LENGTH 100
#define HCAP (1 << 14)

#ifndef NTHREADS
#define NTHREADS 16
#endif

// branchless min/max (on some machines at least)
#define min(a, b) (a ^ ((b ^ a) & -(b < a)));
#define max(a, b) (a ^ ((a ^ b) & -(a < b)));

struct Group {
  unsigned int count;
  long sum;
  int min;
  int max;
  char key[MAX_GROUPBY_KEY_LENGTH];
};

struct Result {
  unsigned int map[HCAP];
  unsigned int hashes[HCAP];
  unsigned int n;
  struct Group groups[MAX_DISTINCT_GROUPS];
};

struct Chunk {
  size_t start;
  size_t end;
  const char *data;
};

// parses a floating point number as an integer
// this is only possible because we know our data file has only a single decimal
static inline const char *parse_number(int *restrict dest, const char *s) {
  // parse sign
  int mod;
  if (*s == '-') {
    mod = -1;
    s++;
  } else {
    mod = 1;
  }

  if (s[1] == '.') {
    *dest = ((s[0] * 10) + s[2] - ('0' * 11)) * mod;
    return s + 4;
  }

  *dest = (s[0] * 100 + s[1] * 10 + s[3] - ('0' * 111)) * mod;
  return s + 5;
}

// qsort callback
static int cmp(const void *ptr_a, const void *ptr_b) {
  return strcmp(((struct Group *)ptr_a)->key, ((struct Group *)ptr_b)->key);
}

// finds hash slot in map of key
static inline unsigned int hash_probe(struct Result *result, const char *restrict key) {

  // hash key
  unsigned int h = (unsigned char)key[0];
  unsigned int len = 1;
  for (; key[len] != 0x0; len++) {
    h = (h * 31) + (unsigned char)key[len];
  }

  // linearly probe hashmap until match OR free spot
  while (result->hashes[h & (HCAP - 1)] != 0 &&
         h != result->hashes[h & (HCAP - 1)]) {
    h++;
  }

  return h;
}

static void *process_chunk(void *ptr) {
  struct Chunk *ch = (struct Chunk *)ptr;

  // skip start forward until SOF or after next newline
  if (ch->start > 0) {
    while (ch->data[ch->start - 1] != '\n') {
      ch->start++;
    }
  }

  while (ch->data[ch->end] != 0x0 && ch->data[ch->end - 1] != '\n') {
    ch->end++;
  }

  struct Result *result = malloc(sizeof(*result));
  if (!result) {
    perror("malloc error");
    exit(EXIT_FAILURE);
  }
  result->n = 0;

  memset(result->hashes, 0, HCAP * sizeof(int));
  memset(result->map, 0, HCAP * sizeof(int));

  const char *s = &ch->data[ch->start];
  const char *end = &ch->data[ch->end];
  const char *linestart;
  unsigned int h;
  int temperature;
  int len;
  unsigned int c;

  while (s != end) {
    linestart = s;

    // hash everything up to ';'
    // assumption: key is at least 1 char
    len = 1;
    h = (unsigned char)s[0];
    while (s[len] != ';') {
      h = (h * 31) + (unsigned char)s[len++];
    }

    // parse decimal number as int
    s = parse_number(&temperature, s + len + 1);

    // probe map until free spot or match
    while (result->hashes[h & (HCAP - 1)] != 0 &&
           h != result->hashes[h & (HCAP - 1)]) {
      h++;
    }
    c = result->map[h & (HCAP - 1)];

    if (c == 0) {
      memcpy(result->groups[result->n].key, linestart, (size_t)len);
      result->groups[result->n].key[len] = 0x0;
      result->groups[result->n].count = 1;
      result->groups[result->n].sum = temperature;
      result->groups[result->n].min = temperature;
      result->groups[result->n].max = temperature;
      result->map[h & (HCAP - 1)] = result->n++;
      result->hashes[h & (HCAP - 1)] = h;
    } else {
      result->groups[c].count += 1;
      result->groups[c].sum += temperature;
      if (temperature < result->groups[c].min) {
        result->groups[c].min = temperature;
      } else if (temperature > result->groups[c].max) {
        result->groups[c].max = temperature;
      }
    }
  }

  return (void *)result;
}

void result_to_str(char *dest, const struct Result *result) {
  char buf[128];
  *dest++ = '{';
  for (unsigned int i = 0; i < result->n; i++) {
    size_t n = (size_t)sprintf(
        buf, "%s=%.1f/%.1f/%.1f", result->groups[i].key,
        (float)result->groups[i].min / 10.0,
        ((float)result->groups[i].sum / (float)result->groups[i].count) / 10.0,
        (float)result->groups[i].max / 10.0);

    memcpy(dest, buf, n);
    if (i < result->n - 1) {
      memcpy(dest + n, ", ", 2);
      n += 2;
    }

    dest += n;
  }
  *dest++ = '}';
  *dest = 0x0;
}

int main(int argc, char **argv) {
  char *file = "measurements.txt";
  if (argc > 1) {
    file = argv[1];
  }

  int fd = open(file, O_RDONLY);
  if (!fd) {
    perror("error opening file");
    exit(EXIT_FAILURE);
  }

  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    perror("error getting file size");
    exit(EXIT_FAILURE);
  }

  // mmap entire file into memory
  size_t sz = (size_t)sb.st_size;
  const char *data = mmap(NULL, sz, PROT_READ, MAP_SHARED, fd, 0);
  if (data == MAP_FAILED) {
    perror("error mmapping file");
    exit(EXIT_FAILURE);
  }

  // distribute work among N worker threads
  pthread_t workers[NTHREADS];
  struct Chunk chunks[NTHREADS];
  size_t chunk_size = sz / (size_t)NTHREADS;
  for (int i = 0; i < NTHREADS; i++) {
    chunks[i].data = data;
    chunks[i].start = chunk_size * (size_t)i;
    chunks[i].end = chunk_size * ((size_t)i + 1);
    pthread_create(&workers[i], NULL, process_chunk, &chunks[i]);
  }

  // wait for all threads to finish
  struct Result *results[NTHREADS];
  for (int i = 0; i < NTHREADS; i++) {
    pthread_join(workers[i], (void *)&results[i]);
  }

  // merge results
  struct Group *b;
  unsigned int h;
  unsigned int c;
  struct Result *result = results[0];
  for (int i = 1; i < NTHREADS; i++) {
    for (unsigned int j = 0; j < results[i]->n; j++) {
      b = &results[i]->groups[j];
      h = hash_probe(result, b->key);

      // TODO: Refactor lines below, we can share some logic with process_chunk
      c = result->map[h & (HCAP - 1)];
      if (c >= 0) {
        result->groups[c].count += b->count;
        result->groups[c].sum += b->sum;
        result->groups[c].min = min(result->groups[c].min, b->min);
        result->groups[c].max = max(result->groups[c].max, b->max);
      } else {
        strcpy(result->groups[result->n].key, b->key);
        result->groups[result->n].count = b->count;
        result->groups[result->n].sum = b->sum;
        result->groups[result->n].min = b->min;
        result->groups[result->n].max = b->max;
        result->map[h & (HCAP - 1)] = result->n++;
        result->hashes[h & (HCAP - 1)] = h;
      }
    }
  }

  // sort results alphabetically
  qsort(result->groups, (size_t)result->n, sizeof(struct Group), cmp);

  // prepare output string
  char buf[(1 << 10) * 16];
  result_to_str(buf, result);
  puts(buf);

  // clean-up
  munmap((void *)data, sz);
  close(fd);
  for (int i = 0; i < NTHREADS; i++) {
    free(results[i]);
  }
  exit(EXIT_SUCCESS);
}
