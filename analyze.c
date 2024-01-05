#include <ctype.h>
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

// Capacity of our hashmap
// Since we use linear probing this needs to be at least twice as big
// as the # of distinct strings in our dataset
// Also must be power of 2 so we can use bit-and instead of modulo
#define HCAP (512 * 2 * 2)
#define MAX_DISTINCT_GROUPS 512
#define MAX_GROUPBY_KEY_LENGTH 100
#define NTHREADS 32

// branchless min/max of 2 integers
static inline int min(int a, int b) { return a ^ ((b ^ a) & -(b < a)); }
static inline int max(int a, int b) { return a ^ ((a ^ b) & -(a < b)); }

// parses a floating point number as an integer
// this is only possible because we know our data file has only a single decimal
static void parse_number(int *dest, char *s, char **endptr) {
  int n = 0;

  // parse sign
  int mod = 1;
  if (*s == '-') {
    mod = -1;
    s++;
  }

  if (*(s + 1) == '.') {
    n = mod * (((s[0] - '0') * 10) + (s[2] - '0'));
    s += 3;
  } else {
    n = mod * ((s[0] - '0') * 100 + (s[1] - '0') * 10 + (s[3] - '0'));
    s += 4;
  }

  *dest = n;
  *endptr = s;
}

// hash returns the fnv-1a hash of the first n bytes in data
static unsigned int hash(const unsigned char *data, int n) {
  unsigned int hash = 0;

  for (int i = 0; i < n; i++) {
    // hash *= 0x811C9DC5; // FNV prime
    hash = (hash * 31) + data[i];
  }

  return hash;
}

struct Group {
  unsigned int count;
  int sum;
  int min;
  int max;
  char *label;
};

struct Result {
  int map[HCAP];
  int n;
  char labels[MAX_DISTINCT_GROUPS][MAX_GROUPBY_KEY_LENGTH];
  struct Group groups[MAX_DISTINCT_GROUPS];
};

struct Chunk {
  size_t start;
  size_t end;
  char *data;
};

// cmp compares the city property of the result that both pointers point
static int cmp(const void *ptr_a, const void *ptr_b) {
  return strcmp(((struct Group *)ptr_a)->label, ((struct Group *)ptr_b)->label);
}

static unsigned int
hash_probe(int map[HCAP],
           char groups[MAX_DISTINCT_GROUPS][MAX_GROUPBY_KEY_LENGTH],
           const char *start, int len) {
  // probe map until free spot or match
  unsigned int h = hash((unsigned char *)start, len) & (HCAP - 1);
  while (map[h] >= 0 && memcmp(groups[map[h]], start, (size_t)len) != 0) {
    h = (h + 1) & (HCAP - 1);
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
  memset(result->labels, 0,
         MAX_DISTINCT_GROUPS * MAX_GROUPBY_KEY_LENGTH * sizeof(char));
  memset(result->map, -1, HCAP * sizeof(int));

  char *s = &ch->data[ch->start];
  char *end = &ch->data[ch->end];
  char *start;
  char *sep;
  unsigned int h;
  int temperature;
  int len;
  int c;

  while (s != end) {
    start = s;
    sep = strchr(start + 2, ';');
    len = (int)(sep - start);
    parse_number(&temperature, sep + 1, &s);

    // probe map until free spot or match
    h = hash_probe(result->map, result->labels, start, len);
    c = result->map[h];

    if (c < 0) {
      memcpy(result->labels[result->n], start, (size_t)len);
      result->labels[result->n][len] = 0x0;
      result->groups[result->n].label = result->labels[result->n];
      result->groups[result->n].count = 1;
      result->groups[result->n].sum = temperature;
      result->groups[result->n].min = temperature;
      result->groups[result->n].max = temperature;
      result->map[h] = result->n++;
    } else {
      result->groups[c].count += 1;
      result->groups[c].sum += temperature;
      result->groups[c].min = min(result->groups[c].min, temperature);
      result->groups[c].max = max(result->groups[c].max, temperature);
    }

    // skip newline
    s++;
  }

  return (void *)result;
}

void result_to_str(char *dest, const struct Result *result) {
  char buf[128];
  *dest++ = '{';
  for (int i = 0; i < result->n; i++) {
    size_t n = (size_t)sprintf(
        buf, "%s=%.1f/%.1f/%.1f", result->groups[i].label,
        (float)result->groups[i].min / 10.0,
        ((float)result->groups[i].sum / (float)result->groups[i].count) / 10.0,
        (float)result->groups[i].max / 10.0);

    // copy buf to output
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
  char *data = mmap(NULL, sz, PROT_READ, MAP_PRIVATE, fd, 0);
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
  struct Result *result = results[0];
  for (int i = 1; i < NTHREADS; i++) {
    for (int j = 0; j < results[i]->n; j++) {
      struct Group *b = &results[i]->groups[j];
      char *label = results[i]->labels[j];
      unsigned int h =
          hash_probe(result->map, result->labels, label, (int)strlen(label));

      // TODO: Refactor lines below, we can share some logic with process_chunk
      int c = result->map[h];
      if (c >= 0) {
        result->groups[c].count += b->count;
        result->groups[c].sum += b->sum;
        result->groups[c].min = min(result->groups[c].min, b->min);
        result->groups[c].max = max(result->groups[c].max, b->max);
      } else {
        // memcpy(&result->groups[result->n], b, sizeof(*b));
        strcpy(result->labels[result->n], label);
        result->groups[result->n].count = b->count;
        result->groups[result->n].sum = b->sum;
        result->groups[result->n].min = b->min;
        result->groups[result->n].max = b->max;
        result->groups[result->n].label = result->labels[result->n];
        result->map[h] = result->n++;
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
  munmap(data, sz);
  close(fd);
  for (int i = 0; i < NTHREADS; i++) {
    free(results[i]);
  }
  exit(EXIT_SUCCESS);
}
