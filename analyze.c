#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
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

// Capacity of our hashmap
// Needs to be a power of 2
// so we can bit-and instead of modulo
#define HASHMAP_CAPACITY 16384
#define HASHMAP_INDEX(h) (h & (HASHMAP_CAPACITY - 1))

#ifndef NTHREADS
#define NTHREADS 16
#endif

static size_t chunk_count;
static size_t chunk_size;
static atomic_uint chunk_selector;

struct Group {
  unsigned int count;
  int min;
  int max;
  long sum;
  char key[MAX_GROUPBY_KEY_LENGTH];
};

struct Result {
  unsigned int n;
  unsigned int map[HASHMAP_CAPACITY];
  struct Group groups[MAX_DISTINCT_GROUPS];
};

// parses a floating point number as an integer
// this is only possible because we know our data file has only a single decimal
static inline const char *parse_number(int *dest, const char *s) {
  // parse sign
  int mod = 1;
  if (*s == '-') {
    mod = -1;
    s++;
  }

  if (s[1] == '.') {
    *dest = ((s[0] * 10) + s[2] - ('0' * 11)) * mod;
    return s + 4;
  }

  *dest = (s[0] * 100 + s[1] * 10 + s[3] - ('0' * 111)) * mod;
  return s + 5;
}

static inline int min(int a, int b) { return a < b ? a : b; }
static inline int max(int a, int b) { return a > b ? a : b; }

// qsort callback
static inline int cmp(const void *ptr_a, const void *ptr_b) {
  return strcmp(((struct Group *)ptr_a)->key, ((struct Group *)ptr_b)->key);
}

// returns a pointer to the slot in our hashmap
// for storing the index in our results array
static inline unsigned int *hashmap_entry(struct Result *result,
                                          const char *key) {
  unsigned int h = 0;
  unsigned int len = 0;
  for (; key[len] != '\0'; len++) {
    h = (h * 31) + (unsigned char)key[len];
  }

  unsigned int *c = &result->map[HASHMAP_INDEX(h)];
  while (*c > 0 && memcmp(result->groups[*c].key, key, len) != 0) {
    h++;
    c = &result->map[HASHMAP_INDEX(h)];
  }

  return c;
}

static void *process_chunk(void *_data) {
  char *data = (char *)_data;

  // initialize result
  struct Result *result = malloc(sizeof(*result));
  if (!result) {
    perror("malloc error");
    exit(EXIT_FAILURE);
  }
  result->n = 0;

  // we could do this in a single call to memset
  // since the two are contiguous in memory
  // but this code is only called NTHREADS times
  // so not really worth it
  memset(result->map, 0, HASHMAP_CAPACITY * sizeof(*result->map));
  memset(result->groups, 0, MAX_DISTINCT_GROUPS * sizeof(*result->groups));

  // keep grabbing chunks until done
  while (1) {
    const unsigned int chunk = chunk_selector++;
    if (chunk >= chunk_count) {
      break;
    }
    size_t chunk_start = chunk * chunk_size;
    size_t chunk_end = chunk_start + chunk_size;

    // skip forward to next newline in chunk
    const char *s = chunk_start > 0
                  ? (char *)memchr(&data[chunk_start], '\n', chunk_size) + 1
                  : &data[chunk_start];

    // this assumes the file ends in a newline...
    const char *end = (char *)memchr(&data[chunk_end], '\n', chunk_size) + 1;

    // flaming hot loop
    while (s != end) {
      const char *linestart = s;

      // find position of ;
      // while simulatenuously hashing everything up to that point
      unsigned int len = 0;
      unsigned int h = 0;
      while (s[len] != ';') {
        h = (h * 31) + (unsigned char)s[len];
        len += 1;
      }

      // parse decimal number as int
      int temperature;
      s = parse_number(&temperature, linestart + len + 1);

      // probe map until free spot or match
      unsigned int *c = &result->map[HASHMAP_INDEX(h)];
      while (*c > 0 && memcmp(result->groups[*c].key, linestart, len) != 0) {
        h += 1;
        c = &result->map[HASHMAP_INDEX(h)];
      }

      // new hashmap entry
      if (*c == 0) {
        *c = result->n;
        memcpy(result->groups[*c].key, linestart, len);
        result->n++;
      }

      // existing entry
      result->groups[*c].count += 1;
      result->groups[*c].min = min(result->groups[*c].min, temperature);
      result->groups[*c].max = max(result->groups[*c].max, temperature);
      result->groups[*c].sum += temperature;
    }
  }

  return (void *)result;
}

static void result_to_str(char *dest, const struct Result *result) {
  char buf[128];
  *dest++ = '{';
  for (unsigned int i = 0; i < result->n; i++) {
    size_t n = (size_t)snprintf(
        buf, 128, "%s=%.1f/%.1f/%.1f%s", result->groups[i].key,
        (float)result->groups[i].min / 10.0,
        ((float)result->groups[i].sum / (float)result->groups[i].count) / 10.0,
        (float)result->groups[i].max / 10.0, i < (result->n - 1) ? ", " : "");

    memcpy(dest, buf, n);
    dest += n;
  }
  *dest++ = '}';
  *dest = 0x0;
}

int main(int argc, char **argv) {
  // set-up pipes for communication
  // then fork into child process which does the actual work
  // this allows us to skip the time the system spends doing munmap
  int pipefd[2];
  if (pipe(pipefd) != 0) {
    perror("pipe error");
    exit(EXIT_FAILURE);
  }
  pid_t pid;
  pid = fork();
  if (pid > 0) {
    // close write pipe
    close(pipefd[1]);
    size_t sz = (1 << 10) * 16;
    char buf[sz];
    if (-1 == read(pipefd[0], &buf, sz)) {
      perror("read error");
    }
    printf("%s", buf);
    close(pipefd[0]);
    exit(EXIT_FAILURE);
  }

  // close unused read pipe
  close(pipefd[0]);

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
  char *data = mmap(NULL, sz, PROT_READ, MAP_SHARED, fd, 0);
  if (data == MAP_FAILED) {
    perror("error mmapping file");
    exit(EXIT_FAILURE);
  }

  // distribute work among N worker threads
  chunk_size = (sz / (2 * NTHREADS));
  chunk_count = (sz / chunk_size - 1) + 1;
  pthread_t workers[NTHREADS];
  for (unsigned int i = 0; i < NTHREADS; i++) {
    pthread_create(&workers[i], NULL, process_chunk, data);
  }

  // wait for all threads to finish
  struct Result *results[NTHREADS];
  for (unsigned int i = 0; i < NTHREADS; i++) {
    pthread_join(workers[i], (void *)&results[i]);
  }

  // merge results
  struct Result *result = results[0];
  for (unsigned int i = 1; i < NTHREADS; i++) {
    for (unsigned int j = 0; j < results[i]->n; j++) {
      struct Group *b = &results[i]->groups[j];
      unsigned int *hm_entry = hashmap_entry(result, b->key);
      unsigned int c = *hm_entry;
      if (c == 0) {
        c = result->n++;
        *hm_entry = c;
        strcpy(result->groups[c].key, b->key);
      }
      result->groups[c].count += b->count;
      result->groups[c].sum += b->sum;
      result->groups[c].min = min(result->groups[c].min, b->min);
      result->groups[c].max = max(result->groups[c].max, b->max);
    }
  }

  // sort results alphabetically
  qsort(result->groups, (size_t)result->n, sizeof(*result->groups), cmp);

  // prepare output string
  char buf[(1 << 10) * 16];
  result_to_str(buf, result);
  if (-1 == write(pipefd[1], buf, strlen(buf))) {
    perror("write error");
  }

  // close write pipe
  close(pipefd[1]);

  // clean-up
  munmap((void *)data, sz);
  close(fd);
  for (unsigned int i = 0; i < NTHREADS; i++) {
    free(results[i]);
  }
  return EXIT_SUCCESS;
}
