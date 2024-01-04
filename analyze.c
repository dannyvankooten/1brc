#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// Capacity of our hashmap
// Since we use linear probing this needs to be at least twice as big
// As the # of distinct strings in our dataset
// Also must be power of 2 so we can use bit-and instead of modulo
#define HCAP (4096*2)

// parses a floating point number as an integer
// this is only possible because we know our data file has only a single decimal
static inline void parse_number(int *dest, char *s, char **endptr) {
  char mod = 1;
  int n = 0;

  // parse sign
  if (*s == '-') {
    mod = -1;
    s++;
  }

  // parse characteristic
  while (*s >= '0' && *s <= '9') {
    n = (n * 10) + (*s++ - '0');
  }

  // skip separator
  s++;

  // parse mantissa
  while (*s >= '0' && *s <= '9') {
    n = (n * 10) + (*s++ - '0');
  }

  *dest = mod * n;
  *endptr = s;
}

// hash returns the fnv-1a hash of the first n bytes in data
static inline unsigned int hash(const unsigned char *data, int n) {
  unsigned int hash = 0;

  for (int i = 0; i < n; data++, i++) {
    hash *= 0x811C9DC5; // prime
    hash ^= (*data);
  }

  return hash;
}

struct Group {
  unsigned int count;
  int sum, min, max;
  char *label;
};

struct Result {
  char labels[420][32];
  struct Group groups[420];
  int map[HCAP];
  int n;
};

struct Chunk {
  char *data;
  size_t start;
  size_t end;
};

// cmp compares the city property of the result that both pointers point
static inline int cmp(const void *ptr_a, const void *ptr_b) {
  return strcmp(((struct Group *)ptr_a)->label, ((struct Group *)ptr_b)->label);
}

static inline unsigned int hash_probe(int map[HCAP], char groups[420][32],
                                      const char *start, int len) {
  // probe map until free spot or match
  unsigned int h = hash((unsigned char *)start, len) & (HCAP-1);
  while (map[h] >= 0 && memcmp(groups[map[h]], start, (size_t) len) != 0) {
    if (h++ == HCAP) {
      h = 0;
    }
  }

  return h;
}

static void *process_chunk(void *data) {
  struct Chunk *ch = (struct Chunk *)data;

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
  result->n = 0;

  // initialize hashmap
  // will hold indexes into our cities and results array
  memset(result->map, -1, HCAP * sizeof(int));

  char *s = &ch->data[ch->start];
  char *start, *pos;
  unsigned int h;
  int measurement;
  int len;
  int c;

  while (1) {
    start = s;
    pos = strchr(s, ';');
    len = (int)(pos - start);
    parse_number(&measurement, pos + 1, &s);

    // probe map until free spot or match
    h = hash_probe(result->map, result->labels, start, len);
    c = result->map[h];

    if (c < 0) {
      memcpy(result->labels[result->n], start, (size_t) len);
      result->labels[result->n][len] = 0x0;
      result->groups[result->n].label = result->labels[result->n];
      result->groups[result->n].count = 1;
      result->groups[result->n].sum = measurement;
      result->groups[result->n].min = measurement;
      result->groups[result->n].max = measurement;
      result->map[h] = result->n++;
    } else {
      result->groups[c].count += 1;
      result->groups[c].sum += measurement;
      if (measurement < result->groups[c].min) {
        result->groups[c].min = measurement;
      } else if (measurement > result->groups[c].max) {
        result->groups[c].max = measurement;
      }
    }

    // skip newline
    // break out of loop if EOF
    if (*++s == 0x0 || s == &ch->data[ch->end]) {
      break;
    }
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
  if (!data || data == MAP_FAILED) {
    perror("error mmapping file");
    exit(EXIT_FAILURE);
  }

  // distribute work among N worker threads
  const int nworkers = 8;
  pthread_t workers[nworkers];
  struct Chunk chunks[nworkers];
  for (int i = 0; i < nworkers; i++) {
    chunks[i].data = data;
    chunks[i].start = sz / (size_t)nworkers * (size_t)i;
    chunks[i].end = sz / (size_t)nworkers * ((size_t)i + 1);
    pthread_create(&workers[i], NULL, process_chunk, &chunks[i]);
  }

  // wait for all threads to finish
  struct Result *results[nworkers];
  for (int i = 0; i < nworkers; i++) {
    pthread_join(workers[i], (void *)&results[i]);
  }

  // merge results
  struct Result *result = results[0];
  for (int i = 1; i < nworkers; i++) {
    for (int j = 0; j < results[i]->n; j++) {
      struct Group *b = &results[i]->groups[j];
      char *label = results[i]->labels[j];
      unsigned int h =
          hash_probe(result->map, result->labels, label, (int) strlen(label));

      // TODO: Refactor lines below, we can share some logic with process_chunk
      int c = result->map[h];
      if (c >= 0) {
        result->groups[c].count += b->count;
        result->groups[c].sum += b->sum;
        if (b->min < result->groups[c].min) {
          result->groups[c].min = b->min;
        }
        if (b->max > result->groups[c].max) {
          result->groups[c].max = b->max;
        }
      } else {
        memcpy(&result->groups[result->n], b, sizeof(*b));
        strcpy(result->labels[result->n], label);
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
  for (int i = 0; i < nworkers; i++) {
    free(results[i]);
  }
  exit(EXIT_SUCCESS);
}
