#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

struct result {
  char *city;
  int count, sum, min, max;
};

#define HCAP 1987

// parses a floating point number as an integer
// this is only possible because we know our data file has only a single decimal
static inline void parse_number(int *dest, char *s, char **endptr) {
  char mod = 1;
  int n = 0;
  int d = 0;

  // parse sign
  if (*s == '-') {
    mod = -1;
    s++;
  }

  // parse characteristic
  while (*s >= '0' && *s <= '9') {
    n = (n * 10) + (*s - '0');
    s++;
  }

  // skip separator
  s++;

  // parse mantissa
  while (*s >= '0' && *s <= '9') {
    d = (d * 10) + (*s - '0');
    s++;
  }

  *dest = mod * (n * 10 + d);
  *endptr = s;
}

// hash returns the fnv-1a hash of the first n bytes in data
static inline unsigned int hash(const unsigned char *data, size_t n) {
  const unsigned int fnv_prime = 0x811C9DC5;
  unsigned int hash = 0;

  for (size_t i = 0; i < n; data++, i++) {
    hash *= fnv_prime;
    hash ^= (*data);
  }

  return hash;
}

// cmp returns -1 if city property of a is lexically smaller than city property
// of b
static inline int cmp(const void *ptr_a, const void *ptr_b) {
  return strcmp(((struct result *)ptr_a)->city, ((struct result *)ptr_b)->city);
}

int main(int argc, char **argv) {
  char *file = "measurements.txt";
  if (argc > 1) {
    file = argv[1];
  }

  struct stat sb;
  int fd = open(file, O_RDONLY);
  if (!fd) {
    perror("error opening file");
    exit(EXIT_FAILURE);
  }

  if (fstat(fd, &sb) == -1) {
    perror("error getting file size");
    exit(EXIT_FAILURE);
  }

  char cities[413][28] = {{0}};
  struct result results[413];
  int nresults = 0;

  // mmap entire file into memory
  char *data = mmap(NULL, (size_t)sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (!data) {
    perror("error mmapping file");
    exit(EXIT_FAILURE);
  }

  // initialize map to -1
  int *map = malloc(HCAP * sizeof(int));
  if (!map) {
    perror("error allocating memory for hashmap");
    exit(EXIT_FAILURE);
  }
  memset(map, -1, HCAP * sizeof(int));

  char *s = data;
  char *start, *pos;
  unsigned int h;
  int measurement;
  size_t len;
  int c;
  while (1) {
    start = s;
    pos = strchr(s, ';');
    len = (size_t)(pos - start);
    parse_number(&measurement, pos + 1, &s);

    // probe map until free spot or match
    h = hash((unsigned char *)start, len) % HCAP;
    c = map[h];
    while (c >= 0 && memcmp(cities[c], start, len) != 0) {
      h = (h == HCAP - 1) ? 0 : h + 1;
      c = map[h];
    }

    if (c < 0) {
      map[h] = nresults;
      char *city = cities[nresults];
      memcpy(city, start, len);
      city[len] = 0x0;

      results[nresults].city = city;
      results[nresults].sum = measurement;
      results[nresults].max = measurement;
      results[nresults].min = measurement;
      results[nresults].count = 1;

      nresults++;
    } else {

      results[c].sum += measurement;
      results[c].count += 1;
      if (measurement < results[c].min) {
        results[c].min = measurement;
      } else if (measurement > results[c].max) {
        results[c].max = measurement;
      }
    }

    // skip newline
    s++;

    if (*s == 0x0) {
      break;
    }
  }

  char buf[128];
  char *output = malloc(1 << 14 * sizeof(char));
  if (!output) {
    perror("error allocating memory for output string");
    exit(EXIT_FAILURE);
  }

  s = output;
  *s++ = '{';
  qsort(results, (size_t)nresults, sizeof(struct result), cmp);
  for (int i = 0; i < nresults; i++) {
    size_t n = (size_t)sprintf(
        buf, "%s=%.1f/%.1f/%.1f", results[i].city,
        (double)results[i].min / 10.0,
        ((double)results[i].sum / (double)results[i].count) / 10.0,
        (double)results[i].max / 10.0);

    // copy buf to output
    memcpy(s, buf, n);

    if (i < nresults - 1) {
      memcpy(s + n, ", ", 2);
      n += 2;
    }

    s += n;
  }
  *s++ = '}';
  *s = 0x0;
  puts(output);

  free(map);
  free(output);
  munmap(data, sb.st_size);
  close(fd);
  exit(EXIT_SUCCESS);
}

// TODO: Concurrency, process in chunks where each chunk works on a separate map
// TODO: SIMD
