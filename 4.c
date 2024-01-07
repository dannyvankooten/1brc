#include <linux/mman.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#define HCAP (4096)
#define BUFSIZE ((1 << 20) * 1024)

struct result {
  char city[32];
  int count;
  double sum, min, max;
};

static const char *parse_double(double *dest, const char *s) {
  // parse sign
  double mod;
  if (*s == '-') {
    mod = -1.0;
    s++;
  } else {
    mod = 1.0;
  }

  if (s[1] == '.') {
    *dest = (((double)s[0] + (double)s[2] / 10.0) - 1.1 * '0') * mod;
    return s + 4;
  }

  *dest =
      ((double)((s[0]) * 10 + s[1]) + (double)s[3] / 10.0 - 11.1 * '0') * mod;
  return s + 5;
}

// hash returns a simple (but fast) hash for the first n bytes of data
static unsigned int hash(const unsigned char *data, int n) {
  unsigned int hash = 0;

  for (int i = 0; i < n; i++) {
    hash = (hash * 31) + data[i];
  }

  return hash;
}

static int cmp(const void *ptr_a, const void *ptr_b) {
  return strcmp(((struct result *)ptr_a)->city, ((struct result *)ptr_b)->city);
}

void print_results(struct result results[], int nresults) {
  char buf[1024 * 16];
  char *b = buf;
  for (int i = 0; i < nresults; i++) {
    size_t n = sprintf(b, "%s=%.1f/%.1f/%.1f%s", results[i].city,
                       results[i].min, results[i].sum / results[i].count,
                       results[i].max, i < nresults - 1 ? ", " : "");
    b += n;
  }
  *b = 0x0;
  puts(buf);
}

int main(int argc, const char **argv) {
  const char *file = "measurements.txt";
  if (argc > 1) {
    file = argv[1];
  }

  FILE *fh = fopen(file, "r");
  if (!fh) {
    perror("error opening file");
    exit(EXIT_FAILURE);
  }

  struct result results[450];
  int nresults = 0;
  // char *buf = mmap(NULL, BUFSIZE, PROT_READ | PROT_WRITE,
  //                  MAP_PRIVATE | MAP_HUGETLB | MAP_ANONYMOUS, -1, 0);
  char *buf = malloc(BUFSIZE);
  if (buf == NULL || buf == MAP_FAILED) {
    perror("mmap");
    return EXIT_FAILURE;
  }
  int map[HCAP];
  memset(map, -1, HCAP * sizeof(int));

  const char *linestart;
  char *pos;
  double measurement;
  unsigned int h;
  int c;

  while (1) {
    size_t nread = fread(buf, 1, BUFSIZE, fh);
    if (nread <= 0) {
      break;
    }

    const char *s = buf;
    size_t rewind = 0;
    while (buf[nread - 1] != '\n') {
      rewind++;
      nread--;
    }
    fseek(fh, -rewind, SEEK_CUR);

    while (s < &buf[nread]) {
      linestart = s;
      pos = strchr(s, ';');
      *pos = 0x0;
      s = parse_double(&measurement, pos + 1);

      // find index of group by key through hash with linear probing
      h = hash((unsigned char *)linestart, pos - linestart) & (HCAP - 1);
      while (map[h] != -1 && strcmp(results[map[h]].city, linestart) != 0) {
        h = (h + 1) & (HCAP - 1);
      }
      c = map[h];

      if (c < 0) {
        strcpy(results[nresults].city, linestart);
        results[nresults].sum = measurement;
        results[nresults].max = measurement;
        results[nresults].min = measurement;
        results[nresults].count = 1;
        map[h] = nresults;
        nresults++;
      } else {
        results[c].sum += measurement;
        results[c].count += 1;
        if (results[c].min > measurement) {
          results[c].min = measurement;
        }
        if (results[c].max < measurement) {
          results[c].max = measurement;
        }
      }
    }
  }

  qsort(results, (size_t)nresults, sizeof(*results), cmp);

  print_results(results, nresults);

  free(buf);
  fclose(fh);
}
