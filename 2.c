#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define HCAP (4096)

struct result {
  char city[100];
  int count;
  double sum, min, max;
};

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
  char buf[1 << 10];
  int map[HCAP];
  memset(map, -1, HCAP * sizeof(int));

  while (fgets(buf, 1 << 10, fh)) {
    char *pos = strchr(buf, ';');
    *pos = 0x0;
    double measurement = strtod(pos + 1, NULL);

    // find index of group by key through hash with linear probing
    int h = hash((unsigned char *)buf, pos - buf) & (HCAP - 1);
    while (map[h] != -1 && strcmp(results[map[h]].city, buf) != 0) {
      h = (h + 1) & (HCAP - 1);
    }
    int c = map[h];

    if (c < 0) {
      strcpy(results[nresults].city, buf);
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

  qsort(results, (size_t)nresults, sizeof(*results), cmp);

  for (int i = 0; i < nresults; i++) {
    printf("%s=%.1f/%.1f/%.1f%s", results[i].city, results[i].min,
           results[i].sum / results[i].count, results[i].max,
           i < nresults ? ", " : "");
  }
  puts("\n");

  fclose(fh);
}
