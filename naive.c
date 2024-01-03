#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct result {
  char city[32];
  int count;
  double sum, min, max;
};

unsigned int hash(char *name) {
  return (name[0] - 'A') * 10000 + (name[1] - 'a') * 100 + (name[2] - 'a');
}

int getcity(char *city, struct result results[], int nresults) {
  for (int i = 0; i < nresults; i++) {
    if (strcmp(results[i].city, city) == 0) {
      return i;
    }
  }

  return -1;
}

int cmp(const void *ptr_a, const void *ptr_b) {
  return strcmp(((struct result *)ptr_a)->city, ((struct result *)ptr_b)->city);
}

int main(int argc, char **argv) {
  char *file = "measurements-1K.txt";
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
  char *buf = malloc(1 << 10);
  while (fgets(buf, 1 << 10, fh)) {
    char *pos = strchr(buf, ';');
    *pos = 0x0;
    double measurement = strtod(pos + 1, NULL);

    int c = getcity(buf, results, nresults);
    if (c < 0) {
      strcpy(results[nresults].city, buf);
      results[nresults].sum = measurement;
      results[nresults].max = measurement;
      results[nresults].min = measurement;
      results[nresults].count = 1;
      nresults++;
    } else {
      results[c].sum += measurement;
      results[c].count += 1;
      results[c].min = fmin(results[c].min, measurement);
      results[c].max = fmax(results[c].max, measurement);
    }
  }

  qsort(results, (size_t)nresults, sizeof(*results), cmp);
  for (int i = 0; i < nresults; i++) {
    printf("%s=%.1f/%.1f/%.1f%s", results[i].city, results[i].min,
           results[i].sum / results[i].count, results[i].max,
           i < nresults ? ", " : "");
  }

  fclose(fh);
}
