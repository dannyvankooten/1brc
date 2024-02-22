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

#define HCAP (4096)
#define BUFSIZE ((1 << 20) * 64)
#define MAX_DISTINCT_GROUPS 512
#define MAX_GROUPBY_KEY_LENGTH 100

#ifndef NTHREADS
#define NTHREADS 16
#endif

struct Group {
  char key[MAX_GROUPBY_KEY_LENGTH];
  int count;
  double sum, min, max;
};

struct Result {
  int n;
  struct Group groups[MAX_DISTINCT_GROUPS];
};

static const char *parse_city(int *len, unsigned int *hash, const char *s) {
  int _len = 1;
  unsigned int _hash = (unsigned char)s[0];

  while (s[_len] != ';') {
    _hash = (_hash * 31) + (unsigned char)s[_len++];
  }

  *len = _len;
  *hash = _hash & (HCAP - 1);
  return s + _len + 1;
}

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
  return strcmp(((struct Group *)ptr_a)->key, ((struct Group *)ptr_b)->key);
}

static void print_results(struct Group groups[], int n) {
  char buf[1024 * 16];
  char *b = buf;
  for (int i = 0; i < n; i++) {
    b += sprintf(b, "%s=%.1f/%.1f/%.1f%s", groups[i].key, groups[i].min,
                 groups[i].sum / groups[i].count, groups[i].max,
                 i < n - 1 ? ", " : "");
  }
  *b = 0x0;
  puts(buf);
}

size_t fread_chunked(char *dest, FILE *fh) {
  static pthread_mutex_t lock;
  pthread_mutex_lock(&lock);

  size_t nread = fread(dest, 1, BUFSIZE, fh);
  if (nread <= 0) {
    pthread_mutex_unlock(&lock);
    return nread;
  }

  long rewind = 0;
  while (dest[nread - 1] != '\n') {
    rewind--;
    nread--;
  }
  fseek(fh, rewind, SEEK_CUR);
  pthread_mutex_unlock(&lock);
  return nread;
}

static void *process_chunk(void *arg) {
  FILE *fh = (FILE *)arg;

  struct Result *result = malloc(sizeof(*result));
  if (!result) {
    perror("malloc");
    exit(EXIT_FAILURE);
  }
  result->n = 0;

  char *buf = malloc(BUFSIZE);
  if (!buf) {
    exit(1);
  }
  int map[HCAP];
  memset(map, -1, HCAP * sizeof(int));

  const char *linestart;
  double measurement;
  unsigned int h;
  int c;
  int keylen;

  while (1) {
    size_t nread = fread_chunked(buf, fh);
    if (nread <= 0) {
      break;
    }

    const char *s = buf;
    while (s < &buf[nread]) {
      linestart = s;
      s = parse_city(&keylen, &h, s);
      s = parse_double(&measurement, s);

      // find index of group by key through hash with linear probing
      c = map[h];
      while (c != -1 &&
             memcmp(result->groups[c].key, linestart, (size_t)keylen) != 0) {
        h = (h + 1) & (HCAP - 1);
        c = map[h];
      }

      if (c < 0) {
        memcpy(result->groups[result->n].key, linestart, (size_t)keylen);
        result->groups[result->n].key[keylen] = '\0';
        result->groups[result->n].sum = measurement;
        result->groups[result->n].max = measurement;
        result->groups[result->n].min = measurement;
        result->groups[result->n].count = 1;
        map[h] = result->n;
        result->n++;
      } else {
        result->groups[c].sum += measurement;
        result->groups[c].count += 1;
        if (result->groups[c].min > measurement) {
          result->groups[c].min = measurement;
        } else if (result->groups[c].max < measurement) {
          result->groups[c].max = measurement;
        }
      }
    }
  }

  free(buf);
  return (void *)result;
}

int main(int argc, const char **argv) {
  const char *file = "measurements.txt";
  if (argc > 1) {
    file = argv[1];
  }

  FILE *fh = fopen(file, "r");
  if (!fh) {
    perror("fopen");
    exit(EXIT_FAILURE);
  }

  // start NTHREADS threads
  pthread_t workers[NTHREADS];
  for (int i = 0; i < NTHREADS; i++) {
    pthread_create(&workers[i], NULL, process_chunk, fh);
  }

  struct Result *results[NTHREADS];
  for (int i = 0; i < NTHREADS; i++) {
    pthread_join(workers[i], (void *)&results[i]);
  }

  struct Result final;
  final.n = 0;
  int map[HCAP];
  memset(map, -1, HCAP * sizeof(int));
  for (int i = 0; i < NTHREADS; i++) {
    for (int j = 0; j < results[i]->n; j++) {
      struct Group *b = &results[i]->groups[j];

      // find index of group by key through hash with linear probing
      unsigned int h =
          hash((unsigned char *)b->key, (int)strlen(b->key)) & (HCAP - 1);
      int c = map[h];
      while (c != -1 && strcmp(final.groups[c].key, b->key) != 0) {
        h = (h + 1) & (HCAP - 1);
        c = map[h];
      }

      // TODO: Refactor lines below, we can share some logic with process_chunk
      if (c < 0) {
        strcpy(final.groups[final.n].key, b->key);
        final.groups[final.n].sum = b->sum;
        final.groups[final.n].max = b->max;
        final.groups[final.n].min = b->min;
        final.groups[final.n].count = 1;
        map[h] = final.n;
        final.n++;
      } else {
        struct Group *a = &final.groups[c];
        a->sum += b->sum;
        a->count += b->count;
        if (a->min > b->min) {
          a->min = b->min;
        } else if (a->max < b->max) {
          a->max = b->max;
        }
      }
    }

    free(results[i]);
  }

  qsort(final.groups, (size_t) final.n, sizeof(struct Group), cmp);
  print_results(final.groups, final.n);
  // free(buf);
  fclose(fh);
}
