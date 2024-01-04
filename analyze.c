#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

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
    n = (n * 10) + (*s++ - '0');
  }

  // skip separator
  s++;

  // parse mantissa
  while (*s >= '0' && *s <= '9') {
    d = (d * 10) + (*s++ - '0');
  }

  *dest = mod * n * 10 + d;
  *endptr = s;
}

// hash returns the fnv-1a hash of the first n bytes in data
static inline unsigned int hash(const unsigned char *data, size_t n) {
  unsigned int hash = 0;

  for (size_t i = 0; i < n; data++, i++) {
    hash *= 0x811C9DC5; // prime
    hash ^= (*data);
  }

  return hash;
}

struct Group {
  unsigned int count;
  int sum, min, max;
  char *city;
};

// cmp compares the city property of the result that both pointers point
static inline int cmp(const void *ptr_a, const void *ptr_b) {
  return strcmp(((struct Group *)ptr_a)->city, ((struct Group *)ptr_b)->city);
}

struct ResultSet {
  char labels[420][32];
  struct Group groups[420];
  int n;
};

struct Chunk {
  char *data;
  size_t start;
  size_t end;
  struct ResultSet result;
};

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

  // initialize hashmap
  // will hold indexes into our cities and results array
  int map[HCAP];
  memset(map, -1, HCAP * sizeof(int));

  char *s = &ch->data[ch->start];
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
    while (c >= 0 && memcmp(ch->result.labels[c], start, len) != 0) {
      if (h++ == HCAP) {
        h = 0;
      }
      c = map[h];
    }

    if (c < 0) {
      memcpy(ch->result.labels[ch->result.n], start, len);
      ch->result.labels[ch->result.n][len] = 0x0;
      ch->result.groups[ch->result.n].city = ch->result.labels[ch->result.n];
      ch->result.groups[ch->result.n].count = 1;
      ch->result.groups[ch->result.n].sum = measurement;
      ch->result.groups[ch->result.n].min = measurement;
      ch->result.groups[ch->result.n].max = measurement;
      map[h] = ch->result.n++;
    } else {
      ch->result.groups[c].count += 1;
      ch->result.groups[c].sum += measurement;
      if (measurement < ch->result.groups[c].min) {
        ch->result.groups[c].min = measurement;
      } else if (measurement > ch->result.groups[c].max) {
        ch->result.groups[c].max = measurement;
      }
    }

    // skip newline
    // break out of loop if EOF
    if (*++s == 0x0 || s == &ch->data[ch->end]) {
      break;
    }
  }

  return (void *)ch;
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
  const int nworkers = 12;
  pthread_t workers[nworkers];
  struct Chunk chunks[nworkers];
  for (int i = 0; i < nworkers; i++) {
    chunks[i].result.n = 0;
    chunks[i].data = data;
    chunks[i].start = sz / (size_t)nworkers * (size_t)i;
    chunks[i].end = sz / (size_t)nworkers * ((size_t)i + 1);
    pthread_create(&workers[i], NULL, process_chunk, &chunks[i]);
  }

  // wait for all threads to finish
  for (int i = 0; i < nworkers; i++) {
    pthread_join(workers[i], NULL);
  }

  // merge results
  struct ResultSet result = chunks[0].result;
  for (int i = 1; i < nworkers; i++) {
    for (int j = 0; j < chunks[i].result.n; j++) {
      // get index of label
      for (int k = 0; k < result.n; k++) {
        if (strcmp(result.labels[k], chunks[i].result.labels[j]) == 0) {
          result.groups[k].count += chunks[i].result.groups[j].count;
          result.groups[k].sum += chunks[i].result.groups[j].sum;
          if (chunks[i].result.groups[j].min < result.groups[k].min) {
            result.groups[k].min = chunks[i].result.groups[j].min;
          }
          if (chunks[i].result.groups[j].max < result.groups[k].max) {
            result.groups[k].max = chunks[i].result.groups[j].max;
          }
          break;
        }
      }
    }
  }

  // sort results alphabetically
  qsort(result.groups, (size_t)result.n, sizeof(struct Group), cmp);

  // prepare output string
  char buf[128];
  char output[1 << 14];
  char *s = output;
  *s++ = '{';
  for (int i = 0; i < result.n; i++) {
    size_t n = (size_t)sprintf(
        buf, "%s=%.1f/%.1f/%.1f", result.groups[i].city,
        (float)result.groups[i].min / 10.0,
        ((float)result.groups[i].sum / (float)result.groups[i].count) / 10.0,
        (float)result.groups[i].max / 10.0);

    // copy buf to output
    memcpy(s, buf, n);

    if (i < result.n - 1) {
      memcpy(s + n, ", ", 2);
      n += 2;
    }

    s += n;
  }
  *s++ = '}';
  *s = 0x0;
  puts(output);

  munmap(data, sz);
  close(fd);
  exit(EXIT_SUCCESS);
}
