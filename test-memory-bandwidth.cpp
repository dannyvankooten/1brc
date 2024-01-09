#include <random>
#include <chrono>
#include <iostream>
#include <future>

using uint = unsigned int;
using ulong = unsigned long;

// Size of data buffer used to test.
const std::size_t N = 1'000'000'000;
uint data[N];

// Worker function.  Sums a chunk of memory.
unsigned int
sum(const uint *const begin, const uint *const end) {

    uint sum = 0;
    for (const uint *p = begin; p < end; p++) {
        sum += *p;
    }

    return sum;
}

// Wrapper function that spawns threads and times them.
void
time(int n_threads) {

    std::vector<std::future<uint>> futures;

    // Make it a double because it might not divide evenly.
    double chunk_size = double(N)/n_threads;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < n_threads; i++) {
        futures.push_back(std::async(sum, data + ulong(i*chunk_size), data + ulong((i + 1)*chunk_size)));
    }

    // Add up all the individual sums.
    uint sum = 0;
    for (auto &f : futures) {
        sum += f.get();
    }

    auto stop = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> secs = stop - start;
    std::cerr << "    " << sizeof(data)/secs.count() << std::endl;

    std::cerr << "    To prevent optimizing out all ops: " << sum << std::endl;
}

int
main() {

    /*
     * Fill with some random numbers.  PRNGs are slow, though,
     * so mostly just use the index.
     */

    std::default_random_engine eng;
    std::uniform_int_distribution<uint> dist(0, 0xffffffffU);

    for (std::size_t i = 0; i < 1'000'000'000; i++) {
        // Only set every 1000 numbers to a random number.
        if (i%1000 == 0) {
            data[i] = dist(eng);
        } else {
            data[i] = i;
        }
    }

    /*
     * Now do the timing.
     */

    for (int i = 1; i < 10; i++) {
        std::cerr << i << " thread(s):" << std::endl;
        time(i);
    }
}
