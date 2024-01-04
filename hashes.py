#!/usr/bin/env python3

import os

def fnv1a(s):
    h = 0
    for ch in s:
        h *= 0x811C9DC5
        h ^= ord(ch)
    return h

def djb2(s):
    h = 5381
    for ch in s:
        h = (h << 5) + h + ord(ch)
    return h

def sdbm(s):
    h = 0
    for ch in s:
        h = ord(ch) + (h << 6) + (h << 16) - h
    return h

def ooooo(s):
    h = 0
    for ch in s:
        h = (h * 26) + ord(ch)
    return h

def adler32(s):
    h1 = 0
    h2 = 0
    for ch in s:
        h1 = (h1 + ord(ch)) % 65521
        h2 = (h2 + h1) % 65521
    return (h2 << 16) | h1

with open("primes.txt") as f:
    primes = [int(x) for x in f.read().splitlines()]

with open("cities.txt") as f:
    cities = f.read().splitlines()


pri = 0
while pri < 100:
    # print("\n\n# {}".format(primes[pri]))
    seen = {}
    collissions = 0
    for city in cities:
        h = adler32(city) % primes[pri]
        if h in seen:
            # print("{} ({}) collides with {}!".format(city, h, seen[h]))
            collissions += 1

        seen[h] = city

    pri += 1
    if collissions == 0:
        printf("{} is very good!".format(primes[pri]))
        os.exit()
        break

    print("{}: {}/{} collissions".format(primes[pri], collissions, len(cities)))
