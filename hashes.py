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

def alphabetical(s):
    h = 0
    for ch in s:
        h = (h * 26) + ord(ch)
    return h

def alphabetical_first_4(s):
    h = 0
    for ch in s[:4]:
        h = (h * 26) + ord(ch)
    return h

def m31(s):
    h = 0
    for ch in s:
        h = (h * 31) + ord(ch)
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


def test(f, p):

    if p.bit_count() == 1:
        fcapped = lambda data: f(data) & (p-1)
    else:
        fcapped = lambda data: f(data) % p
    seen = {}
    collissions = 0
    for city in cities:
        h = fcapped(city)
        seen[h] = city

    collissions = len(cities)-len(seen)
    return (collissions / len(cities), f, p)


primes = primes[:150]
powers = [(2**n) for n in range(9, 15)]
results = [test(f, cap) for f in  [alphabetical, alphabetical_first_4, m31, adler32, djb2, fnv1a, sdbm, hash] for cap in primes+powers]
results.sort(key=lambda x: x[0])

for (col_rate, fn, cap) in results[0:10]:
    print("{} {}: {:.2f}".format(fn.__qualname__, cap, col_rate))
