#!/usr/bin/bash

rm -f bin/analyze
CC="$HOME/gcc14/bin/gcc" make

for i in {1..10}; do
    time ./bin/analyze >/dev/null;
    sleep 20
done
