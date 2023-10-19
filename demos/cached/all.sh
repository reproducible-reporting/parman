#!/usr/bin/env bash

rm -f cache.txt
./cached.py &
sleep 0.5
echo NEXT
./cached.py
