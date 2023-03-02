#!/bin/bash
set -e

STORAGE_SLOW="/storage" \
STORAGE_FAST="/home/filip" \
ENABLE_BADGER="YES" \
ENABLE_MARGARET="YES" \
ENABLE_BBOLT="YES" \
ENABLE_DATA_RANDOM="" \
ENABLE_DATA_LIKE_SSB="YES" \
make bench

make bench-report
