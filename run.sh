#!/bin/sh
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
export MIMALLOC_LARGE_OS_PAGES=1 MIMALLOC_EAGER_COMMIT=1
exec /app/shopping-sessions
