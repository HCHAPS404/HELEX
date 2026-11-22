#!/usr/bin/env bash
# vm_bench.sh NPAIRS BIN [BIN...]   — interleaved benchmark, cold cache per run.
# Usage: ./vm_bench.sh 6 ./ss-alex ./ss-cand
set -u
cd "$(dirname "$0")"
N=${1:-4}; shift
DATA=${BENCH_DATA:-$HOME/bench_data}
OUT=/tmp/helex_out; SCR=/tmp/helex_scr
mkdir -p "$DATA" "$OUT" "$SCR"

if [ ! -f "$DATA/events/part-00000.csv" ]; then
  echo "[gen] generating dataset (20M events / 5M clicks, seed 3)..."
  ./generate --seed 3 --events 20000000 --clicks 5000000 --output "$DATA"
fi

export DATA_DIR="$DATA" OUTPUT_DIR="$OUT" SCRATCH_DIR="$SCR"
export HOME_BK="$HOME" HOME="$SCR" TMPDIR="$SCR"
export NCPU=$(nproc) MEM_GB=24 DEBUG_BENCH=1
export MIMALLOC_LARGE_OS_PAGES=1 MIMALLOC_EAGER_COMMIT=1

echo "=== cpu ==="
grep -m1 "model name" /proc/cpuinfo
grep -o "avx512[a-z_]*" /proc/cpuinfo | sort -u | tr '\n' ' '; echo
nproc

drop_cache() {
  sync
  if sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then return; fi
  python3 - <<'EOF'
import os
d = os.environ.get("DATA_DIR", ".")
for root, _, fs in os.walk(d):
    for f in fs:
        try:
            fd = os.open(os.path.join(root, f), os.O_RDONLY)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            os.close(fd)
        except OSError:
            pass
EOF
}

declare -A T
for i in $(seq 1 "$N"); do
  for b in "$@"; do
    name=$(basename "$b")
    drop_cache
    rm -rf "$OUT"/* 2>/dev/null
    t0=$(date +%s.%N)
    "$b" >/dev/null 2>/tmp/bench_err || echo "!! rc=$? for $name"
    t1=$(date +%s.%N)
    dt=$(echo "$t1 $t0" | awk '{printf "%.3f", $1-$2}')
    line=$(grep '\[bench\]' /tmp/bench_err | head -1)
    echo "${name}#${i}: run_s=${dt} ${line}"
    T[$name]+="${dt} "
  done
done

echo "=== medians ==="
for b in "$@"; do
  name=$(basename "$b")
  med=$(echo ${T[$name]} | tr ' ' '\n' | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}')
  echo "${name}: median=${med}  all=[${T[$name]% }]"
done
