#!/bin/sh
# Synthetic dataset for PGO profiling only. Exercises the hot paths:
# slow (long) ids, quoted fields, F1/F2/F3 timestamps, dup event ids,
# links for the DSU, clicks, purchases. Output need not be semantically
# valid for scoring - just representative branch mixes.
set -eu
OUT="${1:-/build/pgdata}"
mkdir -p "$OUT/events" "$OUT/clicks" "$OUT/links"

cat > "$OUT/manifest.json" <<'EOF'
{"window_start":"2026-10-01T00:00:00Z","window_end":"2026-10-25T00:00:00Z"}
EOF

awk 'BEGIN {
  printf "product_id,category\n"
  for (i = 0; i < 2000; i++) printf "p%d,cat%d\n", i, i % 8
}' > "$OUT/products.csv"

awk 'BEGIN {
  printf "campaign_id,channel\n"
  for (i = 0; i < 50; i++) printf "c%d,%s\n", i, (i % 3 == 0 ? "search" : (i % 3 == 1 ? "social" : "email"))
}' > "$OUT/campaigns.csv"

# user id helper embedded in each generator: base "u" + digits, 1-in-8 gets
# a 32-hex suffix (slow path). ~20% of fields quoted.
awk 'BEGIN {
  printf "user_a,user_b\n"
  for (i = 0; i < 200000; i++) {
    a = i % 400000; b = (i * 7 + 13) % 400000
    ua = "u" a; ub = "u" b
    if (a % 8 == 3) ua = ua "0a1b2c3d4e5f60718293a4b5c6d7e8f9"
    if (b % 8 == 5) ub = ub "11223344556677889900aabbccddeeff"
    printf "%s,%s\n", ua, ub
  }
}' > "$OUT/links/part-00000.csv"

awk 'BEGIN {
  printf "click_id,user_id,campaign_id,ts\n"
  base = 1759276800  # 2026-10-01T00:00:00Z
  for (i = 0; i < 500000; i++) {
    u = i % 400000; uid = "u" u
    if (u % 8 == 3) uid = uid "0a1b2c3d4e5f60718293a4b5c6d7e8f9"
    t = base + (i * 173) % 2073600
    sec = t % 60; m = (t / 60) % 60; h = (t / 3600) % 24; d = 1 + (t - base) / 86400
    ts = sprintf("2026-10-%02dT%02d:%02d:%02dZ", d, h, m, sec)
    if (i % 5 == 2) uid = "\"" uid "\""
    printf "c%06d,%s,c%d,%s\n", i, uid, i % 50, ts
  }
}' > "$OUT/clicks/part-00000.csv"

awk 'BEGIN {
  printf "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
  base = 1759276800
  for (i = 0; i < 2400000; i++) {
    eid = "e" (i * 31 + 7) % 2400000         # some dup ids below
    if (i % 33 == 0) eid = "e" ((i * 31 + 7) / 33) % 2400000   # ~3% dup
    if (i % 8 == 1) eid = eid "abcdef0123456789abcdef0123456789"
    u = (i * 13) % 400000; uid = "u" u
    if (u % 8 == 3) uid = uid "0a1b2c3d4e5f60718293a4b5c6d7e8f9"
    t = base + (i * 97) % 2073600
    it = t + (i % 3600)
    sec = t % 60; m = (t / 60) % 60; h = (t / 3600) % 24; d = 1 + (t - base) / 86400
    isec = it % 60; im = (it / 60) % 60; ih = (it / 3600) % 24; idd = 1 + (it - base) / 86400
    ingest = sprintf("2026-10-%02dT%02d:%02d:%02dZ", idd, ih, im, isec)
    r = i % 10
    if (r < 6)      ts = sprintf("2026-10-%02dT%02d:%02d:%02dZ", d, h, m, sec)
    else if (r < 7) ts = sprintf("2026-10-%02dT%02d:%02d:%02d-03:00", d, h, m, sec)
    else            ts = sprintf("%d", t * 1000)
    et = (i % 20 == 0 ? "purchase" : (i % 5 == 0 ? "cart" : "view"))
    pr = (et == "purchase" ? "+" (i % 50000) : "")
    pid = "p" (i % 2000)
    if (i % 5 == 1) uid = "\"" uid "\""
    if (i % 5 == 3) pid = "\"" pid "\""
    printf "%s,%s,%s,%s,%s,%s,%s\n", eid, uid, ts, ingest, et, pid, pr
  }
}' > "$OUT/events/part-00000.csv"

echo "pgo dataset ready at $OUT"
