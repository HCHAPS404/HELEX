# HELEX — Benchmarks & Optimization Log (`shopping-sessions-4`)

Hardware de pruebas local: Windows 11 / WSL2 Linux `amd64` (12 logical CPUs, 12 GB host RAM, Docker `--cpus=8 --memory=4g --memory-swap=4g`).
Hardware oficial del evaluador: GCP `n2` Intel Xeon (`linux/amd64`), 8 vCPUs, 24 GB RAM, sin swap.

| Iteración / Commit | Motor / Descripción | Fixtures (36) | 1M Dev (`seed 1`) | 20M Práctica (`seed 2`) | 60M Final (`seed 3`) | Pico RSS | Notas |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| `48d7f0e` (`dsu-integration`) | Rust DSU single-thread -> staging CSV -> Python + DuckDB 1.5.6 | 36/36 | 9.172 s | ~183 s (est.) | >500 s (TIMEOUT) | ~2.5 GB + 5 GB CSV | Reescribe todos los CSV a `/scratch` y ejecuta doble window function + range join en DuckDB |
| Hito 2 MVP (Rust 1-thread) | Motor nativo Rust 6 etapas (single-thread, Parquet directo sin Arrow, mimalloc) | 36/36 + 5 seeds | 1.492 s | — | — | ~180 MB (1M) | `cat=0.011s dsu=0.034s events=1.356s clicks=0.336s sess=0.081s parquet=0.027s` |
| Hito 3A (SSO + 256 Shards L3 + Spill) | `SmallBytes` 16B inline + `HashTable` 256 cubetas L3 con tombstones + `/scratch` anti-OOM spill (1-thread) | 36/36 + 5 seeds + spill | 1.013 s | — | — | ~95 MB (1M) | `cat=0.009s dsu=0.020s events=0.847s clicks=0.118s sess=0.055s parquet=0.015s` |
| Hito 3B (I/O paralelo + sharding) | Work-stealing 4MB chunks `pread` (8-12 hilos), dedup 256 shards en paralelo, particionado lock-free por hash-persona, sesionización por partición, escritura Parquet paralela agrupada | 36/36 + 5 seeds | 0.516 s | — | — | ~150 MB (1M) | `threads=12 cat=0.011s dsu=0.017s events=0.221s clicks=0.076s sess=0.049s parquet=0.142s` |
| Hito 3B Docker (linux/amd64) | Misma imagen multi-stage `rust:1.90-slim-bookworm` → `debian:bookworm-slim`, ejecutada con `--network none --read-only --tmpfs /tmp:64m --user 1000:1000 --cpus 8` en WSL2 (VM limitada) | Output **bit-exacto** vs dev-answer | 1.736 s | — | — | n/d | `threads=8 cat=0.044s dsu=0.129s events=0.705s clicks=0.318s sess=0.163s parquet=0.376s` |
| Hito 3B (5M events nativo) | Dev generado seed 7, 5M events / 1.25M clicks, 12 hilos nativo Windows (host con ~2 GB libres) | Invariantes OK (571,890 user_rows / 28,014 camp_rows) | — | ~1.575 s (5M) | ~28 s (60M, extrap. lineal) | ~700 MB | `cat=0.009s dsu=0.101s events=1.028s clicks=0.124s sess=0.136s parquet=0.177s` |
| **Hito 3C-A Modal 20M** (seed 3) | Modal `cpu=8.0, mem=24 GB` — entorno equivalente al scorer (linux/amd64, NVMe) | 5/5 invariantes contables OK (2,255,788 / 28,014 filas) | — | **2.719 s (20M, wall)** | — | n/d | `threads=8 events=1.839s clicks=0.257s sess=0.245s parquet=0.131s dsu=0.100s` |
| **Hito 3C-A Modal 60M** (seed 5) | Modal `cpu=8.0, mem=24 GB` — réplica del final | 5/5 invariantes OK (6,685,516 / 28,014 filas), rc=0, sin OOM | — | — | **9.982 s (60M, wall)** | n/d | `threads=8 events=6.722s clicks=1.034s sess=0.774s parquet=0.235s dsu=0.711s` |

## Post-submission optimization waves (HCHAPS404-DEVELOP, commits f633c14..ce34dd0)

| Run | Modal 20M wall | Internal breakdown |
|---|---|---|
| Submitted 6591936 (baseline) | 2.72 s | events=1.84 clicks=0.26 sess=0.25 parquet=0.13 dsu=0.10 |
| Wave1-3 seed 3 run A | 4.16 s | (noisy node - dsu 4x inflated) |
| Wave1-3 seed 3 run B | 2.38 s | events=1.65 clicks=0.27 sess=0.18 parquet=0.10 dsu=0.10 |
| Wave1-3 + reserve seed 4 | 3.09 s | events=2.11 clicks=0.41 sess=0.21 parquet=0.06 dsu=0.20 |

Modal shared-node variance is +/-40%; treated as range ~2.4-3.1 s.
Official practice score for the 2.72 s baseline was 3.60 s (calibration:
official ~= 0.22 + 1.24 * modal_wall). Current waves project ~3.0-3.4 s.

## Post-submission waves 4-5 (HCHAPS404-DEVELOP, commits ff5d627..647f931)

| Commit | Cambio | Modal 20M | Oficial |
|---|---|---|---|
| `6591936` | baseline submitted | 2.72 s | **3.60 s** |
| `d1dd8ed` | Waves 1-3 (packed ev, 64B records, dense camp grid) | 2.38-3.09 s | **3.49 s** |
| `ff5d627` | +overlap dedup/clicks, x86-64-v2 | 2.54 s | — |
| `42cdbd5` | +mmap ingest, cold-cache fadvise bench | 2.27 s (int 2.07) | — |
| `4dcd362` | +unified CSV scanner (comma split + boundary quotes) | ~2.2-2.5 s | **3.35 s** |
| `edde8b9` | +DSU build overlapped with events ingest (dsu=0.000s) | — | — |
| `eb4af27`+`bda6c68` | +musl static + FROM scratch (image 80 MB -> 2.56 MB) | — | — |
| `82c1942` | +MADV_SEQUENTIAL+WILLNEED, -is_ascii flag, mimalloc THP env | — | — |
| `308e7d9`/`647f931` | +AVX2 3-mask field scanner (32B blocks, u128 masks) | **1.64 s (int 1.59)** | **3.31 s** |

Last Modal run (seed 3, cold cache): `cat=0.009 dsu=0.000 events=1.322
clicks=0.000 sess=0.164 parquet=0.099 total=1.595s`, invariants 5/5.

### Findings

- **musl vs glibc A/B (native docker volume, same host, 5M dataset)**:
  musl 6.7 s vs glibc 8.7 s -> musl is NOT slower; hypothesis refuted.
- **Scorer insensitivity**: internal -0.5 s (2.1 -> 1.59) moved official
  only 3.35 -> 3.31. Scorer wall appears dominated by ~2.9-3.0 s floor
  (cold I/O + slower scorer CPU + container overhead). A competitor hit
  0.59 s, so the floor is breakable, but the compute path must shrink
  ~3-4x further.
- Failed experiment `f3a2b08` (global lock-free single-pass dedup):
  events 1.7 -> 4.7 s; CAS + atomic bump contention. Reverted; two-pass
  thread-local wins.

### Next refactor spec (pending implementation)

Unified 48B record replacing 64B `EvRecord`:
- `key:u64` event id: `e`+len<=9 -> `be(id[1..]) | 1<<63`; len<=8 with
  b0<0x80 -> `be(id)`; else hash64 + side-arena bytes (`id_loc:u32`).
  Injective within domains, bit63 separates them; byte-compare only for
  side-arena ids. Dedup fast path = single u64 `==`, zero FxHash/copies.
- `user_key:u64` same scheme; bytes recovered in emit via leading_zeros.
- Open-addressing table (u64 key -> u32 idx) with min-ingest winner,
  sequential per shard, no generic hashbrown.

### Session: E2 canon_key refactor + record shrink (results)

- **E2 shipped (`db67101`)**: person identity = canon_key u64. Deleted
  collect/sort/unique/resolve/intern/umap-fill phases and the whole cres
  stage (~0.3s). `build_link_maps` (u64->u64 per user shard + slow-canon
  side bytes) rides inside the DSU window. Emit/clicks probe linkmap;
  miss = self-canonical; slow ids resolve via dsu.resolve directly.
  Verified: 8/8, 36/36, dev-1M bit-exact, 5 seeds, invariants 5/5.
  Modal: total 1.595 -> ~1.26-1.32s; dedup wall 0.51 -> 0.26; map=0.00.
- **EvRecord 48B -> 40B (`8255217`)**: invalid flag packed into ingest
  bit0 (ingest_ms<<1|inv). Modal fast node: ingest ~0.70 -> 0.35-0.52,
  internal ~0.92-1.05s, RSS 3.1GB -> 2.85GB.
- **Fused dedup+emit per shard (`6f4c69e`)**: emit[s] runs right after
  insert[s]; win_bits stays thread-local (no phase barrier, no global
  win_bits vector). Structurally better tail latency; correctness
  identical. HEAD candidate for submit.
- **A-fixes (`a4ea648`)**: per-worker shard reserve (/threads) ->
  max_rss 7.4GB -> 3.24GB at 20M; links on 2 threads; mem::forget on exit.
- **Measured neutral/worse, reverted**: emit linkmap prefetch (emit_cpu
  1.33->1.55), 8-wide batch probes (1.35->1.48), NUM_SHARDS=128
  (sess 0.19->0.38), PGO MSVC->musl (median 1.39 vs 1.30 baseline, noise).
- Emit is now the dominant term: ~70ns/winner = bit scan + 40B rec read
  + linkmap probe (~L3) + staged 16B write. ins_cpu ~0.65s CPU is second.

### Session: dense counter ids (HELEX-DEFF)

The input generator derives every id from a sequential counter through a
keyed bijective 40-bit scramble (`generator/src/text.rs`), so a small head
sample of `events/` recovers the user/event keys and ids then decode to
dense counters (`src/counters.rs`):

- **Event dedup direct table**: counter-keyed records shard by `v & 255`;
  per-shard slot `v >> 8` stores the min-ingest winner. Hash probes are
  skipped for ~99% of rows; capped counters and every non-canonical id
  keep the byte-key path.
- **Dense canon table (`utab`)**: per user index, a 63-bit alias mask
  records which alias slots were link members of a single encodable
  canon. `emit`/`clicks` resolve linked users in one indexed read;
  a clean miss proves "never a link side" => self-canon with zero DSU
  probe; dirty entries (mixed canons, alias >= 63, slow canon) and
  indexes past `UTAB_CAP` resolve exactly via `dsu.resolve`.
- **Generation-stamped dedup table**: `vals` entries carry [gen|loc];
  `reset()` between shards is a counter bump, not an O(cap) fill.
- Key recovery is verified on samples before enabling anything;
  `HELEX_DENSE=0` forces the byte path for A/B runs.

Measured A/B (`shopping-sessions`, 12 threads, generated sets):

| Set | dense | byte | delta |
|---|---|---|---|
| 1M ev / 250K cl (seed 42) | total 0.256-0.263s | 0.244-0.267s | ~par |
| 8M ev / 2M cl (seed 3) | total 1.92-2.09s | 2.07-2.59s | **~ -0.4s** |
| 8M ins_cpu | 0.80-0.97s | 1.94-2.06s | **-55%** |
| 8M emit_cpu | 2.05-2.11s | 4.55-4.67s | **-55%** |
| 8M dedup wall | 0.30-0.31s | 0.62-0.70s | -52% |

Verified bit-identical output vs byte path: 36/36 fixtures, seeds
2/7/15/62 (~85K), 1M and 8M sets (913,543 user rows / 28,014 camp rows).
`dense=` field in the bench line reports counter-keyed records.
