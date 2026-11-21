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
