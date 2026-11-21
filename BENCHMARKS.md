# HELEX — Benchmarks & Optimization Log (`shopping-sessions-4`)

Hardware de pruebas local: Windows 11 / WSL2 Linux `amd64` (12 logical CPUs, 12 GB host RAM, Docker `--cpus=8 --memory=4g --memory-swap=4g`).
Hardware oficial del evaluador: GCP `n2` Intel Xeon (`linux/amd64`), 8 vCPUs, 24 GB RAM, sin swap.

| Iteración / Commit | Motor / Descripción | Fixtures (36) | 1M Dev (`seed 1`) | 20M Práctica (`seed 2`) | 60M Final (`seed 3`) | Pico RSS | Notas |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| `48d7f0e` (`dsu-integration`) | Rust DSU single-thread -> staging CSV -> Python + DuckDB 1.5.6 | 36/36 | 9.172 s | ~183 s (est.) | >500 s (TIMEOUT) | ~2.5 GB + 5 GB CSV | Reescribe todos los CSV a `/scratch` y ejecuta doble window function + range join en DuckDB |
| Hito 2 MVP (Rust 1-thread) | Motor nativo Rust 6 etapas (single-thread, Parquet directo sin Arrow, mimalloc) | 36/36 + 5 seeds | 1.492 s | — | — | ~180 MB (1M) | `cat=0.011s dsu=0.034s events=1.356s clicks=0.336s sess=0.081s parquet=0.027s` |
