# HELEX — Shopping Sessions 4 (Rust engine)

Implementación nativa en Rust del benchmark `shopping-sessions-4`. Reemplaza
el baseline PySpark (~172s) por un pipeline de un solo paso en Rust.

**Score oficial actual: 2.75s** (leaderboard conserva el mejor envío).

## Estado del repo

| Rama | Contenido |
|---|---|
| `main` | Merge estable del motor (este estado) |
| `HCHAPS404-DEVELOP` | Rama de desarrollo activa (trabajo de optimización) |
| `dsu-integration` | Trabajo original del equipo (DSU/links) — preservada |

Mejor commit medido en el scorer: `ead4d6c` (= contenido de `9cf82d4`/`1004df9`
más cambios neutros posteriores).

## Arquitectura (pipeline, `src/pipeline.rs`)

1. `catalog.rs` — ventana temporal + catálogo/categorías.
2. `dsu.rs` — links usuario→persona canónica (union-find, ID lexicográfico menor).
3. `io_pool.rs` — pool de workers que lee CSVs con `mmap` (orden LPT por tamaño,
   `MADV_SEQUENTIAL`/`WILLNEED`).
4. `csv_dialect.rs` — splitter de líneas/campos con ruta **AVX2** de 3 máscaras
   (detección runtime; fallback escalar para x86-64-v2).
5. `timestamp.rs` — parser F1/F2/F3 con LUT de días-epoch.
6. Eventos: parse → `EvRecord` de **48B** con claves `u64` inline
   (`field_key`: `'e'+len<=9` y `'u'+len<=9` caben en u64 big-endian con tag;
   el resto va a arena lateral + hash64).
7. Dedup global por `event_id`: `DedupTab` open-addressing (u64→loc) por shard;
   gana el copy con menor `ingest_ms` (incluido si es inválido).
   `win_bits` bitmap marca ganadores; el barrido posterior es **secuencial**.
8. Interning denso: `user_key → person_idx` vía `UMap` (open-addressing u64)
   construido una sola vez; clicks con fallback DSU para usuarios click-only.
9. `Rec12` (16B: person_idx|ev) → particiones densas por persona;
   `Vec<Vec<u64>>` indexado por `person_idx`, sin hashing en sesión.
10. `parquet_out.rs` — salida Parquet sin compresión, sin dict, sin stats.

## Verificación (obligatoria tras cada cambio)

```bash
cargo test --release                                        # 8/8
uv run --with polars python scripts/run_fixtures.py fixtures -- target/release/shopping-sessions   # 36/36
uv run --with polars python scripts/verify_seeds.py --bin target/release/shopping-sessions \
  --generate generator_extracted/generator/target/release/generate.exe \
  --dev <dev-1M-dir> --answer dev-answer                    # bit-exact
```

Modal 20M (nodo similar al scorer):
`cd scripts && GEN_DIR=../generator_extracted/generator uv run --with modal modal run modal_bench.py`

Telemetría: el binario imprime `[bench] ... avx2={0|1} cat dsu ingest dedup
ins_cpu map emit_cpu clicks cres events sess parquet total` a stderr
**incondicionalmente** — visible en los logs del scorer.

## Historial de resultados (scorer oficial)

| Commit | Cambio | Modal | Oficial |
|---|---|---|---|
| `6591936` | motor base | ~2.6s | 3.60 |
| `d1dd8ed` | mejoras varias | ~2.5s | 3.49 |
| `4dcd362` | dsu overlap | ~2.1s | 3.35 |
| `647f931` | AVX2 3-máscaras CSV | 1.59s | 3.31 |
| `ca615d9` | records 48B + tabla u64 | 1.58s | 3.33 |
| `5783ec2` | split hot/cold (REVERTIDO) | 1.69s | **2.94 ❌** |
| `ead4d6c` | bitmap secuencial + staging + fusión collect→insert | ~1.66s | **2.75 ✅** |
| `9cf82d4` | revert hot/cold (= `ead4d6c` semánticamente) | — | — |
| `1004df9` | + prefetch de slots dedup | — | candidato |

## Lo que el scorer premia / penaliza (¡importante, Alex!)

El hardware del scorer es mucho más sensible a **patrones de acceso a memoria**
que a CPU pura:

✅ **Premia**: una sola corriente secuencial por pasada, bitmaps en vez de
accesos aleatorios, writes en bloques (staging), menos sweeps sobre DRAM.

❌ **Penaliza**: streams intercalados (el split hot/cold 24B+24B leía
hot[i]+cold[i] por iteración = 2 streams: −0.19s **peor** pese a tocar menos
bytes), más RSS/páginas comprometidas.

## NO hacer (ya medido, no reintentar)

- **Dedup con CAS/atómicos globales** — ~2.7× más lento (`f3a2b08`).
- **Zero-copy sobre mmap con punteros** — TLB thrashing, peor.
- **Bitmap compartido entre threads para dedup** — contención + bugs de
  corrección.
- **musl para ganar velocidad** — A/B real: musl no es el cuello (musl 6.7s vs
  glibc 8.7s en el mismo host; el scorer corre musl pero eso no es el problema).
- **Split hot/cold de records** — medido: 2.94 (peor que 2.75). Ver arriba.
- **Dedup por usuario/persona antes del dedup global por event_id** — rompe
  reglas: el dedup es global por `event_id` y el copy ganador es el de menor
  `ingest_ms` aunque sea inválido.
- **Asumir que usuarios de clicks aparecen en events** — hay click-only users;
  requiere fallback DSU (bug real en `696c55f`, ya corregido).
- **Segunda pasada radix** — medido sin beneficio.
- **`gather` AVX2 (vpgatherqq)** para probes — microcode lento, no compensa.

## Reglas de corrección intocables

- Dedup global por `event_id`, gana el menor `ingest_ms` (incluso inválidos).
- Persona canónica = menor ID lexicográfico (bytes) del conjunto linkado.
- Usuarios click-only resuelven vía DSU.
- Spill a `/scratch` debe seguir funcionando a 60M eventos (guardar RAM<24GB).
- stdout vacío siempre; telemetría solo a stderr.

## Cola de optimización (siguientes candidatos)

1. ukey open-set por u-shard (elimina collect de ~19.4M pushes + sort, ~300MB).
2. PGO en Dockerfile (profile-generate → run → profile-use).
3. `NUM_SHARDS` A/B (menos streams de escritura).
4. SWAR en parse de timestamps.
5. Recalibrar `should_spill_events` al layout actual (RSS ~10.8GB a 20M).

Plan completo por etapas: ver `BENCHMARKS.md` y el historial de commits de
`HCHAPS404-DEVELOP` (mensajes documentan cada experimento y su resultado).

---

## Pipeline legacy (DuckDB) — descripción original de Alex

> Nota: el motor de producción actual es el pipeline Rust de arriba.
> Se conserva esta sección como referencia del enfoque anterior.


This submission implements the Shopping Sessions specification in
`rules.md`. It combines a Rust CSV and identity preprocessor with DuckDB
sessionization, attribution, aggregation, and Parquet output.

## Validation

The implementation passed all 36 edge-case fixtures and the 1M-event public
development dataset exactly.

Run the fixtures without Docker:

```sh
pip install duckdb==1.5.6 polars
python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py
```

## Runtime layout

- `DATA_DIR`: mounted input CSVs and `manifest.json`
- `OUTPUT_DIR`: destination for `user_daily/` and `campaign_daily/`
- `SCRATCH_DIR`: temporary CSV staging and DuckDB spill files
- `NCPU`: thread count
- `MEM_GB`: memory limit

The container entrypoint is [run.sh](/workspaces/HELEX/run.sh).
