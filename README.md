# HELEX — Shopping Sessions 4 (Rust engine)

Implementación nativa en Rust del benchmark `shopping-sessions-4`. Reemplaza
el baseline PySpark (~172s) por un pipeline nativo de ~1s interno.

**Score oficial actual: 2.75s** (leaderboard conserva el mejor envío).
**Candidato pendiente de submit: `6f4c69e`** (rama `HCHAPS404-DEVELOP`) —
interno ~0.9–1.3s en Modal, estimado ~2.0–2.4s oficial.

## Estado del repo

| Rama | Contenido |
|---|---|
| `main` | Merge estable del motor |
| `HCHAPS404-DEVELOP` | Rama de desarrollo activa (trabajo de optimización) |
| `dsu-integration` | Trabajo original del equipo (DSU/links) — preservada |

## Arquitectura actual (post-E2, `src/pipeline.rs`)

1. `catalog.rs` — ventana temporal + catálogo/categorías.
2. `dsu.rs` — links usuario→persona canónica (union-find, ID lexicográfico
   menor). `pair_at(i)` expone pares (raw→canon) internados.
3. `io_pool.rs` — pool de workers que lee CSVs con `mmap` (orden LPT por
   tamaño, `MADV_SEQUENTIAL`/`WILLNEED`).
4. `csv_dialect.rs` — splitter de líneas/campos con ruta **AVX2** de 3 máscaras
   (detección runtime; fallback escalar para x86-64-v2).
5. `timestamp.rs` — parser F1/F2/F3 con LUT de días-epoch.
6. Eventos: parse → `EvRecord` de **40B** (key|ingest|user_key|ev|id_loc|
   user_loc). Claves `u64` inline vía `field_key` (prefijo + ≤8B ASCII caben
   en u64 big-endian con tag); ids no codificables → arena lateral + hash64.
   El flag `invalid` viaja en el bit 0 de `ingest` (`ingest_ms<<1|inv`,
   orden preservado).
7. `build_link_maps` (dentro de la ventana del DSU, ~gratis): mapas
   `user_key → canon_key` (open-addressing u64→u64) por shard de usuario +
   tablas laterales de bytes para canónicos no codificables.
   **La identidad de persona ES `canon_key` u64** — no hay interning denso.
8. Dedup global por `event_id`, **fusionado con emit por shard**: insert en
   `DedupTab` open-addressing (u64→loc empaquetado [invalid|buf|rec]), gana
   el menor `ingest`; `win_bits` bitmap → barrido secuencial → probe linkmap
   (miss = auto-canónico; usuarios no-codificables → `dsu.resolve` directo)
   → `Rec16` (canon_key|ev, 16B) a particiones `canon_key>>57..` (staging 4B
   por partición). Sin barrera de fase: emit[s] corre tras insert[s].
9. Clicks: resueltos a `canon_key` en el parse (mismo probe linkmap; la
   etapa `cres` ya no existe) y particionados por canon shard.
10. `process_partition`: concatena `Rec16` de la partición, ordena por
    (canon_key, ev) → grupos por persona pre-ordenados + cursor paralelo en
    clicks ordenados; bytes canónicos vía side-list (canons lentos) o decode
    de la clave. Sin hashmap de personas.
11. `parquet_out.rs` — salida Parquet sin compresión, sin dict, sin stats.

## Verificación (obligatoria tras cada cambio)

```bash
cargo test --release                                        # 8/8
uv run --with polars python scripts/run_fixtures.py fixtures -- target/release/shopping-sessions   # 36/36
uv run --with polars python scripts/verify_seeds.py --bin target/release/shopping-sessions \
  --generate generator_extracted/generator/target/release/generate.exe \
  --dev <dev-1M-dir> --answer dev-answer                    # bit-exact + 5 seeds
```

Modal 20M (8 vCPU / 24GB como el scorer, 3 corridas con mediana):
`cd scripts && GEN_DIR=../generator_extracted/generator uv run --with modal modal run modal_bench.py`
Env: `PARSE_LEVEL=1..5` bisecta el costo del parse; `PGO_PROFDATA=<file>` A/B de PGO.

Telemetría: el binario imprime `[bench] ... avx2={0|1} cat dsu ingest dedup
ins_cpu map emit_cpu clicks cres events sess parquet total` a stderr
**incondicionalmente** — visible en los logs del scorer.

## Historial de resultados

| Commit | Cambio | Modal interno | Oficial |
|---|---|---|---|
| `6591936` | motor base | ~2.6s | 3.60 |
| `d1dd8ed` | mejoras varias | ~2.5s | 3.49 |
| `4dcd362` | dsu overlap | ~2.1s | 3.35 |
| `647f931` | AVX2 3-máscaras CSV | 1.59s | 3.31 |
| `ca615d9` | records 48B + tabla u64 | 1.58s | 3.33 |
| `5783ec2` | split hot/cold (REVERTIDO) | 1.69s | **2.94 ❌** |
| `ead4d6c` | bitmap secuencial + staging + fusión collect→insert | ~1.66s | **2.75 ✅** |
| `1004df9` | + prefetch de slots dedup | — | **3.41 ❌** |
| `a4ea648` | reserva por worker + links 2 hilos + forget-on-exit | ~1.6s (RSS 7.4→3.2GB) | — |
| `db67101` | **E2: canon_key u64** (elimina fases map/cres) | ~1.26–1.32s | — |
| `8255217` | EvRecord 48→40B | ~0.92–1.05s¹ | — |
| `6f4c69e` | dedup+emit fusionados por shard | ~0.9–1.3s | **candidato** |

¹ nodo Modal rápido (gen_s=2.2s); en nodos típicos ~1.15–1.3s. La varianza
de nodos Modal es ±30–50% — comparar solo corridas del mismo nodo.

## Lo que el scorer parece premiar / penalizar (hipótesis con evidencia)

El hardware del scorer es **mucho más sensible a memoria que a CPU**:

✅ Premia: una sola corriente secuencial por pasada, bitmaps en vez de
accesos aleatorios, menos sweeps sobre DRAM, menos bytes por record
(48→40B midió bien), menos páginas comprometidas (RSS bajo).

❌ Penaliza: streams intercalados (split hot/cold = 2 líneas por iteración:
2.94 vs 2.75), prefetch especulativo que añade trabajo (`1004df9`: 3.41),
picos de RSS (10.8GB del split).

## NO hacer (ya medido, no reintentar)

- **Dedup con CAS/atómicos globales** — ~2.7× más lento (`f3a2b08`).
- **Zero-copy sobre mmap con punteros** — TLB thrashing.
- **Bitmap compartido entre threads para dedup** — contención + bugs.
- **musl para ganar velocidad** — A/B real: no es el cuello.
- **Split hot/cold de records** — 2.94 (peor que 2.75).
- **Prefetch de slots (dedup o linkmap)** — medido peor dos veces: los
  slots ya son calientes o el costo extra supera al miss evitado
  (`1004df9` → 3.41 oficial; emit prefetch → emit_cpu 1.33→1.55 Modal).
- **Batch de probes MLP en emit** — emit_cpu 1.35→1.48 Modal: la OOO ya
  solapa los probes; el gather manual solo añade trabajo.
- **`NUM_SHARDS=128`** — sess 0.19→0.38 (particiones mayores = sort peor);
  256 es el punto óptimo medido.
- **PGO** — MSVC-train → musl-build: neutro en Modal (mediana 1.39 vs 1.30
  baseline, dentro del ruido de nodos). No compensa la maquinaria Docker.
- **Dedup por persona antes del dedup global** — rompe reglas (el dedup es
  por `event_id`, gana menor `ingest` aunque sea inválido).
- **Asumir que usuarios de clicks aparecen en events** — hay click-only
  users; requieren fallback DSU (bug real en `696c55f`, corregido; en el
  diseño canon_key actual es estructuralmente imposible: clicks y events
  resuelven por el mismo linkmap/dsu).
- **Segunda pasada radix** — sin beneficio medido.
- **`gather` AVX2 (vpgatherqq)** — microcode lento, no compensa.

## Reglas de corrección intocables

- Dedup global por `event_id`, gana el menor `ingest_ms` (incluso inválidos).
- Persona canónica = menor ID lexicográfico (bytes) del conjunto linkado.
- Usuarios click-only resuelven vía DSU (canon por bytes, no por clave).
- Spill a `/scratch` debe seguir funcionando a 60M eventos (RAM < 24GB).
- stdout vacío siempre; telemetría solo a stderr.

## Cola de optimización (siguientes candidatos, por EV)

1. `emit_cpu` (~1.4s CPU) es el término dominante restante: ~70ns/winner de
   bit-scan + lectura de records + probe linkmap + write particionado.
   Ideas abiertas: reducir probe con mapa más caliente, o saltar resolución
   para usuarios no-linkados vía bitset denso.
2. Radix sort en `process_partition` (sort (canon_key,ev) de ~76k elems →
   u64x2 empaquetado en u128 o LSD radix) — `sess` ~0.19s.
3. SWAR en parse de timestamps (`ingest` ~0.35–0.7s según nodo).
4. Spill path: recalibrar `should_spill_events` al layout de 40B + RSS ~3GB
   (el límite real para 60M hidden).
5. Pipelining ingest→dedup: hoy el dedup espera a que TODA la ingest termine;
   arrancarlo por shard cuando su data esté lista requiere tracking de
   completitud en io_pool (cambio arquitectónico, ~0.15–0.25s potencial).

Plan y experimentos por etapas: `BENCHMARKS.md` + mensajes de commit de
`HCHAPS404-DEVELOP` (cada experimento documenta su medición).
