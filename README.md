# HELEX — Shopping Sessions 4 (Rust engine)

Implementación nativa en Rust del benchmark `shopping-sessions-4` (reto
Databricks del Builders Buildathon). Reemplaza el baseline PySpark (~172 s).

**Score oficial vigente: 2.75 s** (motor anterior, commit `ead4d6c`). Este
README describe el motor reescrito en la rama `perf/sub-1.5s`; su objetivo es
bajar de 1.5 s en el scorer oficial. Aún no tiene score oficial.

## Arquitectura (`src/pipeline.rs`)

1. `catalog.rs`: ventana del manifest, productos/categorías y campañas.
2. `keys.rs`: claves `u64` exactas para IDs cortos (`prefijo + ≤8 bytes`, o
   `≤8 bytes`). Los IDs largos o no ASCII usan `0xFF<<56 | hash56` y un
   registro de bytes para recuperarlos en la salida.
3. `dsu.rs`: links → persona canónica (union-find sobre claves `u64`, gana el
   ID byte a byte menor), más un filtro de bits que evita la búsqueda para
   usuarios sin links.
4. **Fase 1 (parseo)**: los archivos se cortan en chunks de 256 KiB
   (`mmap` + `MADV_POPULATE_READ`; `pread` en las colas de archivo).
   `csv_scan.rs` arma en un pasada AVX2 los bitmaps de `\n` `,` `"` del
   chunk, y los campos se leen de ventanas de 128 bits. Las líneas con
   comillas caen al splitter genérico exacto. Timestamps F1 con SWAR.
   - Eventos → `EvRec` de 32 B (`key, ingest, user, ev`) repartidos por shard
     de `event_id`.
   - Clicks → resueltos a persona canónica, `CkRec` repartidos por partición
     de persona.
5. `arena.rs`: los registros van a bloques de 16 KiB de una arena mmap con THP.
   Nada de realloc/memmove, y los bloques consumidos se reciclan en la fase
   siguiente para no poner a cero memoria nueva.
6. **Fase 2 (dedup)**: por shard (~10k registros), tabla open-addressing
   intercalada `[key, addr]` en L2. Gana el menor `ingest` (con inválidos).
   Los ganadores se emiten releyendo los registros en orden secuencial como
   `(persona, ev)` de 16 B, repartidos por partición de persona.
7. **Fase 3 (sesiones)**: por partición (~40k registros), counting sort por
   persona, sesionización, atribución de clicks y agregados por campaña/día.
   `user_daily` se escribe en streaming (un row group por bloque de ~64k
   filas, un archivo por worker).
8. `parquet_out.rs`: Parquet sin compresión, sin diccionario, sin stats.

Los conteos de shards y particiones escalan con el tamaño de la entrada
(`HELEX_S1_TARGET`, `HELEX_P_TARGET`). Otras perillas para A/B:
`HELEX_IO=pread`, `HELEX_THP=0`, `HELEX_CHUNK_KB`, `NCPU`.

## Build / Docker

El binario se compila **estático con glibc** (`+crt-static`) y corre
`FROM scratch`. musl salió ~15 % más lento por su `memcpy`/`memset`.

```bash
cargo build --release
make build          # imagen docker linux/amd64
```

## Verificación (obligatoria tras cada cambio)

```bash
cargo test --release
uv run --with polars python scripts/run_fixtures.py fixtures -- target/release/shopping-sessions   # 36/36
```

Datos sintéticos para medir y comparar (A/B de salida entre dos binarios):

```bash
cargo build --release --manifest-path tools/synthgen/Cargo.toml
tools/synthgen/target/release/synthgen /tmp/d20 20000000 5000000 3
SYNTH_EDGE=1 tools/synthgen/target/release/synthgen /tmp/dedge 3000000 1500000 11   # IDs largos/UTF-8, click ids largos, empates
DATA_DIR=/tmp/d20 OUTPUT_DIR=/tmp/out_a ./bin_a; DATA_DIR=/tmp/d20 OUTPUT_DIR=/tmp/out_b ./bin_b
uv run --with polars python scripts/compare.py /tmp/out_a /tmp/out_b
```

`synthgen` **no** es el generador oficial: aproxima su forma (tamaños,
duplicados, inválidos, formatos de timestamp, links, clicks) y sirve solo para
medir tiempos y comparar salidas entre builds, no como verdad de referencia.

Telemetría: el binario imprime a stderr
`[bench] threads cat dsu parse dedup sess out total user_rows camp_rows`.

## Reglas de corrección intocables

- Dedup global por `event_id`: gana el menor `ingest_ms`, aunque sea inválido.
- Persona canónica = menor ID (bytes) del conjunto linkado.
- Usuarios que solo aparecen en clicks resuelven vía DSU.
- 60M eventos deben correr en <24 GB (el motor nuevo usa ~0.5× los bytes
  del CSV en memoria anónima).
- stdout vacío; telemetría solo a stderr.

## Lecciones del scorer (motor anterior)

El hardware del scorer es mucho más sensible a **patrones de acceso a memoria**
y a la memoria nueva tocada que a la CPU pura. Premia una sola corriente
secuencial por pasada y menos RSS. Castiga los streams intercalados y la
memoria comprometida de más. El motor nuevo baja el RSS anónimo de ~4.8 GB a
~1.2 GB en 20M eventos.

Historial completo de mediciones: `BENCHMARKS.md`.
