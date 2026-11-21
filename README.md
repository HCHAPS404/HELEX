# HELEX Shopping Sessions Pipeline

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
