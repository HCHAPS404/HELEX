mod arena;
mod catalog;
mod csv_dialect;
mod csv_scan;
mod dsu;
mod keys;
mod pq;
mod pipeline;
mod sso;
mod timestamp;

use std::env;
use std::path::PathBuf;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let data_dir = PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let scratch_dir = PathBuf::from(env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into()));
    let debug_bench = env::var("DEBUG_BENCH").is_ok_and(|v| v == "1");

    // HELEX_REPEAT=n (benchmarking only): run the pipeline n times in one
    // process so per-phase minimums filter out host noise.
    let repeat: usize = env::var("HELEX_REPEAT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .max(1);
    for _ in 0..repeat {
        if let Err(err) = pipeline::run_pipeline(&data_dir, &out_dir, &scratch_dir, debug_bench) {
            eprintln!("shopping-sessions error: {err}");
            std::process::exit(1);
        }
    }
    std::process::exit(0);
}
