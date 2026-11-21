mod catalog;
mod csv_dialect;
mod dsu;
mod parquet_out;
mod pipeline;
mod timestamp;

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    let data_dir = PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let scratch_dir = PathBuf::from(env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into()));
    let debug_bench = env::var("DEBUG_BENCH").is_ok_and(|v| v == "1");

    if let Err(err) = pipeline::run_pipeline(&data_dir, &out_dir, &scratch_dir, debug_bench) {
        eprintln!("shopping-sessions error: {err}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
