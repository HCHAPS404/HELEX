#!/usr/bin/env python3
"""Benchmark HELEX Rust engine on Modal at scorer limits (8 vCPU, 24 GB).

    pip install modal duckdb
    modal token set
    python scripts/modal_bench.py --generator-dir /path/to/generator \
        --seed 3 --events 20000000 --clicks 5000000
"""
import os
import sys
import modal

class ARGS:
    generator_dir = os.environ.get("GEN_DIR", "")
    seed = int(os.environ.get("SEED", "3"))
    events = int(os.environ.get("EVENTS", "20000000"))
    clicks = int(os.environ.get("CLICKS", "5000000"))

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

app = modal.App("helex-bench")

_base = (
    modal.Image.from_registry("rust:1.90-slim-bookworm", add_python="3.12")
    .apt_install("python3-pip")
    .pip_install("duckdb==1.5.6")
)

if ARGS.generator_dir:
    image = (
        _base
        .add_local_dir(os.path.join(REPO, "src"), "/build/src", copy=True)
        .add_local_file(os.path.join(REPO, "Cargo.toml"), "/build/Cargo.toml", copy=True)
        .add_local_file(os.path.join(REPO, "Cargo.lock"), "/build/Cargo.lock", copy=True)
        .add_local_dir(ARGS.generator_dir, "/gen", copy=True)
        .run_commands(
            'cd /build && RUSTFLAGS="-C target-cpu=x86-64-v2" cargo build --release',
            "cp /build/target/release/shopping-sessions /usr/local/bin/",
            "cd /gen && cargo build --release && cp target/release/generate /usr/local/bin/",
        )
    )
else:
    image = _base


@app.function(image=image, cpu=8.0, memory=24576, timeout=3600)
def bench(seed: int, events: int, clicks: int) -> dict:
    import os
    import subprocess
    import threading
    import time

    data, out, scratch = "/data", "/out", "/scratch"
    for d in (data, out, scratch):
        os.makedirs(d, exist_ok=True)

    t0 = time.perf_counter()
    subprocess.run(
        ["generate", "--seed", str(seed), "--events", str(events),
         "--clicks", str(clicks), "--output", data],
        check=True,
    )
    gen_s = time.perf_counter() - t0

    env = {
        "PATH": os.environ["PATH"],
        "DATA_DIR": data, "OUTPUT_DIR": out, "SCRATCH_DIR": scratch,
        "HOME": scratch, "TMPDIR": scratch,
        "NCPU": "8", "MEM_GB": "24", "DEBUG_BENCH": "1",
    }
    peak = [0]

    def poll_mem():
        try:
            while proc.poll() is None:
                try:
                    peak[0] = max(peak[0], int(open("/sys/fs/cgroup/memory.current").read()))
                except OSError:
                    pass
                time.sleep(0.025)
        except Exception:
            pass

    # Drop page cache on /data so CSV reads hit disk like the scorer's cold start.
    for root, _, files in os.walk("/data"):
        for fn in files:
            try:
                fd = os.open(os.path.join(root, fn), os.O_RDONLY)
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                os.close(fd)
            except OSError:
                pass

    t1 = time.perf_counter()
    proc = subprocess.Popen(
        ["shopping-sessions"], env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    th = threading.Thread(target=poll_mem)
    th.start()
    stderr = proc.communicate()[1]
    th.join()
    run_s = time.perf_counter() - t1

    result = {
        "seed": seed, "events": events, "gen_s": round(gen_s, 2),
        "run_s": round(run_s, 3), "rc": proc.returncode,
        "peak_rss_mib": round(peak[0] / 2**20, 1),
        "stderr_tail": stderr.decode(errors="replace")[-1500:],
    }
    if proc.returncode == 0:
        import duckdb

        con = duckdb.connect()
        u = con.execute(
            "SELECT sum(sessions) s, sum(revenue_cents) r "
            "FROM read_parquet('/out/user_daily/*.parquet')"
        ).fetchone()
        c = con.execute(
            "SELECT sum(sessions) s, sum(revenue_cents) r, sum(assisted_sessions) a, "
            "sum(CASE WHEN campaign_id<>'organic' THEN sessions ELSE 0 END) ns, "
            "sum(CASE WHEN campaign_id='organic' THEN assisted_sessions ELSE 0 END) oa, "
            "sum(CASE WHEN sessions=0 AND assisted_sessions=0 THEN 1 ELSE 0 END) empty "
            "FROM read_parquet('/out/campaign_daily/*.parquet')"
        ).fetchone()
        result["invariants"] = {
            "sessions_match": u[0] == c[0],
            "revenue_match": u[1] == c[1],
            "assisted_ge_credited": c[2] >= c[3],
            "organic_assist_zero": (c[4] or 0) == 0,
            "no_empty_rows": (c[5] or 0) == 0,
        }
    return result


@app.local_entrypoint()
def main() -> None:
    res = bench.remote(ARGS.seed, ARGS.events, ARGS.clicks)
    print(res)
    sys.exit(0 if res["rc"] == 0 else 1)
