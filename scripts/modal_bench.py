#!/usr/bin/env python3
"""
Benchmark the HELEX Rust engine on Modal at scorer-like limits (8 vCPU, 24 GB).

Prereqs (client side, never in chat):
    pip install modal duckdb
    modal token set            # or: modal token new

Usage:
    python scripts/modal_bench.py --generator-dir /path/to/generator \
        --seed 3 --events 60000000 --clicks 15000000

The function compiles the engine + generator inside a rust:1.90 image,
generates the dataset on Modal (no CSV upload needed), runs the engine with
the scorer's env vars, and reports wall time and peak cgroup RSS.
"""
import argparse
import sys
import time

import modal

app = modal.App("helex-bench")


def build_image(generator_dir: str):
    return (
        modal.Image.from_registry("rust:1.90-slim-bookworm", add_python="3.12")
        .apt_install("python3-pip")
        .pip_install("duckdb==1.5.6")
        .add_local_dir("src", "/build/src")
        .add_local_file("Cargo.toml", "/build/Cargo.toml")
        .add_local_dir(generator_dir, "/gen")
        .run_commands(
            "cd /build && cargo build --release",
            "cp /build/target/release/shopping-sessions /usr/local/bin/",
            "cd /gen && cargo build --release && cp target/release/generate /usr/local/bin/",
        )
    )


@app.function(cpu=8.0, memory=24576, timeout=3600)
def run_bench(seed: int, events: int, clicks: int, check_invariants: bool) -> dict:
    import os
    import subprocess
    import threading

    data, out, scratch = "/data", "/out", "/scratch"
    for d in (data, out, scratch):
        os.makedirs(d, exist_ok=True)

    t0 = time.perf_counter()
    subprocess.run(
        [
            "generate", "--seed", str(seed), "--events", str(events),
            "--clicks", str(clicks), "--output", data,
        ],
        check=True,
    )
    gen_s = time.perf_counter() - t0

    env = {
        "DATA_DIR": data, "OUTPUT_DIR": out, "SCRATCH_DIR": scratch,
        "HOME": scratch, "TMPDIR": scratch,
        "NCPU": "8", "MEM_GB": "24", "DEBUG_BENCH": "1",
    }

    peak = [0]

    def poll_mem():
        try:
            while proc.poll() is None:
                try:
                    cur = int(open("/sys/fs/cgroup/memory.current").read())
                    peak[0] = max(peak[0], cur)
                except OSError:
                    pass
                time.sleep(0.025)
        except Exception:
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

    if proc.returncode == 0 and check_invariants:
        import duckdb

        con = duckdb.connect()
        u = con.execute(
            "SELECT sum(sessions) s, sum(revenue_cents) r FROM read_parquet('/out/user_daily/*.parquet')"
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
            "user_rows": u[0],
        }
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--generator-dir", required=True, help="Local path to extracted generator/ dir")
    ap.add_argument("--seed", type=int, default=3)
    ap.add_argument("--events", type=int, default=20_000_000)
    ap.add_argument("--clicks", type=int, default=5_000_000)
    args = ap.parse_args()

    image = build_image(args.generator_dir)

    @app.function(cpu=8.0, memory=24576, timeout=3600, image=image)
    def _bench(seed: int, events: int, clicks: int) -> dict:
        import os
        import subprocess
        import threading
        import time as _time

        data, out, scratch = "/data", "/out", "/scratch"
        for d in (data, out, scratch):
            os.makedirs(d, exist_ok=True)

        t0 = _time.perf_counter()
        subprocess.run(
            ["generate", "--seed", str(seed), "--events", str(events),
             "--clicks", str(clicks), "--output", data],
            check=True,
        )
        gen_s = _time.perf_counter() - t0

        env = {
            "DATA_DIR": data, "OUTPUT_DIR": out, "SCRATCH_DIR": scratch,
            "HOME": scratch, "TMPDIR": scratch,
            "NCPU": "8", "MEM_GB": "24", "DEBUG_BENCH": "1",
        }
        peak = [0]

        def poll_mem():
            try:
                while proc.poll() is None:
                    try:
                        cur = int(open("/sys/fs/cgroup/memory.current").read())
                        peak[0] = max(peak[0], cur)
                    except OSError:
                        pass
                    _time.sleep(0.025)
            except Exception:
                pass

        t1 = _time.perf_counter()
        proc = subprocess.Popen(
            ["shopping-sessions"], env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        th = threading.Thread(target=poll_mem)
        th.start()
        stderr = proc.communicate()[1]
        th.join()
        run_s = _time.perf_counter() - t1

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

    with modal.enable_output():
        with app.run():
            res = _bench.remote(args.seed, args.events, args.clicks)
    print(res)
    return 0 if res["rc"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
