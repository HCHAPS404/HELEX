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
    # Optional merged .profdata file: builds the bench binary with
    # -Cprofile-use for PGO A/B measurements.
    pgo_profdata = os.environ.get("PGO_PROFDATA", "")
    # Extra RUSTFLAGS appended to the image build (e.g. target-cpu A/B).
    rustflags = os.environ.get("RUSTFLAGS_EXTRA", "")
    # Same, but produces a second binary at /usr/local/bin/shopping-sessions-alt
    # for build-flag A/B inside one container (bin=alt variant key).
    rustflags_alt = os.environ.get("RUSTFLAGS_ALT", "")
    # A/B variants run interleaved in the SAME container (kills node variance).
    # VARIANTS="HELEX_DENSE=1;HELEX_DENSE=0" or "bin=v2;bin=alt" or mixes
    # like "HELEX_DENSE=1+bin=alt;HELEX_DENSE=0+bin=alt". Every k=v becomes an
    # env override except bin, which selects the binary name.
    variants = os.environ.get("VARIANTS", "")

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

app = modal.App("helex-bench")

_base = (
    modal.Image.from_registry("rust:1.90-slim-bookworm", add_python="3.12")
    .apt_install("python3-pip", "time")
    .pip_install("duckdb==1.5.6")
)

if ARGS.generator_dir:
    _img = (
        _base
        .add_local_dir(os.path.join(REPO, "src"), "/build/src", copy=True)
        .add_local_file(os.path.join(REPO, "Cargo.toml"), "/build/Cargo.toml", copy=True)
        .add_local_file(os.path.join(REPO, "Cargo.lock"), "/build/Cargo.lock", copy=True)
        .add_local_dir(ARGS.generator_dir, "/gen", copy=True)
    )
    if ARGS.pgo_profdata:
        _img = _img.add_local_file(
            ARGS.pgo_profdata, "/pgo/merged.profdata", copy=True
        ).run_commands(
            'cd /build && RUSTFLAGS="-C target-cpu=x86-64-v2 '
            '-Cprofile-use=/pgo/merged.profdata" cargo build --release',
            "cp /build/target/release/shopping-sessions /usr/local/bin/",
            "cd /gen && cargo build --release && cp target/release/generate /usr/local/bin/",
        )
    else:
        _img = _img.run_commands(
            f'cd /build && RUSTFLAGS="-C target-cpu=x86-64-v2 {ARGS.rustflags}" cargo build --release',
            "cp /build/target/release/shopping-sessions /usr/local/bin/",
            "cd /gen && cargo build --release && cp target/release/generate /usr/local/bin/",
        )
        if ARGS.rustflags_alt:
            _img = _img.run_commands(
                f'cd /build && RUSTFLAGS="-C target-cpu=x86-64-v2 {ARGS.rustflags_alt}" cargo build --release',
                "cp /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions-alt",
            )
    image = _img
else:
    image = _base


@app.function(image=image, cpu=8.0, memory=24576, timeout=3600)
def bench(seed: int, events: int, clicks: int, variants: list) -> dict:
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

    base_env = {
        "PATH": os.environ["PATH"],
        "DATA_DIR": data, "OUTPUT_DIR": out, "SCRATCH_DIR": scratch,
        "HOME": scratch, "TMPDIR": scratch,
        "NCPU": "8", "MEM_GB": "24", "DEBUG_BENCH": "1",
        "MIMALLOC_LARGE_OS_PAGES": "1", "MIMALLOC_EAGER_COMMIT": "1",
        "PARSE_LEVEL": os.environ.get("PARSE_LEVEL", "5"),
    }

    def variant_env(v, idx):
        env = dict(base_env)
        for k, val in v.items():
            if k != "bin":
                env[k] = val
        # Keep each variant's output isolated; invariants check variant 0's dir.
        env["OUTPUT_DIR"] = out if idx == 0 else f"{out}-{idx}"
        os.makedirs(env["OUTPUT_DIR"], exist_ok=True)
        return env

    def variant_bin(v):
        suffix = v.get("bin", "")
        return f"shopping-sessions-{suffix}" if suffix else "shopping-sessions"

    def run_once(v, idx=0, use_time=False):
        env = variant_env(v, idx)
        cmd = ["/usr/bin/time", "-v", variant_bin(v)] if use_time else [variant_bin(v)]
        wt = time.perf_counter()
        p = subprocess.Popen(
            cmd, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        err = p.communicate()[1].decode("utf-8", "replace")
        return {
            "run_s": round(time.perf_counter() - wt, 3),
            "rc": p.returncode,
            "bench": next(
                (l for l in err.splitlines() if l.startswith("[bench]")), ""
            ).strip(),
            "stderr": err,
        }

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
    cold = run_once(variants[0], use_time=True)  # cold always on variant 0
    run_s = cold["run_s"]
    proc_rc = cold["rc"]
    stderr = cold["stderr"]

    # Warm repeats: variants interleaved round-robin in this same container so
    # node-to-node variance cancels out of the A/B comparison.
    warm_rounds = int(os.environ.get("WARM_RUNS", "2"))
    warm_per_variant = [[] for _ in variants]
    for _ in range(warm_rounds):
        for i, v in enumerate(variants):
            r = run_once(v, idx=i)
            r.pop("stderr", None)
            warm_per_variant[i].append(r)

    warm_runs = [
        {"variant": variants[i], "runs": rs,
         "median_s": sorted(r["run_s"] for r in rs)[len(rs) // 2] if rs else None}
        for i, rs in enumerate(warm_per_variant)
    ]

    # I/O floor probe: drop cache again, then sequentially read events CSVs
    # to measure the node's raw disk floor for the input volume.
    for root, _, files in os.walk("/data"):
        for fn in files:
            try:
                fd = os.open(os.path.join(root, fn), os.O_RDONLY)
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                os.close(fd)
            except OSError:
                pass
    ev_files = [
        os.path.join(root, fn)
        for root, _, files in os.walk(os.path.join(data, "events"))
        for fn in files
    ]
    ev_bytes = 0
    t2 = time.perf_counter()
    for f in ev_files:
        with open(f, "rb", buffering=0) as fh:
            while fh.read(8 << 20):
                pass
        ev_bytes += os.path.getsize(f)
    io_floor = time.perf_counter() - t2

    v0_runs = [run_s] + [r["run_s"] for r in warm_per_variant[0]]
    result = {
        "seed": seed, "events": events, "gen_s": round(gen_s, 2),
        "run_s": round(run_s, 3), "rc": proc_rc,
        "warm_runs": warm_runs,
        "median_run_s": round(sorted(v0_runs)[len(v0_runs) // 2], 3),
        "io_floor_s": round(io_floor, 3),
        "events_mb": round(ev_bytes / 2**20, 1),
        "stderr_tail": stderr[-4000:],
    }
    if proc_rc == 0:
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


def _parse_variants(spec: str) -> list:
    # "HELEX_DENSE=1;HELEX_DENSE=0" -> [{"HELEX_DENSE": "1"}, {"HELEX_DENSE": "0"}]
    # "+" joins several overrides in one variant: "bin=alt+HELEX_DENSE=0"
    out = []
    for var in spec.split(";"):
        var = var.strip()
        if not var:
            continue
        out.append(dict(kv.split("=", 1) for kv in var.split("+")))
    return out or [{}]


@app.local_entrypoint()
def main() -> None:
    res = bench.remote(
        ARGS.seed, ARGS.events, ARGS.clicks, _parse_variants(ARGS.variants)
    )
    print(res)
    sys.exit(0 if res["rc"] == 0 else 1)
