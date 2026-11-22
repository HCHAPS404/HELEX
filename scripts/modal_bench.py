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
    # Optional: dir with {src/, Cargo.toml, Cargo.lock} for variant A.
    # The repo itself builds variant B. Set HELEX_AB_N for interleaved pairs.
    baseline_dir = os.environ.get("BASELINE_DIR", "")

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Extra env for the engine, captured locally at upload time.
ENV_KEYS = ("HELEX_S1_TARGET", "HELEX_P_TARGET", "HELEX_CHUNK_KB",
            "HELEX_IO", "HELEX_THP", "HELEX_RUNS", "HELEX_PL",
            "HELEX_PL_SWEEP", "HELEX_AB_N")
EXTRA_ENV = {k: os.environ[k] for k in ENV_KEYS if k in os.environ}

app = modal.App("helex-bench")

_base = (
    modal.Image.from_registry("rust:1.90-slim-bookworm", add_python="3.12")
    .apt_install("python3-pip", "time")
    .pip_install("duckdb==1.5.6")
)

if ARGS.generator_dir:
    image = (
        _base
        .add_local_dir(os.path.join(REPO, "src"), "/build_b/src", copy=True)
        .add_local_file(os.path.join(REPO, "Cargo.toml"), "/build_b/Cargo.toml", copy=True)
        .add_local_file(os.path.join(REPO, "Cargo.lock"), "/build_b/Cargo.lock", copy=True)
        .add_local_dir(ARGS.generator_dir, "/gen", copy=True)
        .run_commands(
            'cd /build_b && RUSTFLAGS="-C target-cpu=x86-64-v2" cargo build --release',
            "cp /build_b/target/release/shopping-sessions /usr/local/bin/ss-b",
            "cd /gen && cargo build --release && cp target/release/generate /usr/local/bin/",
        )
    )
    if ARGS.baseline_dir:
        image = (
            image
            .add_local_dir(os.path.join(ARGS.baseline_dir, "src"), "/build_a/src", copy=True)
            .add_local_file(os.path.join(ARGS.baseline_dir, "Cargo.toml"), "/build_a/Cargo.toml", copy=True)
            .add_local_file(os.path.join(ARGS.baseline_dir, "Cargo.lock"), "/build_a/Cargo.lock", copy=True)
            .run_commands(
                'cd /build_a && RUSTFLAGS="-C target-cpu=x86-64-v2" cargo build --release',
                "cp /build_a/target/release/shopping-sessions /usr/local/bin/ss-a",
            )
        )
else:
    image = _base

if EXTRA_ENV:
    image = image.env(EXTRA_ENV)


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
        "MIMALLOC_LARGE_OS_PAGES": "1", "MIMALLOC_EAGER_COMMIT": "1",
    }
    for k in ("HELEX_S1_TARGET", "HELEX_P_TARGET", "HELEX_CHUNK_KB",
              "HELEX_IO", "HELEX_THP", "HELEX_RUNS", "HELEX_PL",
              "HELEX_PL_SWEEP", "HELEX_AB_N"):
        if k in os.environ:
            env[k] = os.environ[k]
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

    def drop_cache():
        for root, _, files in os.walk("/data"):
            for fn in files:
                try:
                    fd = os.open(os.path.join(root, fn), os.O_RDONLY)
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                    os.close(fd)
                except OSError:
                    pass

    proc = None

    def run_once(binary):
        nonlocal proc
        drop_cache()
        t1 = time.perf_counter()
        proc = subprocess.Popen(
            ["/usr/bin/time", "-v", binary], env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        th = threading.Thread(target=poll_mem)
        th.start()
        err = proc.communicate()[1]
        th.join()
        return time.perf_counter() - t1, err

    cpu_txt = open("/proc/cpuinfo").read()
    cpu_model = ""
    fam, mod, step = "", "", ""
    for line in cpu_txt.splitlines():
        if line.startswith("model name"):
            cpu_model = line.split(":", 1)[1].strip()
        elif line.startswith("cpu family"):
            fam = line.split(":", 1)[1].strip()
        elif line.startswith("model\t"):
            mod = line.split(":", 1)[1].strip()
        elif line.startswith("stepping"):
            step = line.split(":", 1)[1].strip()
    if not cpu_model and fam:
        cpu_model = f"family{fam}/model{mod}/step{step}"

    have_a = os.path.exists("/usr/local/bin/ss-a")
    ab_n = int(os.environ.get("HELEX_AB_N", "0"))
    stderr = b""
    ab_times = {"a": [], "b": []}
    run_s = 0.0

    if have_a and ab_n > 0:
        # Interleaved A/B on the same node: node variance cancels out of
        # the comparison. Cache dropped before every run (cold I/O like
        # the scorer).
        for i in range(ab_n * 2):
            tag = "a" if i % 2 == 0 else "b"
            dt, stderr = run_once(f"/usr/local/bin/ss-{tag}")
            ab_times[tag].append(dt)
            for line in stderr.decode(errors="replace").splitlines():
                if "[bench]" in line:
                    print(f"{tag}{i // 2}: run_s={dt:.3f} {line}", flush=True)
        for tag in "ab":
            ts = sorted(ab_times[tag])
            mid = ts[len(ts) // 2]
            print(f"{tag}: median={mid:.3f} all={[round(t, 3) for t in ts]}", flush=True)
        run_s = min(ab_times["b"])
    else:
        sweep = os.environ.get("HELEX_PL_SWEEP", "")
        pl_levels = [int(x) for x in sweep.split(",") if x.strip()] or [None]
        n_runs = int(os.environ.get("HELEX_RUNS", "1"))
        ri = -1
        for pl in pl_levels * (n_runs if not sweep else 1):
            ri += 1
            if pl is not None:
                env["HELEX_PL"] = str(pl)
            dt, stderr = run_once("/usr/local/bin/ss-b")
            if ri == 0 or dt < run_s:
                run_s = dt
            for line in stderr.decode(errors="replace").splitlines():
                if "[bench]" in line:
                    lbl = f"pl{pl}" if pl is not None else f"run{ri}"
                    print(f"{lbl}: run_s={dt:.3f} {line}", flush=True)

    # I/O floor probe: drop cache again, then sequentially read events CSVs
    # to measure the node's raw disk floor for the input volume.
    drop_cache()
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

    result = {
        "seed": seed, "events": events, "gen_s": round(gen_s, 2),
        "cpu_model": cpu_model,
        "cpu_flags": "avx512bw" in cpu_txt,
        "run_s": round(run_s, 3), "rc": proc.returncode,
        "io_floor_s": round(io_floor, 3),
        "events_mb": round(ev_bytes / 2**20, 1),
        "peak_rss_mib": round(peak[0] / 2**20, 1),
        "stderr_tail": stderr.decode(errors="replace")[-4000:],
    }
    if ab_times["a"]:
        for tag in "ab":
            ts = sorted(ab_times[tag])
            result[f"median_{tag}"] = ts[len(ts) // 2]
            result[f"runs_{tag}"] = [round(t, 3) for t in ts]
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
