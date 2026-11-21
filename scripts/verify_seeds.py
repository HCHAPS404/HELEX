#!/usr/bin/env python3
"""
Verification suite for Shopping Sessions:
1. Checks 1M dev set against dev-answer/ (if --dev and --answer exist).
2. Generates >=5 seeds (including 2028-02-29 leap-day windows: seeds 15 and 62),
   runs the candidate binary, checks all rules.md accounting invariants, and
   optionally compares row-for-row against an oracle command (--oracle).
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

import polars as pl
from run_fixtures import SCHEMAS, load_actual, load_expected


DEFAULT_SEEDS = [2, 7, 15, 42, 62]


def check_invariants(label: str, user_df: pl.DataFrame, camp_df: pl.DataFrame) -> None:
    if user_df.height != user_df.n_unique(subset=["user_id", "day"]):
        raise AssertionError(f"{label}: duplicate (user_id, day) keys in user_daily")
    if camp_df.height != camp_df.n_unique(subset=["campaign_id", "day"]):
        raise AssertionError(f"{label}: duplicate (campaign_id, day) keys in campaign_daily")

    u_sessions = int(user_df["sessions"].sum() or 0)
    c_sessions = int(camp_df["sessions"].sum() or 0)
    if u_sessions != c_sessions:
        raise AssertionError(f"{label}: sessions sum mismatch user={u_sessions} vs campaign={c_sessions}")

    u_rev = int(user_df["revenue_cents"].sum() or 0)
    c_rev = int(camp_df["revenue_cents"].sum() or 0)
    if u_rev != c_rev:
        raise AssertionError(f"{label}: revenue_cents sum mismatch user={u_rev} vs campaign={c_rev}")

    non_org = camp_df.filter(pl.col("campaign_id") != "organic")
    total_assisted = int(camp_df["assisted_sessions"].sum() or 0)
    non_org_sessions = int(non_org["sessions"].sum() or 0)
    if total_assisted < non_org_sessions:
        raise AssertionError(
            f"{label}: assisted_sessions ({total_assisted}) < non-organic sessions ({non_org_sessions})"
        )

    org = camp_df.filter(pl.col("campaign_id") == "organic")
    org_assisted = int(org["assisted_sessions"].sum() or 0)
    if org_assisted != 0:
        raise AssertionError(f"{label}: organic has non-zero assisted_sessions ({org_assisted})")
    if org.filter(pl.col("channel") != "organic").height > 0:
        raise AssertionError(f"{label}: organic row has channel != 'organic'")

    empty_camp = camp_df.filter((pl.col("sessions") == 0) & (pl.col("assisted_sessions") == 0))
    if empty_camp.height > 0:
        raise AssertionError(f"{label}: campaign_daily has {empty_camp.height} zero-activity rows")

    empty_user = user_df.filter(pl.col("sessions") <= 0)
    if empty_user.height > 0:
        raise AssertionError(f"{label}: user_daily has rows with sessions <= 0")

    bad_null_cat = user_df.filter(
        ((pl.col("revenue_cents") == 0) & pl.col("top_category").is_not_null())
        | ((pl.col("revenue_cents") > 0) & pl.col("top_category").is_null())
    )
    if bad_null_cat.height > 0:
        raise AssertionError(f"{label}: inconsistent top_category nullability vs revenue_cents")


def compare_tables(label: str, act_u: pl.DataFrame, act_c: pl.DataFrame, exp_u: pl.DataFrame, exp_c: pl.DataFrame) -> None:
    for name, a, b in (("user_daily", act_u, exp_u), ("campaign_daily", act_c, exp_c)):
        only_a = a.join(b, on=list(SCHEMAS[name]), how="anti", nulls_equal=True)
        only_b = b.join(a, on=list(SCHEMAS[name]), how="anti", nulls_equal=True)
        if only_a.height or only_b.height or a.height != b.height:
            raise AssertionError(
                f"{label} {name} DIFFER: rows {a.height} vs {b.height}, "
                f"only_actual={only_a.height}, only_expected={only_b.height}\n"
                f"actual sample:\n{only_a.head(5)}\nexpected sample:\n{only_b.head(5)}"
            )


def run_cmd(cmd: list[str], data_dir: Path, out_dir: Path, scratch_dir: Path) -> float:
    shutil.rmtree(out_dir, ignore_errors=True)
    shutil.rmtree(scratch_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        "DATA_DIR": str(data_dir),
        "OUTPUT_DIR": str(out_dir),
        "SCRATCH_DIR": str(scratch_dir),
    }
    t0 = time.perf_counter()
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    dt = time.perf_counter() - t0
    if p.returncode != 0:
        raise RuntimeError(f"Command {cmd} failed ({p.returncode}):\n{p.stderr[-2000:]}")
    if p.stdout:
        raise AssertionError(f"Non-empty stdout detected ({len(p.stdout)} bytes): production requires zero stdout")
    return dt


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True, help="Candidate binary to execute")
    ap.add_argument("--generate", default="generate", help="Path to generator binary")
    ap.add_argument("--dev", default="dev", help="Path to 1M dev dataset")
    ap.add_argument("--answer", default="dev-answer", help="Path to dev-answer folder")
    ap.add_argument("--events", type=int, default=50000, help="Events per generated seed")
    ap.add_argument("--clicks", type=int, default=12500, help="Clicks per generated seed")
    ap.add_argument("--oracle", nargs="*", help="Optional reference command for differential testing")
    args = ap.parse_args()

    cand_cmd = [args.bin]
    dev_dir = Path(args.dev)
    ans_dir = Path(args.answer)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_root:
        tmp = Path(tmp_root)
        if dev_dir.is_dir() and ans_dir.is_dir():
            out_dev = tmp / "out_dev"
            scr_dev = tmp / "scr_dev"
            dt = run_cmd(cand_cmd, dev_dir, out_dev, scr_dev)
            act_u = load_actual(out_dev / "user_daily", SCHEMAS["user_daily"])
            act_c = load_actual(out_dev / "campaign_daily", SCHEMAS["campaign_daily"])
            check_invariants("dev-1M", act_u, act_c)
            exp_u = load_expected(ans_dir / "user_daily.csv", SCHEMAS["user_daily"])
            exp_c = load_expected(ans_dir / "campaign_daily.csv", SCHEMAS["campaign_daily"])
            compare_tables("dev-1M", act_u, act_c, exp_u, exp_c)
            print(f"ok   dev-1M ({dt:.3f}s, user_daily={act_u.height}, campaign_daily={act_c.height})")
            del act_u, act_c, exp_u, exp_c
            shutil.rmtree(out_dev, ignore_errors=True)

        for seed in DEFAULT_SEEDS:
            seed_data = tmp / f"data_s{seed}"
            subprocess.run(
                [
                    args.generate,
                    "--seed",
                    str(seed),
                    "--events",
                    str(args.events),
                    "--clicks",
                    str(args.clicks),
                    "--output",
                    str(seed_data),
                ],
                check=True,
            )
            out_cand = tmp / f"out_s{seed}"
            scr_cand = tmp / f"scr_s{seed}"
            dt = run_cmd(cand_cmd, seed_data, out_cand, scr_cand)
            act_u = load_actual(out_cand / "user_daily", SCHEMAS["user_daily"])
            act_c = load_actual(out_cand / "campaign_daily", SCHEMAS["campaign_daily"])
            check_invariants(f"seed-{seed}", act_u, act_c)

            if args.oracle:
                out_ref = tmp / f"ref_s{seed}"
                scr_ref = tmp / f"ref_scr_s{seed}"
                shutil.rmtree(out_ref, ignore_errors=True)
                shutil.rmtree(scr_ref, ignore_errors=True)
                out_ref.mkdir(parents=True, exist_ok=True)
                scr_ref.mkdir(parents=True, exist_ok=True)
                env_ref = {
                    **os.environ,
                    "DATA_DIR": seed_data.as_posix(),
                    "OUTPUT_DIR": out_ref.as_posix(),
                    "SCRATCH_DIR": scr_ref.as_posix(),
                    "NCPU": "4",
                    "MEM_GB": "2",
                }
                subprocess.run(args.oracle, env=env_ref, check=True)
                exp_u = load_actual(out_ref / "user_daily", SCHEMAS["user_daily"])
                exp_c = load_actual(out_ref / "campaign_daily", SCHEMAS["campaign_daily"])
                compare_tables(f"seed-{seed}", act_u, act_c, exp_u, exp_c)

            print(f"ok   seed-{seed} ({dt:.3f}s, user_daily={act_u.height}, campaign_daily={act_c.height})")

    print("ALL VERIFICATION CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
