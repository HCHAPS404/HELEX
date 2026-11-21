//! Synthetic dataset generator for local benchmarking of the shopping-sessions
//! engine. It approximates the official generator's shape (sizes, id formats,
//! duplicate/invalid rates, mixed timestamp formats, links, clicks) so the
//! engine can be profiled at 20M/60M without the official tool. It is NOT the
//! official generator: use it for timing and for A/B output comparisons
//! between two engine builds, never as ground truth.
//!
//! usage: synthgen <out_dir> <events> <clicks> [seed]
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        ((self.next() >> 32) * n) >> 32
    }
    fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }
}

const DAY_MS: i64 = 86_400_000;
const WS: i64 = 1_790_812_800_000; // 2026-10-01T00:00:00Z
const NUM_DAYS: i64 = 14;

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn fmt_iso(out: &mut Vec<u8>, ms: i64, offset_min: Option<i64>) {
    let local = ms + offset_min.unwrap_or(0) * 60_000;
    let secs = local.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    write!(
        out,
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
        tod / 3600,
        (tod / 60) % 60,
        tod % 60
    )
    .unwrap();
    match offset_min {
        None => out.push(b'Z'),
        Some(o) => {
            let sign = if o < 0 { '-' } else { '+' };
            let a = o.abs();
            write!(out, "{sign}{:02}:{:02}", a / 60, a % 60).unwrap();
        }
    }
}

fn fmt_ts(out: &mut Vec<u8>, rng: &mut Rng, ms: i64) {
    let r = rng.below(100);
    if r < 88 {
        fmt_iso(out, ms - ms.rem_euclid(1000), None);
    } else if r < 94 {
        let off = (rng.below(27) as i64 - 12) * 60 + if rng.chance(100) { 30 } else { 0 };
        fmt_iso(out, ms - ms.rem_euclid(1000), Some(off));
    } else {
        write!(out, "{ms}").unwrap();
    }
}

fn user_name(out: &mut Vec<u8>, u: u64) {
    write!(out, "u{u}").unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: synthgen <out_dir> <events> <clicks> [seed]");
        std::process::exit(2);
    }
    let out = Path::new(&args[1]);
    let n_events: u64 = args[2].parse().unwrap();
    let n_clicks: u64 = args[3].parse().unwrap();
    let seed: u64 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(1);
    let mut rng = Rng(seed.wrapping_mul(0xA24B_AED4_963E_E407) ^ 0x1234_5678);

    for d in ["events", "clicks", "links"] {
        fs::create_dir_all(out.join(d)).unwrap();
    }
    fs::write(
        out.join("manifest.json"),
        "{\n  \"window_start\": \"2026-10-01T00:00:00Z\",\n  \"window_end\": \"2026-10-15T00:00:00Z\"\n}\n",
    )
    .unwrap();

    let n_products = 5_000u64;
    let cats = [
        "apparel", "beauty", "books", "electronics", "garden", "grocery", "home", "jewelry",
        "kitchen", "music", "office", "outdoors", "pets", "shoes", "sports", "tools", "toys",
        "video_games",
    ];
    let mut w = BufWriter::new(File::create(out.join("products.csv")).unwrap());
    writeln!(w, "product_id,category").unwrap();
    for p in 0..n_products {
        writeln!(w, "p{p},{}", cats[rng.below(cats.len() as u64) as usize]).unwrap();
    }
    drop(w);
    let n_camps = 2_000u64;
    let chans = ["search", "social", "email", "display", "affiliate"];
    let mut w = BufWriter::new(File::create(out.join("campaigns.csv")).unwrap());
    writeln!(w, "campaign_id,channel").unwrap();
    for c in 0..n_camps {
        writeln!(w, "c_{c},{}", chans[rng.below(5) as usize]).unwrap();
    }
    drop(w);

    let n_users = (n_events / 18).max(10);

    // Links: ~4% of users get linked to 1-3 aliases (some ghost ids).
    let n_links = n_users / 25;
    let link_parts = 4;
    let mut lw: Vec<BufWriter<File>> = (0..link_parts)
        .map(|i| {
            let mut f = BufWriter::new(File::create(out.join(format!("links/part-{i:05}.csv"))).unwrap());
            writeln!(f, "user_id_a,user_id_b").unwrap();
            f
        })
        .collect();
    let mut line = Vec::with_capacity(128);
    for _ in 0..n_links {
        let a = rng.below(n_users);
        let b = if rng.chance(150) { n_users + rng.below(n_users) } else { rng.below(n_users) };
        line.clear();
        if rng.chance(20) {
            line.extend_from_slice(b"\"");
            user_name(&mut line, b);
            line.extend_from_slice(b"\",");
        } else {
            user_name(&mut line, b);
            line.push(b',');
        }
        user_name(&mut line, a);
        line.push(b'\n');
        let f = &mut lw[rng.below(link_parts) as usize];
        f.write_all(&line).unwrap();
    }
    drop(lw);

    // Events: generated per session, written to a random part file so the
    // physical order is shuffled across users/time (like a real ingest dump).
    let ev_parts = 16u64;
    let mut ew: Vec<BufWriter<File>> = (0..ev_parts)
        .map(|i| {
            let mut f = BufWriter::with_capacity(1 << 20, File::create(out.join(format!("events/part-{i:05}.csv"))).unwrap());
            writeln!(f, "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents").unwrap();
            f
        })
        .collect();
    let mut eid: u64 = 0;
    let mut dup_ctr: u64 = 0;
    let mut written: u64 = 0;
    let mut recent: Vec<(u64, u64, i64)> = Vec::with_capacity(4096);
    while written < n_events {
        // skewed user pick
        let r = rng.below(1 << 20) as f64 / (1u64 << 20) as f64;
        let u = ((r * r) * n_users as f64) as u64;
        let start = WS - DAY_MS / 2 + rng.below(((NUM_DAYS + 1) * DAY_MS) as u64) as i64;
        let n_in = 1 + rng.below(12);
        let mut t = start;
        for _ in 0..n_in {
            if written >= n_events {
                break;
            }
            t += if rng.chance(60) { 1_800_000 + rng.below(3_600_000) as i64 } else { rng.below(1_500_000) as i64 };
            if rng.chance(30) {
                t += 1_800_000; // exact gap boundary probing
                t -= rng.below(2) as i64;
            }
            eid += 1;
            let mut id = eid;
            let mut uu = u;
            let ingest_delay: i64 = if rng.chance(15) {
                DAY_MS + rng.below(2 * DAY_MS as u64) as i64 - DAY_MS / 2
            } else {
                rng.below(600_000) as i64
            };
            let mut ingest = t + ingest_delay;
            let mut ts_ms = t;
            let mut is_dup = false;
            // duplicates: re-emit a recent event with another ingest time
            if !recent.is_empty() && rng.chance(25) {
                let (rid, ru, rts) = recent[rng.below(recent.len() as u64) as usize];
                id = rid;
                uu = if rng.chance(100) { rng.below(n_users) } else { ru };
                ts_ms = rts;
                dup_ctr += 1;
                ingest = rts + 1 + (dup_ctr % 899_999) as i64;
                is_dup = true;
                eid -= 1;
            } else {
                if recent.len() < 4096 {
                    recent.push((id, u, t));
                } else {
                    let k = rng.below(4096) as usize;
                    recent[k] = (id, u, t);
                }
            }
            line.clear();
            // event_id
            if rng.chance(2) {
                write!(line, "\"e{id}\",").unwrap();
            } else if rng.chance(1) {
                write!(line, "evt-long-{id:012},").unwrap();
            } else if rng.chance(1) {
                line.push(b',');
            } else {
                write!(line, "e{id},").unwrap();
            }
            // user
            let ur = rng.below(1000);
            if ur < 2 {
                line.push(b',');
            } else if ur < 5 {
                write!(line, "\"u{uu}\",").unwrap();
            } else if ur < 6 && uu % 64 == 0 {
                write!(line, "user_with_a_long_name_{uu},").unwrap();
            } else {
                write!(line, "u{uu},").unwrap();
            }
            // ts
            if rng.chance(4) {
                line.extend_from_slice(b"2026-10-03 10:00:00,");
            } else {
                fmt_ts(&mut line, &mut rng, ts_ms);
                line.push(b',');
            }
            if is_dup {
                // duplicate copy: F3 epoch-ms ingest keeps it distinct from the original
                write!(line, "{ingest}").unwrap();
            } else {
                fmt_iso(&mut line, ingest - ingest.rem_euclid(1000) + 1000, None);
            }
            line.push(b',');
            let tr = rng.below(1000);
            let (ty, purchase) = if tr < 650 {
                ("view", false)
            } else if tr < 880 {
                ("cart", false)
            } else if tr < 995 {
                ("purchase", true)
            } else if tr < 997 {
                ("Purchase", true)
            } else {
                ("click", false)
            };
            line.extend_from_slice(ty.as_bytes());
            line.push(b',');
            if rng.chance(3) {
                // empty product
            } else if rng.chance(10) {
                write!(line, "p{}", n_products + rng.below(100)).unwrap();
            } else {
                write!(line, "p{}", rng.below(n_products)).unwrap();
            }
            line.push(b',');
            if purchase {
                let pr = rng.below(1000);
                if pr < 5 {
                    line.extend_from_slice(b"19.99");
                } else if pr < 8 {
                    line.extend_from_slice(b"0");
                } else if pr < 10 {
                    line.extend_from_slice(b"-5");
                } else {
                    write!(line, "{}", 100 + rng.below(50_000)).unwrap();
                }
            } else if rng.chance(50) {
                write!(line, "{}", rng.below(1000)).unwrap();
            }
            if rng.chance(1) {
                line.extend_from_slice(b",extra");
            }
            line.push(b'\n');
            ew[rng.below(ev_parts) as usize].write_all(&line).unwrap();
            written += 1;
        }
    }
    drop(ew);

    let ck_parts = 8u64;
    let mut cw: Vec<BufWriter<File>> = (0..ck_parts)
        .map(|i| {
            let mut f = BufWriter::with_capacity(1 << 20, File::create(out.join(format!("clicks/part-{i:05}.csv"))).unwrap());
            writeln!(f, "click_id,user_id,campaign_id,ts").unwrap();
            f
        })
        .collect();
    for k in 0..n_clicks {
        let r = rng.below(1 << 20) as f64 / (1u64 << 20) as f64;
        let u = ((r * r) * n_users as f64) as u64;
        let t = WS - 8 * DAY_MS + rng.below(((NUM_DAYS + 9) * DAY_MS) as u64) as i64;
        line.clear();
        let kid = if rng.chance(5) { rng.below(k + 1) } else { k };
        write!(line, "k{kid},").unwrap();
        if rng.chance(3) {
            write!(line, "\"u{u}\",").unwrap();
        } else {
            write!(line, "u{u},").unwrap();
        }
        if rng.chance(10) {
            write!(line, "c_{},", n_camps + 5).unwrap();
        } else {
            write!(line, "c_{},", rng.below(n_camps)).unwrap();
        }
        fmt_ts(&mut line, &mut rng, t);
        line.push(b'\n');
        cw[rng.below(ck_parts) as usize].write_all(&line).unwrap();
    }
    drop(cw);
    eprintln!("users={n_users} links={n_links} events={n_events} clicks={n_clicks}");
}
