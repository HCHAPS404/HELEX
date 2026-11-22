//! Shopping Sessions engine.
//!
//! Phase 1  parse events + clicks (pread into cache-sized buffers, work queue
//!          over file chunks). Events -> 32 B records scattered by event-id
//!          shard; clicks -> resolved to canonical person, scattered by person
//!          partition.
//! Phase 2  per event-id shard: dedup (min ingest wins, invalid copies
//!          included), winners -> (person key, packed event) 16 B records
//!          scattered by person partition. Consumed blocks are recycled.
//! Phase 3  per person partition: counting-sort by person, sessionize,
//!          attribute clicks, aggregate.
//! Phase 4  Parquet output.

use std::cmp::Ordering;
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtOrd};
use std::sync::Mutex;
use std::time::Instant;

use crate::arena::{Arena, Blk, Scatter};
use crate::catalog::Catalog;
use crate::csv_dialect::parse_price_cents;
use crate::csv_scan::{for_each_record, Masks};
use crate::dsu::{csv_paths, load_links, Dsu};
use crate::keys::{fast_key_bytes, id_key, id_key_padded, is_slow, mix64, shard_of, KeyMap};
use crate::pq::{self, Col, Writer as PqWriter};
use crate::sso::SmallBytes;
use crate::timestamp::parse_ts_padded;

/// Shard counts scale with the input. Fewer, larger dedup shards (~40k
/// records) keep the phase-1 scatter to few write streams, which costs far
/// less than the larger (prefetched) dedup tables.
#[derive(Clone, Copy)]
struct Bits {
    s1: u32,
    p: u32,
}

impl Bits {
    fn for_event_bytes(bytes: u64) -> Self {
        let est = (bytes / 75).max(1);
        let lg = |target: u64| 64 - (est / target).max(1).leading_zeros() - 1;
        let env = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(d)
        };
        Bits {
            s1: lg(env("HELEX_S1_TARGET", 40_000)).clamp(6, 13),
            p: lg(env("HELEX_P_TARGET", 80_000)).clamp(4, 11),
        }
    }
}

const DAY_MS: i64 = 86_400_000;
const LATE_LIMIT_MS: i64 = 86_400_000;
const SESSION_GAP_MS: i64 = 1_800_000;
const CLICK_REACH_MS: i64 = 7 * DAY_MS;

const CHUNK_DEFAULT: u64 = 256 << 10;

fn chunk_bytes() -> u64 {
    std::env::var("HELEX_CHUNK_KB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(CHUNK_DEFAULT, |kb| (kb.max(4)) << 10)
}
const TAIL: u64 = 16 << 10;
const PAD: usize = crate::csv_scan::SCAN_PAD;

// Packed event: rel_ms:32 | price_cents:24 | cat_id:7 | is_purchase:1.
#[inline(always)]
fn pack_ev(rel_ms: u32, price_cents: u32, cat_id: u16, is_purchase: bool) -> u64 {
    (u64::from(rel_ms) << 32)
        | ((u64::from(price_cents) & 0xFF_FFFF) << 8)
        | ((u64::from(cat_id) & 0x7F) << 1)
        | u64::from(is_purchase)
}
#[inline(always)]
fn ev_rel(ev: u64) -> i64 {
    (ev >> 32) as i64
}
#[inline(always)]
fn ev_price(ev: u64) -> u32 {
    ((ev >> 8) & 0xFF_FFFF) as u32
}
#[inline(always)]
fn ev_cat(ev: u64) -> usize {
    ((ev >> 1) & 0x7F) as usize
}
#[inline(always)]
fn ev_purch(ev: u64) -> bool {
    ev & 1 != 0
}

/// Phase-1 event copy. `user == 0` marks an invalid copy (it still takes part
/// in dedup).
#[repr(C)]
#[derive(Clone, Copy)]
struct EvRec {
    key: u64,
    ingest: i64,
    user: u64,
    ev: u64,
}

/// Compact phase-1 event copy (24 B), used for every invalid copy and for
/// valid non-purchase copies whose `ingest - ts` fits in i32 ms:
/// * invalid: `user == 0`, `w = ingest`;
/// * valid:   `w = rel_ms << 32 | (ingest - ts) as u32`.
/// Everything else goes to the wide `EvRec` list.
#[repr(C)]
#[derive(Clone, Copy)]
struct Ev24 {
    key: u64,
    user: u64,
    w: u64,
}

impl Ev24 {
    #[inline(always)]
    fn ingest(&self, ws: i64) -> i64 {
        if self.user == 0 {
            self.w as i64
        } else {
            ws + (self.w >> 32) as i64 + i64::from(self.w as u32 as i32)
        }
    }
}

/// Phase-2 survivor: canonical person key + packed event.
#[repr(C)]
#[derive(Clone, Copy)]
struct PRec {
    canon: u64,
    ev: u64,
}

/// Click resolved to a canonical person (24 B). `ts` is ms since
/// `click_min_ts`. `cid` is the click id's key with prefix 'k': `form` 0 =
/// tag form, 1 = plain form (both order-preserving within the form), 2 =
/// long id stored in the per-worker side lists (`cid` = worker << 40 | idx).
#[repr(C)]
#[derive(Clone, Copy)]
struct CkRec {
    canon: u64,
    cid: u64,
    ts: u32,
    camp: u16,
    form: u16,
}

#[derive(Default, Clone, Copy)]
struct CampDayAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

/// Read-only parse context shared by phase-1 workers.
struct PCtx<'a> {
    catalog: &'a Catalog,
    /// fast product key -> category id (first catalog row wins)
    prod: KeyMap,
    /// fast campaign key -> campaign id
    camp: KeyMap,
}

impl PCtx<'_> {
    #[inline(always)]
    fn campaign_of(&self, cid: &[u8]) -> Option<u16> {
        let (k, slow) = unsafe { id_key_padded(cid, b'c') };
        if slow {
            return self.catalog.campaign_of(cid);
        }
        self.camp.get(k).map(|c| c as u16)
    }

    #[inline(always)]
    fn category_of(&self, product: &[u8]) -> u16 {
        let (k, slow) = unsafe { id_key_padded(product, b'p') };
        if slow {
            return self.catalog.category_of(product);
        }
        match self.prod.get(k) {
            Some(c) => c as u16,
            None => self.catalog.unknown_cat_id,
        }
    }
}

/// Fields must point into a padded chunk buffer (see `run_chunk`).
#[inline(always)]
fn validate_event_copy(fields: &[&[u8]; 7], ingest_ms: i64, pc: &PCtx) -> Option<u64> {
    let catalog = pc.catalog;
    if fields[1].is_empty() || fields[5].is_empty() {
        return None;
    }
    let (is_purchase, price_cents) = match fields[4] {
        b"view" | b"cart" => (false, 0u32),
        b"purchase" => (true, parse_price_cents(fields[6])?),
        _ => return None,
    };
    let ts = unsafe { parse_ts_padded(fields[2]) }?;
    if ts < catalog.window_start || ts >= catalog.window_end {
        return None;
    }
    if ingest_ms - ts > LATE_LIMIT_MS {
        return None;
    }
    let cat_id = if is_purchase {
        pc.category_of(fields[5])
    } else {
        0
    };
    let rel_ms = u32::try_from(ts - catalog.window_start).ok()?;
    Some(pack_ev(rel_ms, price_cents, cat_id, is_purchase))
}

// ---------------------------------------------------------------- phase 1 io

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Events,
    Clicks,
}

struct InFile {
    file: File,
    map: Option<memmap2::Mmap>,
    len: u64,
    data_start: u64,
    kind: Kind,
}

struct Task {
    file: usize,
    start: u64,
    end: u64,
}

fn open_inputs(paths: &[PathBuf], kind: Kind, out: &mut Vec<InFile>) -> io::Result<()> {
    for p in paths {
        let file = File::open(p)?;
        let len = file.metadata()?.len();
        if len == 0 {
            continue;
        }
        // No header probe: chunk 0 of each file starts at byte 0 and, like
        // every other chunk, skips its first (partial) line, i.e. the header.
        let data_start = 0u64;
        let map = if use_mmap() {
            unsafe { memmap2::Mmap::map(&file) }.ok()
        } else {
            None
        };
        out.push(InFile {
            file,
            map,
            len,
            data_start,
            kind,
        });
    }
    Ok(())
}

/// Reads the chunk [start, end) of `f` (plus the line that straddles `end`)
/// and calls `line_fn` for every line that starts inside the chunk.
fn run_chunk(
    f: &InFile,
    start: u64,
    end: u64,
    buf: &mut Vec<u8>,
    body: impl FnOnce(&[u8], bool, usize),
) -> io::Result<()> {
    let from = if start == 0 {
        start
    } else {
        start - 1
    };
    let limit = (end - from) as usize;
    if let Some(map) = &f.map {
        // Zero-copy path: populate the chunk's page-table entries in one
        // call, then parse in place. Used only when the bytes after the last
        // line still lie inside the file (wide loads read past field ends).
        let m = &map[..];
        let stop = match memchr::memchr(b'\n', &m[from as usize + limit - 1..]) {
            Some(r) => from as usize + limit + r,
            None => m.len(),
        };
        if stop + PAD <= m.len() {
            let _ = map.advise_range(
                memmap2::Advice::PopulateRead,
                from as usize,
                stop - from as usize,
            );
            body(&m[from as usize..stop], false, limit);
            return Ok(());
        }
    }
    let mut want = ((end - from) + TAIL).min(f.len - from) as usize;
    loop {
        // +PAD: parsers do wide unaligned loads past field ends.
        if buf.len() < want + PAD {
            buf.resize(want + PAD, 0);
        }
        f.file.read_exact_at(&mut buf[..want], from)?;
        let at_eof = from + want as u64 >= f.len;
        if at_eof || memchr::memchr(b'\n', &buf[limit - 1..want]).is_some() {
            break;
        }
        want = (want * 2).min((f.len - from) as usize);
    }
    body(&buf[..want], false, limit);
    Ok(())
}

const READAHEAD_BYTES: u64 = 192 << 20;

/// One stderr line describing the machine (visible in the scorer logs):
/// CPU model, detected threads, cgroup limits, THP mode, memory, and how much
/// of the event input is already in the page cache (cold vs warm start).
fn report_env(data_dir: &Path, threads: usize) {
    let rd = |p: &str| fs::read_to_string(p).unwrap_or_default();
    let cpuinfo = rd("/proc/cpuinfo");
    let model = cpuinfo
        .lines()
        .find(|l| l.starts_with("model name"))
        .and_then(|l| l.split(':').nth(1))
        .unwrap_or("?")
        .trim()
        .to_string();
    let ncpu_proc = cpuinfo.lines().filter(|l| l.starts_with("processor")).count();
    let flags = cpuinfo.lines().find(|l| l.starts_with("flags")).unwrap_or("");
    let avx512 = flags.contains("avx512bw");
    let cpu_max = rd("/sys/fs/cgroup/cpu.max").trim().to_string();
    let mem_max = rd("/sys/fs/cgroup/memory.max").trim().to_string();
    let thp = rd("/sys/kernel/mm/transparent_hugepage/enabled").trim().to_string();
    let memtotal = rd("/proc/meminfo")
        .lines()
        .find(|l| l.starts_with("MemTotal"))
        .and_then(|l| l.split_whitespace().nth(1).map(|v| v.to_string()))
        .unwrap_or_default();
    // page-cache residency of the first event file (mincore)
    let mut cached_pct = -1i64;
    #[cfg(target_os = "linux")]
    if let Some(p) = csv_paths(&data_dir.join("events")).first() {
        if let Ok(f) = File::open(p) {
            if let Ok(m) = unsafe { memmap2::Mmap::map(&f) } {
                let pages = m.len().div_ceil(4096);
                let mut vec = vec![0u8; pages];
                let r = unsafe {
                    libc::mincore(m.as_ptr() as *mut libc::c_void, m.len(), vec.as_mut_ptr())
                };
                if r == 0 && pages > 0 {
                    cached_pct =
                        (vec.iter().filter(|&&b| b & 1 != 0).count() * 100 / pages) as i64;
                }
            }
        }
    }
    eprintln!(
        "[env] cpu=\"{model}\" nproc={ncpu_proc} threads={threads} avx512={avx512} cpu.max=\"{cpu_max}\" mem.max={mem_max} memtotal_kb={memtotal} thp=\"{thp}\" ev0_cached_pct={cached_pct}"
    );
}

/// Detached background threads issue POSIX_FADV_WILLNEED over all input
/// files (in 8 MiB pieces, interleaved across threads) so cold reads run at
/// full device queue depth. Purely advisory: no effect when cached.
fn prefetch_inputs(data_dir: &Path) {
    let mut paths: Vec<PathBuf> = Vec::new();
    for sub in ["links", "events", "clicks"] {
        let mut v = csv_paths(&data_dir.join(sub));
        if sub == "events" {
            // same order as the task list: largest first
            v.sort_by_key(|p| std::cmp::Reverse(p.metadata().map(|m| m.len()).unwrap_or(0)));
        }
        paths.extend(v);
    }
    let mut pieces: Vec<(usize, u64, u64)> = Vec::new();
    let mut files: Vec<File> = Vec::new();
    for p in paths {
        let Ok(f) = File::open(&p) else { continue };
        let len = f.metadata().map(|m| m.len()).unwrap_or(0);
        let fi = files.len();
        files.push(f);
        let mut s = 0u64;
        while s < len {
            let e = (s + (8 << 20)).min(len);
            pieces.push((fi, s, e - s));
            s = e;
        }
    }
    let files = std::sync::Arc::new(files);
    let pieces = std::sync::Arc::new(pieces);
    let next = std::sync::Arc::new(AtomicUsize::new(0));
    let n_threads: usize = std::env::var("HELEX_RA_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    for _ in 0..n_threads {
        let (files, pieces, next) = (files.clone(), pieces.clone(), next.clone());
        std::thread::spawn(move || loop {
            let i = next.fetch_add(1, AtOrd::Relaxed);
            let Some(&(fi, off, len)) = pieces.get(i) else { break };
            readahead(&files[fi], off, len);
        });
    }
}

fn readahead(f: &File, off: u64, len: u64) {
    #[cfg(target_os = "linux")]
    unsafe {
        use std::os::unix::io::AsRawFd;
        libc::posix_fadvise(
            f.as_raw_fd(),
            off as libc::off_t,
            len as libc::off_t,
            libc::POSIX_FADV_WILLNEED,
        );
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (f, off, len);
}

fn use_mmap() -> bool {
    !std::env::var("HELEX_IO").is_ok_and(|v| v == "pread")
}

#[cfg(test)]
fn process_lines(
    b: &[u8],
    first: bool,
    limit: usize,
    mut line_fn: impl FnMut(&[u8]),
) -> io::Result<()> {
    let mut pos = if first {
        0
    } else if b[0] == b'\n' {
        1
    } else {
        match memchr::memchr(b'\n', &b[1..]) {
            Some(r) => r + 2,
            None => return Ok(()),
        }
    };
    while pos < limit && pos < b.len() {
        match memchr::memchr(b'\n', &b[pos..]) {
            Some(r) => {
                line_fn(&b[pos..pos + r]);
                pos += r + 1;
            }
            None => {
                line_fn(&b[pos..]);
                break;
            }
        }
    }
    Ok(())
}

pub fn worker_count() -> usize {
    if let Ok(val) = std::env::var("NCPU") {
        if let Ok(n) = val.trim().parse::<usize>() {
            if n > 0 {
                return n.min(64);
            }
        }
    }
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .clamp(1, 64)
}

/// Runs `count` tasks over `threads` workers with per-worker state.
fn par_tasks<S: Send, I, F>(count: usize, threads: usize, init: I, f: F) -> Vec<S>
where
    I: Fn(usize) -> S + Sync,
    F: Fn(&mut S, usize) + Sync,
{
    let next = AtomicUsize::new(0);
    let threads = threads.max(1);
    let slots: Vec<Mutex<Option<S>>> = (0..threads).map(|_| Mutex::new(None)).collect();
    std::thread::scope(|sc| {
        for w in 0..threads {
            let next = &next;
            let slot = &slots[w];
            let init = &init;
            let f = &f;
            sc.spawn(move || {
                let mut st = init(w);
                loop {
                    let i = next.fetch_add(1, AtOrd::Relaxed);
                    if i >= count {
                        break;
                    }
                    f(&mut st, i);
                }
                *slot.lock().unwrap() = Some(st);
            });
        }
    });
    slots
        .into_iter()
        .map(|m| m.into_inner().unwrap().unwrap())
        .collect()
}

struct P1 {
    worker: usize,
    bits: Bits,
    ev: Scatter<Ev24>,
    wide: Vec<EvRec>,
    slow_seen: KeyMap,
    ck: Scatter<CkRec>,
    slow_u: Vec<(u64, Box<[u8]>)>,
    long_cids: Vec<Box<[u8]>>,
    ck_pend: Vec<CkRec>,
    ck_defer: Vec<CkRec>,
    buf: Vec<u8>,
    masks: Masks,
    err: Option<io::Error>,
}

const CID_LONG: u16 = 2;
const CID_IDX_BITS: u32 = 40;

#[inline]
fn cid_bytes<'a>(c: &CkRec, longs: &'a [Vec<Box<[u8]>>], tmp: &'a mut [u8; 9]) -> &'a [u8] {
    if c.form == CID_LONG {
        return &longs[(c.cid >> CID_IDX_BITS) as usize]
            [(c.cid & ((1u64 << CID_IDX_BITS) - 1)) as usize];
    }
    let n = fast_key_bytes(c.cid, b'k', tmp);
    &tmp[..n]
}

/// Byte-wise click-id order.
#[inline]
fn cid_cmp(a: &CkRec, b: &CkRec, longs: &[Vec<Box<[u8]>>]) -> Ordering {
    if a.form == b.form && a.form != CID_LONG {
        // same fast form: zero-padded big-endian payloads (no NULs) order
        // like the bytes; tag-form ids share the 'k' prefix.
        (a.cid & !crate::keys::TAG).cmp(&(b.cid & !crate::keys::TAG))
    } else {
        let mut ta = [0u8; 9];
        let mut tb = [0u8; 9];
        cid_bytes(a, longs, &mut ta).cmp(cid_bytes(b, longs, &mut tb))
    }
}

// ------------------------------------------------------------- phase 3 core

struct P3 {
    pmap: KeyMap,
    lids: Vec<u32>,
    counts: Vec<u32>,
    pkeys: Vec<u64>,
    evs: Vec<u64>,
    ck_lid: Vec<u32>,
    ck_cnt: Vec<u32>,
    cks: Vec<CkRec>,
    ucols: Vec<Col>,
    worker: usize,
    writer: Option<PqWriter>,
    rows_written: usize,
    err: Option<io::Error>,
    grid: Vec<CampDayAgg>,
    camp_counts: Vec<u32>,
    in_active: Vec<bool>,
    active: Vec<u32>,
    day_cat_rev: Vec<u64>,
}

struct Ctx<'a> {
    catalog: &'a Catalog,
    ws: i64,
    click_base: i64,
    day0: i32,
    num_days: usize,
    longs: &'a [Vec<Box<[u8]>>],
    slow_names: &'a KeyMap,
    slow_bytes: &'a [Box<[u8]>],
}

impl Ctx<'_> {
    #[inline]
    fn person_bytes<'b>(&'b self, canon: u64, tmp: &'b mut [u8; 9]) -> &'b [u8] {
        if is_slow(canon) {
            let i = self
                .slow_names
                .get(canon)
                .expect("slow id missing from registry");
            &self.slow_bytes[i as usize]
        } else {
            let n = fast_key_bytes(canon, b'u', tmp);
            &tmp[..n]
        }
    }

    #[allow(dead_code)]
    fn person_name(&self, canon: u64) -> SmallBytes {
        if is_slow(canon) {
            let i = self
                .slow_names
                .get(canon)
                .expect("slow id missing from registry");
            SmallBytes::from_slice(&self.slow_bytes[i as usize])
        } else {
            let mut b = [0u8; 9];
            let n = fast_key_bytes(canon, b'u', &mut b);
            SmallBytes::from_slice(&b[..n])
        }
    }
}

fn top_cat(day_cat_rev: &mut [u64]) -> u16 {
    let mut best_cat = 0u16;
    let mut best_rev = 0u64;
    for (cid, &r) in day_cat_rev.iter().enumerate() {
        if r > best_rev {
            best_rev = r;
            best_cat = cid as u16;
        }
    }
    day_cat_rev.fill(0);
    best_cat
}

#[inline]
fn insertion_sort(v: &mut [u64]) {
    for i in 1..v.len() {
        let x = v[i];
        let mut j = i;
        while j > 0 && v[j - 1] > x {
            v[j] = v[j - 1];
            j -= 1;
        }
        v[j] = x;
    }
}

fn process_person(st: &mut P3, ctx: &Ctx, canon: u64, evs: &mut [u64], clicks: &mut [CkRec]) {
    let ws = ctx.ws;
    let num_days = ctx.num_days;
    let organic = ctx.catalog.organic_camp_id as usize;
    if evs.len() > 1 {
        if evs.len() <= 24 {
            insertion_sort(evs);
        } else {
            evs.sort_unstable();
        }
    }
    let mut clicks_len = clicks.len();
    if clicks_len > 1 {
        let longs = ctx.longs;
        // ts ascending, click_id descending: among equal ts the smallest id
        // sorts last and wins as "latest".
        clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| cid_cmp(b, a, longs)));
        let mut w = 1usize;
        for r in 1..clicks_len {
            let same = clicks[r].ts == clicks[w - 1].ts
                && cid_cmp(&clicks[r], &clicks[w - 1], longs) == Ordering::Equal;
            if !same {
                clicks[w] = clicks[r];
                w += 1;
            }
        }
        clicks_len = w;
    }
    let clicks = &clicks[..clicks_len];

    let mut left = 0usize;
    let mut right = 0usize;
    let mut cur_day: i32 = i32::MIN;
    let mut day_sessions: i64 = 0;
    let mut day_events: i64 = 0;
    let mut day_rev: i64 = 0;

    let mut idx = 0usize;
    while idx < evs.len() {
        let sess_start = ws + ev_rel(evs[idx]);
        let mut prev_ts = sess_start;
        let mut sess_events: i64 = 0;
        let mut sess_rev: i64 = 0;
        let mut sess_has_purchase = false;

        let sess_day = sess_start.div_euclid(DAY_MS) as i32;
        if cur_day != i32::MIN && sess_day != cur_day {
            let tc = if day_rev > 0 {
                Some(top_cat(&mut st.day_cat_rev))
            } else {
                None
            };
            push_row(st, ctx, canon, cur_day, day_sessions, day_events, day_rev, tc);
            day_sessions = 0;
            day_events = 0;
            day_rev = 0;
        }
        cur_day = sess_day;

        while idx < evs.len() {
            let e = evs[idx];
            let ts = ws + ev_rel(e);
            if ts - prev_ts > SESSION_GAP_MS {
                break;
            }
            prev_ts = ts;
            sess_events += 1;
            if ev_purch(e) {
                sess_has_purchase = true;
                let p = ev_price(e);
                sess_rev += i64::from(p);
                st.day_cat_rev[ev_cat(e)] += u64::from(p);
            }
            idx += 1;
        }

        day_sessions += 1;
        day_events += sess_events;
        day_rev += sess_rev;

        let sess_rel = sess_start - ctx.click_base;
        while right < clicks.len() && i64::from(clicks[right].ts) <= sess_rel {
            let cid = clicks[right].camp as usize;
            if !st.in_active[cid] {
                st.in_active[cid] = true;
                st.active.push(cid as u32);
            }
            st.camp_counts[cid] += 1;
            right += 1;
        }
        let min_click_ts = sess_start - CLICK_REACH_MS;
        while left < right && i64::from(clicks[left].ts) < min_click_ts - ctx.click_base {
            st.camp_counts[clicks[left].camp as usize] -= 1;
            left += 1;
        }

        let day_i = (sess_day - ctx.day0) as usize;
        let win_camp = if left == right {
            organic
        } else {
            clicks[right - 1].camp as usize
        };
        {
            let agg = &mut st.grid[win_camp * num_days + day_i];
            agg.sessions += 1;
            if sess_has_purchase {
                agg.purchasing_sessions += 1;
                agg.revenue_cents += sess_rev;
            }
        }
        if left != right {
            let mut k = 0usize;
            while k < st.active.len() {
                let cid = st.active[k] as usize;
                if st.camp_counts[cid] == 0 {
                    st.in_active[cid] = false;
                    st.active.swap_remove(k);
                } else {
                    st.grid[cid * num_days + day_i].assisted_sessions += 1;
                    k += 1;
                }
            }
        }
    }

    if cur_day != i32::MIN {
        let tc = if day_rev > 0 {
            Some(top_cat(&mut st.day_cat_rev))
        } else {
            None
        };
        push_row(st, ctx, canon, cur_day, day_sessions, day_events, day_rev, tc);
    }
    for &cid in &st.active {
        st.camp_counts[cid as usize] = 0;
        st.in_active[cid as usize] = false;
    }
    st.active.clear();
}

fn process_partition(st: &mut P3, ctx: &Ctx, pblocks: &[&[Blk<PRec>]], cblocks: &[&[Blk<CkRec>]]) {
    let n: usize = pblocks
        .iter()
        .flat_map(|b| b.iter())
        .map(|b| b.n as usize)
        .sum();
    if n == 0 {
        return;
    }
    st.pmap.clear_for(n / 8 + 64);
    st.lids.clear();
    st.counts.clear();
    st.pkeys.clear();
    for blks in pblocks {
        for b in blks.iter() {
            for r in b.as_slice() {
                let next = st.pkeys.len() as u32;
                let (lid, ins) = st.pmap.get_or_insert(r.canon, next);
                if ins {
                    st.pkeys.push(r.canon);
                    st.counts.push(0);
                }
                st.counts[lid as usize] += 1;
                st.lids.push(lid);
            }
        }
    }
    let np = st.pkeys.len();
    // Exclusive prefix sums: counts[lid] becomes the write cursor and ends
    // as the end offset of person lid.
    let mut acc = 0u32;
    for c in st.counts.iter_mut() {
        let v = *c;
        *c = acc;
        acc += v;
    }
    st.evs.clear();
    st.evs.resize(n, 0);
    {
        let mut i = 0usize;
        for blks in pblocks {
            for b in blks.iter() {
                for r in b.as_slice() {
                    let lid = st.lids[i] as usize;
                    let at = st.counts[lid] as usize;
                    st.evs[at] = r.ev;
                    st.counts[lid] = at as u32 + 1;
                    i += 1;
                }
            }
        }
    }

    st.ck_lid.clear();
    st.ck_cnt.clear();
    st.ck_cnt.resize(np, 0);
    let mut nck = 0usize;
    for blks in cblocks {
        for b in blks.iter() {
            for c in b.as_slice() {
                match st.pmap.get(c.canon) {
                    Some(lid) => {
                        st.ck_cnt[lid as usize] += 1;
                        st.ck_lid.push(lid);
                        nck += 1;
                    }
                    None => st.ck_lid.push(u32::MAX),
                }
            }
        }
    }
    let mut acc = 0u32;
    for c in st.ck_cnt.iter_mut() {
        let v = *c;
        *c = acc;
        acc += v;
    }
    let zero = CkRec {
        canon: 0,
        cid: 0,
        ts: 0,
        camp: 0,
        form: 0,
    };
    st.cks.clear();
    st.cks.resize(nck, zero);
    {
        let mut i = 0usize;
        for blks in cblocks {
            for b in blks.iter() {
                for c in b.as_slice() {
                    let lid = st.ck_lid[i];
                    i += 1;
                    if lid != u32::MAX {
                        let at = st.ck_cnt[lid as usize] as usize;
                        st.cks[at] = *c;
                        st.ck_cnt[lid as usize] = at as u32 + 1;
                    }
                }
            }
        }
    }

    let mut evs = std::mem::take(&mut st.evs);
    let mut cks = std::mem::take(&mut st.cks);
    let mut ev_start = 0usize;
    let mut ck_start = 0usize;
    for lid in 0..np {
        let ev_end = st.counts[lid] as usize;
        let ck_end = st.ck_cnt[lid] as usize;
        let canon = st.pkeys[lid];
        process_person(
            st,
            ctx,
            canon,
            &mut evs[ev_start..ev_end],
            &mut cks[ck_start..ck_end],
        );
        ev_start = ev_end;
        ck_start = ck_end;
    }
    st.evs = evs;
    st.cks = cks;
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn push_row(
    st: &mut P3,
    ctx: &Ctx,
    canon: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
) {
    let mut tmp = [0u8; 9];
    let name = ctx.person_bytes(canon, &mut tmp);
    st.ucols[0].push_bytes(name);
    st.ucols[1].push_i32(day);
    st.ucols[2].push_i64(sessions);
    st.ucols[3].push_i64(events);
    st.ucols[4].push_i64(revenue);
    st.ucols[5].push_opt_bytes(top.map(|c| ctx.catalog.categories[c as usize].as_slice()));
}

const ROW_GROUP: usize = 65_536;

fn flush_rows(st: &mut P3, dir: &Path) {
    let n = st.ucols[0].n;
    if n == 0 || st.err.is_some() {
        return;
    }
    if st.writer.is_none() {
        match PqWriter::create(
            &dir.join(format!("part-{}.parquet", st.worker)),
            "user_daily",
            &pq::USER_DAILY,
        ) {
            Ok(w) => st.writer = Some(w),
            Err(e) => {
                st.err = Some(e);
                return;
            }
        }
    }
    let w = st.writer.as_mut().unwrap();
    std::mem::swap(&mut w.cols, &mut st.ucols);
    let r = w.flush_row_group();
    std::mem::swap(&mut w.cols, &mut st.ucols);
    if let Err(e) = r {
        st.err = Some(e);
        return;
    }
    st.rows_written += n;
}

// ------------------------------------------------------------- line parsers

#[inline(always)]
fn parse_event_line(st: &mut P1, fields: &[&[u8]; 7], pc: &PCtx, arena: &Arena) {
    let event_id = fields[0];
    if event_id.is_empty() {
        return;
    }
    let Some(ingest) = (unsafe { parse_ts_padded(fields[3]) }) else {
        return;
    };
    // Slow (long) event ids need no bytes: equality on the 56-bit hash key.
    let (key, _slow_id) = unsafe { id_key_padded(event_id, b'e') };
    let shard = shard_of(key, st.bits.s1);
    match validate_event_copy(fields, ingest, pc) {
        Some(ev) => {
            let (uk, slow_u) = unsafe { id_key_padded(fields[1], b'u') };
            if slow_u {
                note_slow_user(st, uk, fields[1]);
            }
            let rel = ev >> 32;
            let delta = ingest - (pc.catalog.window_start + rel as i64);
            if ev as u32 == 0 && delta >= i64::from(i32::MIN) && delta <= i64::from(i32::MAX) {
                st.ev.push(
                    arena,
                    shard,
                    Ev24 {
                        key,
                        user: uk,
                        w: (rel << 32) | u64::from(delta as i32 as u32),
                    },
                );
            } else {
                st.wide.push(EvRec {
                    key,
                    ingest,
                    user: uk,
                    ev,
                });
            }
        }
        None => st.ev.push(
            arena,
            shard,
            Ev24 {
                key,
                user: 0,
                w: ingest as u64,
            },
        ),
    }
}

/// Records a slow user id's bytes once per worker.
#[cold]
fn note_slow_user(st: &mut P1, uk: u64, raw: &[u8]) {
    let (_, ins) = st.slow_seen.get_or_insert(uk, 0);
    if ins {
        st.slow_u.push((uk, raw.into()));
    }
}

#[inline(always)]
fn parse_click_line(
    st: &mut P1,
    fields: &[&[u8]; 4],
    pc: &PCtx,
    dsu: Option<&Dsu>,
    arena: &Arena,
    click_min_ts: i64,
    window_end: i64,
) {
    
    let click_id = fields[0];
    let user_raw = fields[1];
    if click_id.is_empty() || user_raw.is_empty() {
        return;
    }
    let Some(camp) = pc.campaign_of(fields[2]) else {
        return;
    };
    let Some(ts) = (unsafe { parse_ts_padded(fields[3]) }) else {
        return;
    };
    if ts < click_min_ts || ts >= window_end {
        return;
    }
    let (uk, slow_u) = unsafe { id_key_padded(user_raw, b'u') };
    if slow_u {
        note_slow_user(st, uk, user_raw);
    }
    let (ck, slow_c) = unsafe { id_key_padded(click_id, b'k') };
    let (cid, form) = if !slow_c {
        (ck, u16::from(ck & crate::keys::TAG == 0))
    } else {
        st.long_cids.push(click_id.into());
        (
            ((st.worker as u64) << CID_IDX_BITS) | (st.long_cids.len() - 1) as u64,
            CID_LONG,
        )
    };
    let rec = CkRec {
        canon: uk,
        cid,
        ts: (ts - click_min_ts) as u32,
        camp,
        form,
    };
    let Some(dsu) = dsu else {
        st.ck_defer.push(rec);
        return;
    };
    // Linked-ID resolution is a DRAM-latency lookup: queue the click, issue
    // a prefetch, and resolve in batches.
    dsu.prefetch(uk);
    st.ck_pend.push(rec);
    if st.ck_pend.len() >= CK_BATCH {
        flush_clicks(st, dsu, arena);
    }
}

const CK_BATCH: usize = 16;

fn flush_clicks(st: &mut P1, dsu: &Dsu, arena: &Arena) {
    for mut c in st.ck_pend.drain(..) {
        c.canon = dsu.resolve(c.canon);
        st.ck.push(arena, shard_of(c.canon, st.bits.p), c);
    }
}

// ------------------------------------------------------------------- driver

pub fn run_pipeline(
    data_dir: &Path,
    out_dir: &Path,
    _scratch_dir: &Path,
    _debug_bench: bool,
) -> io::Result<()> {
    let t0 = Instant::now();
    let threads = worker_count();
    report_env(data_dir, threads);
    // Cold-cache input: ask the kernel for every input file right away, from
    // several threads, so the disk sees deep queues while we load the
    // catalog/links and parse. Order = processing order (links, events,
    // clicks). HELEX_RA=throttle keeps the old windowed readahead only.
    let ra_mode = std::env::var("HELEX_RA").unwrap_or_default();
    if ra_mode == "all" {
        prefetch_inputs(data_dir);
    }
    let catalog = Catalog::load(data_dir)?;
    if catalog.categories.len() > 127 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "category count exceeds packed-event capacity",
        ));
    }
    if catalog.window_end - (catalog.window_start - CLICK_REACH_MS) > i64::from(u32::MAX) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "window exceeds packed-event capacity",
        ));
    }
    let ws = catalog.window_start;
    let we = catalog.window_end;
    let day0 = ws.div_euclid(DAY_MS) as i32;
    let num_days = ((we - 1).div_euclid(DAY_MS) as i32 - day0) as usize + 1;
    let click_min_ts = ws - CLICK_REACH_MS;
    // F1 prefix cache: months from the click lookback through late ingests.
    crate::timestamp::seed_prefix_cache(click_min_ts - DAY_MS);
    let t_cat = t0.elapsed();

    // ---- phase 1: parse
    let t1 = Instant::now();
    let mut files: Vec<InFile> = Vec::new();
    open_inputs(
        &csv_paths(&data_dir.join("events")),
        Kind::Events,
        &mut files,
    )?;
    open_inputs(
        &csv_paths(&data_dir.join("clicks")),
        Kind::Clicks,
        &mut files,
    )?;
    let mut tasks: Vec<Task> = Vec::new();
    let chunk = chunk_bytes();
    let ev_bytes: u64 = files
        .iter()
        .filter(|f| f.kind == Kind::Events)
        .map(|f| f.len)
        .sum();
    let bits = Bits::for_event_bytes(ev_bytes);
    // Events first (largest files first), then clicks: links/DSU (needed only
    // by clicks) load while events parse.
    for kind in [Kind::Events, Kind::Clicks] {
        let mut idxs: Vec<usize> = (0..files.len())
            .filter(|&i| files[i].kind == kind)
            .collect();
        idxs.sort_by_key(|&i| std::cmp::Reverse(files[i].len));
        for i in idxs {
            let f = &files[i];
            let mut s = f.data_start;
            while s < f.len {
                let e = (s + chunk).min(f.len);
                tasks.push(Task {
                    file: i,
                    start: s,
                    end: e,
                });
                s = e;
            }
        }
    }

    let mut prod = KeyMap::with_capacity(1024);
    for (pid, cat) in catalog.products() {
        let (k, slow) = id_key(pid, b'p');
        if !slow {
            prod.get_or_insert(k, u32::from(cat));
        }
    }
    let mut camp = KeyMap::with_capacity(catalog.campaign_ids.len() + 1);
    for (i, cid) in catalog.campaign_ids[..catalog.organic_camp_id as usize]
        .iter()
        .enumerate()
    {
        let (k, slow) = id_key(cid, b'c');
        if !slow {
            camp.get_or_insert(k, i as u32);
        }
    }
    let pctx = PCtx {
        catalog: &catalog,
        prod,
        camp,
    };
    let pctx_ref = &pctx;
    let arena = Arena::new();
    // Linked IDs build concurrently with event parsing (event chunks come
    // first in the task order); click chunks wait for it.
    let dsu_cell: std::sync::OnceLock<io::Result<Dsu>> = std::sync::OnceLock::new();
    let dsu_cell_ref = &dsu_cell;
    // DSU work is short (~0.2 CPU-s); all threads keep its serial span small
    // even while it competes with the parse workers.
    let dsu_threads = std::env::var("HELEX_DSU_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or((threads / 2).clamp(1, 4));
    let files_ref = &files;
    let tasks_ref = &tasks;
    let arena_ref = &arena;

    // Readahead: a helper thread asks the kernel to page in input ahead of
    // the parse workers (matters when the page cache is cold); throttled to
    // stay at most READAHEAD_BYTES past the furthest claimed chunk.
    let claimed = AtomicUsize::new(0);
    let claimed_ref = &claimed;
    let ra_done = std::sync::atomic::AtomicBool::new(false);
    let ra_done_ref = &ra_done;
    let t_setup = t1.elapsed();
    let (p1_states, t_dsu) = std::thread::scope(|sc| {
        let dsu_thread = sc.spawn(move || {
            let t = Instant::now();
            // test hook: force the deferred-click path
            if let Some(ms) = std::env::var("HELEX_TEST_DSU_DELAY_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
            {
                std::thread::sleep(std::time::Duration::from_millis(ms));
            }
            let _ = dsu_cell_ref.set(load_links(data_dir, dsu_threads));
            t.elapsed()
        });
        if ra_mode != "0" && ra_mode != "all" {
            // Windowed readahead: K threads issue WILLNEED for the chunks
            // just ahead of the furthest one claimed by the parse workers.
            let ra_threads: usize = std::env::var("HELEX_RA_THREADS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);
            let win_mb: u64 = std::env::var("HELEX_RA_WIN_MB")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(READAHEAD_BYTES >> 20);
            let win_tasks = ((win_mb << 20) / chunk.max(1)).max(1) as usize;
            let issue = &*Box::leak(Box::new(AtomicUsize::new(0)));
            for _ in 0..ra_threads {
                sc.spawn(move || loop {
                    if ra_done_ref.load(AtOrd::Relaxed) {
                        break;
                    }
                    let i = issue.fetch_add(1, AtOrd::Relaxed);
                    if i >= tasks_ref.len() {
                        break;
                    }
                    loop {
                        let c = claimed_ref.load(AtOrd::Relaxed);
                        if i <= c + win_tasks || ra_done_ref.load(AtOrd::Relaxed) {
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_micros(100));
                    }
                    if i + 1 < claimed_ref.load(AtOrd::Relaxed) {
                        continue; // workers already past it
                    }
                    let t = &tasks_ref[i];
                    readahead(&files_ref[t.file].file, t.start, t.end - t.start);
                });
            }
        }
        let states = par_tasks(
            tasks_ref.len(),
            threads,
            |w| P1 {
                worker: w,
                bits,
                ev: Scatter::new(1 << bits.s1),
                wide: Vec::new(),
                slow_seen: KeyMap::with_capacity(16),
                ck: Scatter::new(1 << bits.p),
                slow_u: Vec::new(),
                long_cids: Vec::new(),
                ck_pend: Vec::with_capacity(CK_BATCH),
                ck_defer: Vec::new(),
                buf: Vec::with_capacity((chunk + TAIL) as usize + PAD),
                masks: Masks::default(),
                err: None,
            },
            |st, ti| {
                if st.err.is_some() {
                    return;
                }
                claimed_ref.fetch_max(ti, AtOrd::Relaxed);
                let task = &tasks_ref[ti];
                let f = &files_ref[task.file];
                let mut buf = std::mem::take(&mut st.buf);
                let mut masks = std::mem::take(&mut st.masks);
                let res = match f.kind {
                    Kind::Events => run_chunk(f, task.start, task.end, &mut buf, |b, first, limit| {
                        for_each_record::<7>(b, first, limit, &mut masks, #[inline(always)] |fields| {
                            parse_event_line(st, fields, pctx_ref, arena_ref)
                        })
                    }),
                    Kind::Clicks => {
                        // Never wait for the linked-ID build: clicks parsed
                        // before it is ready are resolved after phase 1.
                        let dsu = dsu_cell_ref.get().and_then(|r| r.as_ref().ok());
                        let r = run_chunk(f, task.start, task.end, &mut buf, |b, first, limit| {
                            for_each_record::<4>(b, first, limit, &mut masks, #[inline(always)] |fields| {
                                parse_click_line(
                                    st,
                                    fields,
                                    pctx_ref,
                                    dsu,
                                    arena_ref,
                                    click_min_ts,
                                    we,
                                )
                            })
                        });
                        if let Some(dsu) = dsu {
                            flush_clicks(st, dsu, arena_ref);
                        }
                        r
                    }
                };
                st.buf = buf;
                st.masks = masks;
                if let Err(e) = res {
                    st.err = Some(e);
                }
            },
        );
        ra_done_ref.store(true, AtOrd::Relaxed);
        (states, dsu_thread.join().unwrap())
    });
    let t_p1end = Instant::now();
    let dsu = dsu_cell.into_inner().unwrap()?;
    // Resolve clicks deferred while the DSU was still building (parallel).
    let mut p1_states = p1_states;
    if p1_states.iter().any(|st| !st.ck_defer.is_empty()) {
        let dsu_r0 = &dsu;
        std::thread::scope(|sc| {
            for st in p1_states.iter_mut() {
                sc.spawn(move || {
                    let defer = std::mem::take(&mut st.ck_defer);
                    for (i, c) in defer.iter().enumerate() {
                        if let Some(nx) = defer.get(i + 16) {
                            dsu_r0.prefetch(nx.canon);
                        }
                        let mut c = *c;
                        c.canon = dsu_r0.resolve(c.canon);
                        st.ck.push(arena_ref, shard_of(c.canon, st.bits.p), c);
                    }
                });
            }
        });
    }
    let mut ev_blocks: Vec<Vec<Vec<Blk<Ev24>>>> = Vec::with_capacity(threads);
    let mut wides: Vec<Vec<EvRec>> = Vec::with_capacity(threads);
    let mut ck_blocks: Vec<Vec<Vec<Blk<CkRec>>>> = Vec::with_capacity(threads);
    let mut longs: Vec<Vec<Box<[u8]>>> = Vec::with_capacity(threads);
    let mut slow_u: Vec<(u64, Box<[u8]>)> = Vec::new();
    for mut st in p1_states {
        if let Some(e) = st.err.take() {
            return Err(e);
        }
        ev_blocks.push(st.ev.finish(&arena));
        ck_blocks.push(st.ck.finish(&arena));
        longs.push(std::mem::take(&mut st.long_cids));
        wides.push(std::mem::take(&mut st.wide));
        slow_u.append(&mut st.slow_u);
    }
    let t_parse = t1.elapsed();
    let t_tail = t_p1end.elapsed();
    let t_reg0 = Instant::now();

    // Slow-id registries (rare): bytes for output + 56-bit hash collision check.
    let (slow_names, slow_bytes) = {
        let mut all_u: Vec<(u64, Box<[u8]>)> = slow_u;
        all_u.extend(dsu.slow_bytes.iter().map(|(k, b)| (*k, b.clone())));
        all_u.sort_unstable();
        all_u.dedup();
        if all_u.windows(2).any(|w| w[0].0 == w[1].0) {
            eprintln!("[warn] slow user id hash collision");
        }
        let mut names = KeyMap::with_capacity(all_u.len().max(1));
        let mut bytes: Vec<Box<[u8]>> = Vec::with_capacity(all_u.len());
        for (k, b) in all_u {
            let (_, ins) = names.get_or_insert(k, bytes.len() as u32);
            if ins {
                bytes.push(b);
            }
        }
        (names, bytes)
    };

    // Wide records (purchases, out-of-range ingest deltas): counting-sort by
    // dedup shard, in parallel per worker list.
    let ns1 = 1usize << bits.s1;
    let hists: Vec<Vec<usize>> = std::thread::scope(|sc| {
        let hs: Vec<_> = wides
            .iter()
            .map(|w| {
                sc.spawn(move || {
                    let mut h = vec![0usize; ns1];
                    for r in w {
                        h[shard_of(r.key, bits.s1)] += 1;
                    }
                    h
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut woff = vec![0usize; ns1 + 1];
    // per-worker write cursors: shard-major, worker-minor
    let mut cursors: Vec<Vec<usize>> = vec![vec![0usize; ns1]; wides.len()];
    {
        let mut acc = 0usize;
        for sh in 0..ns1 {
            woff[sh] = acc;
            for (wi, h) in hists.iter().enumerate() {
                cursors[wi][sh] = acc;
                acc += h[sh];
            }
        }
        woff[ns1] = acc;
    }
    let mut wide_all: Vec<EvRec> = Vec::with_capacity(woff[ns1]);
    {
        let dst = wide_all.as_mut_ptr() as usize;
        std::thread::scope(|sc| {
            for (w, mut cur) in wides.iter().zip(cursors.into_iter()) {
                sc.spawn(move || {
                    let dst = dst as *mut EvRec;
                    for r in w {
                        let sh = shard_of(r.key, bits.s1);
                        unsafe { dst.add(cur[sh]).write(*r) };
                        cur[sh] += 1;
                    }
                });
            }
        });
        unsafe { wide_all.set_len(woff[ns1]) };
    }
    drop(wides);
    let t_reg = t_reg0.elapsed();
    // ---- phase 2: dedup per event-id shard
    let t2 = Instant::now();
    let dsu_r = &dsu;
    let evb = &ev_blocks;
    let woff_r = &woff;
    // shards own disjoint ranges of wide_all; losers are retired in place
    let wide_ptr = wide_all.as_mut_ptr() as usize;
    struct P2 {
        out: Scatter<PRec>,
        // interleaved [key, record address] slots; key 0 = empty
        tab: Vec<[u64; 2]>,
    }
    let p2_states = par_tasks(
        1 << bits.s1,
        threads,
        |_| P2 {
            out: Scatter::new(1 << bits.p),
            tab: Vec::new(),
        },
        |st, s| {
            let n_wide = woff_r[s + 1] - woff_r[s];
            let n: usize = evb
                .iter()
                .map(|w| w[s].iter().map(|b| b.n as usize).sum::<usize>())
                .sum::<usize>()
                + n_wide;
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two().max(64);
            if st.tab.len() < cap {
                st.tab = vec![[0u64; 2]; cap];
            } else {
                st.tab[..cap].fill([0, 0]);
            }
            let mask = cap - 1;
            let tab = &mut st.tab[..cap];
            let tab_base = tab.as_ptr() as usize;
            // Table value: record address; low bit set = wide EvRec.
            let ingest_of = |a: u64| -> i64 {
                unsafe {
                    if a & 1 == 0 {
                        (*(a as *const Ev24)).ingest(ws)
                    } else {
                        (*((a & !1) as *const EvRec)).ingest
                    }
                }
            };
            let retire = |a: u64| unsafe {
                if a & 1 == 0 {
                    (*(a as *mut Ev24)).user = 0;
                } else {
                    (*((a & !1) as *mut EvRec)).user = 0;
                }
            };
            let mut insert = |k: u64, ingest: i64, addr: u64| {
                let mut j = (mix64(k) as usize) & mask;
                loop {
                    let e = unsafe { tab.get_unchecked_mut(j) };
                    if e[0] == 0 {
                        *e = [k, addr];
                        return;
                    }
                    if e[0] == k {
                        // Duplicate: the loser is retired in place (user = 0),
                        // so the emit pass needs no probe.
                        if ingest < ingest_of(e[1]) {
                            retire(e[1]);
                            e[1] = addr;
                        } else {
                            retire(addr);
                        }
                        return;
                    }
                    j = (j + 1) & mask;
                }
            };
            for w in evb.iter() {
                for b in w[s].iter() {
                    // Raw pointers: losers are written in place (user = 0).
                    let base = b.ptr;
                    let nrec = b.n as usize;
                    for i in 0..nrec {
                        #[cfg(target_arch = "x86_64")]
                        if i + 8 < nrec {
                            let nk = unsafe { (*base.add(i + 8)).key };
                            unsafe {
                                core::arch::x86_64::_mm_prefetch(
                                    (tab_base as *const [u64; 2]).add((mix64(nk) as usize) & mask)
                                        as *const i8,
                                    core::arch::x86_64::_MM_HINT_T0,
                                );
                            }
                        }
                        let rp = unsafe { base.add(i) };
                        let (k, ingest) = unsafe { ((*rp).key, (*rp).ingest(ws)) };
                        insert(k, ingest, rp as u64);
                    }
                }
            }
            let wbase = (wide_ptr as *mut EvRec).wrapping_add(woff_r[s]);
            for i in 0..n_wide {
                let rp = unsafe { wbase.add(i) };
                let (k, ingest) = unsafe { ((*rp).key, (*rp).ingest) };
                insert(k, ingest, rp as u64 | 1);
            }
            // Winner emit: losers were retired during insert, so every record
            // still carrying a user is a valid winner.
            for w in evb.iter() {
                for b in w[s].iter() {
                    let recs = b.as_slice();
                    for (i, r) in recs.iter().enumerate() {
                        if let Some(nx) = recs.get(i + 12) {
                            if nx.user != 0 {
                                dsu_r.prefetch(nx.user);
                            }
                        }
                        if r.user == 0 {
                            continue;
                        }
                        let canon = dsu_r.resolve(r.user);
                        st.out.push(
                            arena_ref,
                            shard_of(canon, bits.p),
                            PRec {
                                canon,
                                ev: r.w & 0xFFFF_FFFF_0000_0000,
                            },
                        );
                    }
                }
            }
            for i in 0..n_wide {
                let r = unsafe { &*wbase.add(i) };
                if r.user == 0 {
                    continue;
                }
                let canon = dsu_r.resolve(r.user);
                st.out.push(
                    arena_ref,
                    shard_of(canon, bits.p),
                    PRec { canon, ev: r.ev },
                );
            }
            arena_ref.release(
                evb.iter()
                    .flat_map(|w| w[s].iter())
                    .map(|b| b.ptr as *mut u8),
            );
        },
    );
    let p_blocks: Vec<Vec<Vec<Blk<PRec>>>> = p2_states
        .into_iter()
        .map(|st| st.out.finish(&arena))
        .collect();
    let t_dedup = t2.elapsed();

    // ---- phase 3: sessionize per person partition
    let t3 = Instant::now();
    let ctx = Ctx {
        catalog: &catalog,
        ws,
        click_base: click_min_ts,
        day0,
        num_days,
        longs: &longs,
        slow_names: &slow_names,
        slow_bytes: &slow_bytes,
    };
    let num_camps = catalog.campaign_ids.len();
    let num_cats = catalog.categories.len();
    let ctx_r = &ctx;
    let pb = &p_blocks;
    let cb = &ck_blocks;
    let user_dir = out_dir.join("user_daily");
    fs::create_dir_all(&user_dir)?;
    let ud = &user_dir;
    let p3_states = par_tasks(
        1 << bits.p,
        threads,
        |w| P3 {
            pmap: KeyMap::with_capacity(1024),
            lids: Vec::new(),
            counts: Vec::new(),
            pkeys: Vec::new(),
            evs: Vec::new(),
            ck_lid: Vec::new(),
            ck_cnt: Vec::new(),
            cks: Vec::new(),
            ucols: (0..pq::USER_DAILY.len()).map(|_| Col::default()).collect(),
            worker: w,
            writer: None,
            rows_written: 0,
            err: None,
            grid: vec![CampDayAgg::default(); num_camps * num_days],
            camp_counts: vec![0; num_camps],
            in_active: vec![false; num_camps],
            active: Vec::with_capacity(64),
            day_cat_rev: vec![0; num_cats],
        },
        |st, p| {
            let pbl: Vec<&[Blk<PRec>]> = pb.iter().map(|t| t[p].as_slice()).collect();
            let cbl: Vec<&[Blk<CkRec>]> = cb.iter().map(|t| t[p].as_slice()).collect();
            process_partition(st, ctx_r, &pbl, &cbl);
            if st.ucols[0].n >= ROW_GROUP {
                flush_rows(st, ud);
            }
        },
    );
    let t_sess = t3.elapsed();

    // ---- phase 4: output (remaining user rows + campaign table)
    let t4 = Instant::now();
    let mut grid = vec![CampDayAgg::default(); num_camps * num_days];
    for st in &p3_states {
        for (g, a) in grid.iter_mut().zip(st.grid.iter()) {
            g.sessions += a.sessions;
            g.purchasing_sessions += a.purchasing_sessions;
            g.revenue_cents += a.revenue_cents;
            g.assisted_sessions += a.assisted_sessions;
        }
    }
    let p3_states: Vec<P3> = std::thread::scope(|sc| {
        let hs: Vec<_> = p3_states
            .into_iter()
            .map(|mut st| {
                sc.spawn(move || {
                    flush_rows(&mut st, ud);
                    if let Some(w) = st.writer.take() {
                        if let Err(e) = w.close() {
                            st.err.get_or_insert(e);
                        }
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut total_user_rows = 0usize;
    let mut any_file = false;
    for st in &p3_states {
        total_user_rows += st.rows_written;
        any_file |= st.rows_written > 0;
    }
    for st in p3_states {
        if let Some(e) = st.err {
            return Err(e);
        }
    }
    if !any_file {
        PqWriter::create(&user_dir.join("part-0.parquet"), "user_daily", &pq::USER_DAILY)?
            .close()?;
    }
    let camp_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&camp_dir)?;
    let mut cw = PqWriter::create(
        &camp_dir.join("part-0.parquet"),
        "campaign_daily",
        &pq::CAMPAIGN_DAILY,
    )?;
    for (cell, agg) in grid.iter().enumerate() {
        if agg.sessions > 0 || agg.assisted_sessions > 0 {
            let cid = cell / num_days;
            cw.cols[0].push_bytes(&catalog.campaign_ids[cid]);
            cw.cols[1].push_bytes(&catalog.campaign_channels[cid]);
            cw.cols[2].push_i32(day0 + (cell % num_days) as i32);
            cw.cols[3].push_i64(agg.sessions);
            cw.cols[4].push_i64(agg.purchasing_sessions);
            cw.cols[5].push_i64(agg.revenue_cents);
            cw.cols[6].push_i64(agg.assisted_sessions);
        }
    }
    let camp_rows_n = cw.close()?;
    let t_out = t4.elapsed();

    let anon_mb = fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|st| {
            st.lines()
                .find(|l| l.starts_with("RssAnon:"))
                .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()))
        })
        .unwrap_or(0)
        / 1024;
    eprintln!(
        "[bench] anon_mb={} setup={:.3}s p1tail={:.3}s reg={:.3}s threads={} cat={:.3}s dsu={:.3}s parse={:.3}s dedup={:.3}s sess={:.3}s out={:.3}s total={:.3}s user_rows={} camp_rows={}",
        anon_mb,
        t_setup.as_secs_f64(),
        t_tail.as_secs_f64(),
        t_reg.as_secs_f64(),
        threads,
        t_cat.as_secs_f64(),
        t_dsu.as_secs_f64(),
        t_parse.as_secs_f64(),
        t_dedup.as_secs_f64(),
        t_sess.as_secs_f64(),
        t_out.as_secs_f64(),
        t0.elapsed().as_secs_f64(),
        total_user_rows,
        camp_rows_n
    );
    std::mem::forget(ev_blocks);
    std::mem::forget(ck_blocks);
    std::mem::forget(p_blocks);
    std::mem::forget(longs);
    std::mem::forget(arena);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn temp_case(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("helex_unit_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("data/events")).unwrap();
        fs::create_dir_all(dir.join("data/clicks")).unwrap();
        fs::create_dir_all(dir.join("data/links")).unwrap();
        fs::create_dir_all(dir.join("out")).unwrap();
        fs::create_dir_all(dir.join("scratch")).unwrap();
        dir
    }

    #[test]
    fn campaign_reentry_after_organic_session_counts_assist_once() {
        let dir = temp_case("reentry");
        let data = dir.join("data");
        fs::write(
            data.join("manifest.json"),
            r#"{"window_start":"2026-10-01T00:00:00Z","window_end":"2026-10-25T00:00:00Z"}"#,
        )
        .unwrap();
        fs::write(data.join("products.csv"), "product_id,category\np1,shoes\n").unwrap();
        fs::write(
            data.join("campaigns.csv"),
            "campaign_id,channel\nc_1,search\n",
        )
        .unwrap();
        fs::write(data.join("links/part-0.csv"), "user_id_a,user_id_b\n").unwrap();
        fs::write(
            data.join("clicks/part-0.csv"),
            "click_id,user_id,campaign_id,ts\n\
             k1,u1,c_1,2026-10-01T09:00:00Z\n\
             k2,u1,c_1,2026-10-20T09:00:00Z\n",
        )
        .unwrap();
        fs::write(
            data.join("events/part-0.csv"),
            "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n\
             e1,u1,2026-10-01T10:00:00Z,2026-10-01T10:01:00Z,view,p1,\n\
             e2,u1,2026-10-12T10:00:00Z,2026-10-12T10:01:00Z,view,p1,\n\
             e3,u1,2026-10-20T10:00:00Z,2026-10-20T10:01:00Z,view,p1,\n",
        )
        .unwrap();

        let out = dir.join("out");
        let scratch = dir.join("scratch");
        run_pipeline(&data, &out, &scratch, false).unwrap();
        let parts = fs::read_dir(out.join("user_daily")).unwrap().count();
        assert!(parts >= 1);
        assert!(out.join("campaign_daily/part-0.parquet").is_file());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn chunk_reader_matches_sequential_lines() {
        let dir = std::env::temp_dir().join(format!("helex_chunk_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("a.csv");
        let mut content = b"col1,col2\n".to_vec();
        for i in 0..500 {
            if i % 37 == 0 {
                content.extend_from_slice(b"\n");
            }
            content.extend_from_slice(format!("row_{i},value_number_{i}\n").as_bytes());
        }
        content.extend_from_slice(b"last,no_newline");
        fs::write(&p, &content).unwrap();
        let mut expected: Vec<Vec<u8>> = Vec::new();
        crate::csv_dialect::iter_data_lines(&content, |l| expected.push(l.to_vec()));
        let mut files = Vec::new();
        open_inputs(&[p], Kind::Events, &mut files).unwrap();
        let f = &files[0];
        for chunk in [1u64, 2, 7, 13, 64, 4096] {
            let mut got: Vec<Vec<u8>> = Vec::new();
            let mut buf = Vec::new();
            let mut s = f.data_start;
            while s < f.len {
                let e = (s + chunk).min(f.len);
                run_chunk(f, s, e, &mut buf, |b, first, limit| {
                    process_lines(b, first, limit, |l| got.push(l.to_vec())).unwrap()
                })
                .unwrap();
                s = e;
            }
            assert_eq!(got, expected, "chunk={chunk}");
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
