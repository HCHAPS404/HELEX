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
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use crate::arena::{Arena, Blk, Scatter};
use crate::catalog::Catalog;
use crate::csv_dialect::parse_price_cents;
use crate::csv_scan::{for_each_record, Masks};
use crate::counters;
use crate::dsu::{csv_paths, load_links, Dsu};
use crate::keys::{fast_key_bytes, id_key, id_key_padded, is_slow, mix64, shard_of, KeyMap};
use crate::parquet_out::{
    write_campaign_daily, write_user_daily_part, CampaignDailyRow, UserDailyRow, UserDailyWriter,
    BATCH_ROWS,
};
use crate::sso::SmallBytes;
use crate::timestamp::parse_ts_padded;

/// Shard counts scale with the input so a dedup shard (~10k records) and a
/// person partition (~40k records) stay L2-sized.
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
            s1: lg(env("HELEX_S1_TARGET", 10_000)).clamp(6, 13),
            p: lg(env("HELEX_P_TARGET", 40_000)).clamp(4, 11),
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
const PAD: usize = 64;

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

/// Phase-2 survivor: canonical person key + packed event.
#[repr(C)]
#[derive(Clone, Copy)]
struct PRec {
    canon: u64,
    ev: u64,
}

/// Click resolved to a canonical person. `cid_hi/cid_lo` hold the click id
/// big-endian zero-padded (exact order for ids <= 16 bytes without NUL);
/// otherwise `long != 0` and (hi, lo) = (worker, index) into the long-id side
/// lists.
#[repr(C)]
#[derive(Clone, Copy)]
struct CkRec {
    canon: u64,
    ts: i64,
    cid_hi: u64,
    cid_lo: u64,
    camp: u32,
    long: u32,
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
    /// recovered generator event key: (key, shard_shift, index_mask)
    dense_e: Option<(u64, u32, u64)>,
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
        // Header = first line.
        let mut probe = vec![0u8; 64 << 10];
        let mut off = 0u64;
        let data_start = loop {
            let want = (probe.len() as u64).min(len - off) as usize;
            file.read_exact_at(&mut probe[..want], off)?;
            if let Some(nl) = memchr::memchr(b'\n', &probe[..want]) {
                break Some(off + nl as u64 + 1);
            }
            off += want as u64;
            if off >= len {
                break None;
            }
        };
        let Some(data_start) = data_start else {
            continue;
        };
        if data_start >= len {
            continue;
        }
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
    let from = if start == f.data_start {
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
            body(&m[from as usize..stop], start == f.data_start, limit);
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
    body(&buf[..want], start == f.data_start, limit);
    Ok(())
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
    ev: Scatter<EvRec>,
    ck: Scatter<CkRec>,
    // Slow ids: (key, slab offset) pairs; slab entries are [len][bytes].
    slow_u: Vec<(u64, u32)>,
    slow_seen: KeyMap,
    slab: Vec<u8>,
    long_cids: Vec<Box<[u8]>>,
    buf: Vec<u8>,
    masks: Masks,
    err: Option<io::Error>,
}

#[inline(always)]
fn push_slow(list: &mut Vec<(u64, u32)>, slab: &mut Vec<u8>, key: u64, bytes: &[u8]) {
    list.push((key, slab.len() as u32));
    slab.push(bytes.len() as u8);
    slab.extend_from_slice(bytes);
}

#[inline]
fn slab_get(slab: &[u8], off: u32) -> &[u8] {
    let n = slab[off as usize] as usize;
    &slab[off as usize + 1..off as usize + 1 + n]
}

#[inline]
fn cid_bytes<'a>(c: &CkRec, longs: &'a [Vec<Box<[u8]>>], tmp: &'a mut [u8; 16]) -> &'a [u8] {
    if c.long != 0 {
        return &longs[c.cid_hi as usize][c.cid_lo as usize];
    }
    tmp[..8].copy_from_slice(&c.cid_hi.to_be_bytes());
    tmp[8..].copy_from_slice(&c.cid_lo.to_be_bytes());
    let n = 16 - tmp.iter().rev().take_while(|&&x| x == 0).count();
    &tmp[..n]
}

#[inline]
fn cid_cmp(a: &CkRec, b: &CkRec, longs: &[Vec<Box<[u8]>>]) -> Ordering {
    if (a.long | b.long) == 0 {
        (a.cid_hi, a.cid_lo).cmp(&(b.cid_hi, b.cid_lo))
    } else {
        let mut ta = [0u8; 16];
        let mut tb = [0u8; 16];
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
    rows: Vec<UserDailyRow>,
    worker: usize,
    writer: Option<UserDailyWriter>,
    rows_written: usize,
    err: Option<io::Error>,
    grid: Vec<CampDayAgg>,
    camp_counts: Vec<u32>,
    in_active: Vec<bool>,
    active: Vec<u32>,
    day_cat_rev: Vec<u64>,
    part_durs: Vec<(usize, f64, usize)>,
}

struct Ctx<'a> {
    catalog: &'a Catalog,
    ws: i64,
    day0: i32,
    num_days: usize,
    longs: &'a [Vec<Box<[u8]>>],
    slow_names: &'a KeyMap,
    slow_bytes: &'a [Box<[u8]>],
}

impl Ctx<'_> {
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

    let mut person: Option<SmallBytes> = None;
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
            let name = person.get_or_insert_with(|| ctx.person_name(canon)).clone();
            st.rows.push(UserDailyRow {
                user_id: name,
                day: cur_day,
                sessions: day_sessions,
                events: day_events,
                revenue_cents: day_rev,
                top_category: tc,
            });
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

        while right < clicks.len() && clicks[right].ts <= sess_start {
            let cid = clicks[right].camp as usize;
            if !st.in_active[cid] {
                st.in_active[cid] = true;
                st.active.push(cid as u32);
            }
            st.camp_counts[cid] += 1;
            right += 1;
        }
        let min_click_ts = sess_start - CLICK_REACH_MS;
        while left < right && clicks[left].ts < min_click_ts {
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
        let name = person.unwrap_or_else(|| ctx.person_name(canon));
        st.rows.push(UserDailyRow {
            user_id: name,
            day: cur_day,
            sessions: day_sessions,
            events: day_events,
            revenue_cents: day_rev,
            top_category: tc,
        });
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
        ts: 0,
        cid_hi: 0,
        cid_lo: 0,
        camp: 0,
        long: 0,
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

fn flush_rows(st: &mut P3, dir: &Path, cats: &[Vec<u8>]) {
    if st.rows.is_empty() || st.err.is_some() {
        return;
    }
    if st.writer.is_none() {
        match UserDailyWriter::create(&dir.join(format!("part-{}.parquet", st.worker)), cats) {
            Ok(w) => st.writer = Some(w),
            Err(e) => {
                st.err = Some(e);
                return;
            }
        }
    }
    let w = st.writer.as_mut().unwrap();
    if let Err(e) = w.write_batch(&st.rows) {
        st.err = Some(e);
        return;
    }
    st.rows_written += st.rows.len();
    st.rows.clear();
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
    let ctr = pc
        .dense_e
        .and_then(|(k, _, _)| counters::decode(event_id, b'e', k, counters::EVENT_CAP));
    let (key, dshard, dflag) = match (ctr, pc.dense_e) {
        (Some(c), Some((_, shift, _))) => (c, Some(c >> shift), i64::MIN),
        _ => (unsafe { id_key_padded(event_id, b'e') }.0, None, 0),
    };
    let (user, ev) = match validate_event_copy(fields, ingest, pc) {
        Some(ev) => {
            let (uk, slow_u) = unsafe { id_key_padded(fields[1], b'u') };
            if slow_u && st.slow_seen.get_or_insert(uk, 0).1 {
                push_slow(&mut st.slow_u, &mut st.slab, uk, fields[1]);
            }
            (uk, ev)
        }
        None => (0, 0),
    };
    let shard = dshard.map(|s| s as usize).unwrap_or_else(|| shard_of(key, st.bits.s1));
    st.ev.push(
        arena,
        shard,
        EvRec {
            key,
            ingest: ingest | dflag,
            user,
            ev,
        },
    );
}

#[inline(always)]
fn parse_click_line(
    st: &mut P1,
    fields: &[&[u8]; 4],
    pc: &PCtx,
    dsu: &Dsu,
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
    if slow_u && st.slow_seen.get_or_insert(uk, 0).1 {
        push_slow(&mut st.slow_u, &mut st.slab, uk, user_raw);
    }
    let canon = dsu.resolve(uk);
    let (cid_hi, cid_lo, long) = if click_id.len() <= 16 && memchr::memchr(0, click_id).is_none() {
        let mut b = [0u8; 16];
        b[..click_id.len()].copy_from_slice(click_id);
        (
            u64::from_be_bytes(b[..8].try_into().unwrap()),
            u64::from_be_bytes(b[8..].try_into().unwrap()),
            0u32,
        )
    } else {
        st.long_cids.push(click_id.into());
        (st.worker as u64, (st.long_cids.len() - 1) as u64, 1u32)
    };
    st.ck.push(
        arena,
        shard_of(canon, st.bits.p),
        CkRec {
            canon,
            ts,
            cid_hi,
            cid_lo,
            camp: u32::from(camp),
            long,
        },
    );
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
    let catalog = Catalog::load(data_dir)?;
    if catalog.categories.len() > 127 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "category count exceeds packed-event capacity",
        ));
    }
    if catalog.window_end - catalog.window_start > i64::from(u32::MAX) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "window exceeds packed-event capacity",
        ));
    }
    let ws = catalog.window_start;
    let we = catalog.window_end;
    crate::timestamp::install_ts_ctx(
        (ws - 8 * DAY_MS).div_euclid(DAY_MS),
        (we + 6 * DAY_MS).div_euclid(DAY_MS),
    );
    let day0 = ws.div_euclid(DAY_MS) as i32;
    let num_days = ((we - 1).div_euclid(DAY_MS) as i32 - day0) as usize + 1;
    let click_min_ts = ws - CLICK_REACH_MS;
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
    // Recover the generator's id->counter keys from the events heads; when
    // it converges, dense counters give direct-index dedup (byte-key path
    // stays for everything that does not decode).
    let heads: Vec<Vec<u8>> = files
        .iter()
        .filter(|f| f.kind == Kind::Events)
        .map(|f| {
            let want = ((f.len - f.data_start).min(256 << 10)) as usize;
            if let Some(m) = &f.map {
                m[f.data_start as usize..f.data_start as usize + want].to_vec()
            } else {
                let mut b = vec![0u8; want];
                let _ = f.file.read_exact_at(&mut b, f.data_start);
                b
            }
        })
        .collect();
    let dense_e = counters::recover(&heads).and_then(|k| {
        if k.ok && bits.s1 <= 28 {
            Some((k.event, 28 - bits.s1, (1u64 << (28 - bits.s1)) - 1))
        } else {
            None
        }
    });
    eprintln!("[dense] enabled={}", dense_e.is_some());
    let pctx = PCtx {
        catalog: &catalog,
        prod,
        camp,
        dense_e,
    };
    let pctx_ref = &pctx;
    let arena = Arena::new();
    let dsu_cell: OnceLock<io::Result<Dsu>> = OnceLock::new();
    let files_ref = &files;
    let tasks_ref = &tasks;
    let arena_ref = &arena;
    let dsu_cell_ref = &dsu_cell;

    let (p1_states, t_dsu) = std::thread::scope(|sc| {
        let dsu_thread = sc.spawn(|| {
            let t = Instant::now();
            let _ = dsu_cell_ref.set(load_links(data_dir, threads));
            t.elapsed()
        });
        let states = par_tasks(
            tasks_ref.len(),
            threads,
            |w| P1 {
                worker: w,
                bits,
                ev: Scatter::new(1 << bits.s1),
                ck: Scatter::new(1 << bits.p),
                slow_u: Vec::new(),
                slow_seen: KeyMap::with_capacity(1024),
                slab: Vec::new(),
                long_cids: Vec::new(),
                buf: Vec::with_capacity((chunk + TAIL) as usize + PAD),
                masks: Masks::default(),
                err: None,
            },
            |st, ti| {
                if st.err.is_some() {
                    return;
                }
                let task = &tasks_ref[ti];
                let f = &files_ref[task.file];
                let mut buf = std::mem::take(&mut st.buf);
                let mut masks = std::mem::take(&mut st.masks);
                let res = match f.kind {
                    Kind::Events => run_chunk(f, task.start, task.end, &mut buf, |b, first, limit| {
                        for_each_record::<7>(b, first, limit, &mut masks, |fields| {
                            parse_event_line(st, fields, pctx_ref, arena_ref)
                        })
                    }),
                    Kind::Clicks => {
                        let dsu = loop {
                            if let Some(d) = dsu_cell_ref.get() {
                                break d;
                            }
                            std::thread::yield_now();
                        };
                        match dsu {
                            Ok(dsu) => run_chunk(f, task.start, task.end, &mut buf, |b, first, limit| {
                                for_each_record::<4>(b, first, limit, &mut masks, |fields| {
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
                            }),
                            Err(_) => Ok(()),
                        }
                    }
                };
                st.buf = buf;
                st.masks = masks;
                if let Err(e) = res {
                    st.err = Some(e);
                }
            },
        );
        (states, dsu_thread.join().unwrap())
    });
    let dsu = dsu_cell.into_inner().unwrap()?;
    let mut ev_blocks: Vec<Vec<Vec<Blk<EvRec>>>> = Vec::with_capacity(threads);
    let mut ck_blocks: Vec<Vec<Vec<Blk<CkRec>>>> = Vec::with_capacity(threads);
    let mut longs: Vec<Vec<Box<[u8]>>> = Vec::with_capacity(threads);
    let mut slow_u: Vec<(u64, u32)> = Vec::new();
    let mut slab: Vec<u8> = Vec::new();
    for mut st in p1_states {
        if let Some(e) = st.err.take() {
            return Err(e);
        }
        ev_blocks.push(st.ev.finish(&arena));
        ck_blocks.push(st.ck.finish(&arena));
        longs.push(std::mem::take(&mut st.long_cids));
        let base = slab.len() as u32;
        slab.extend_from_slice(&st.slab);
        slow_u.extend(st.slow_u.iter().map(|&(k, o)| (k, o + base)));
    }
    let t_parse = t1.elapsed();

    // Slow-id registries (rare): bytes for output + 56-bit hash collision check.
    let (slow_names, slow_bytes) = {
        let mut all_u: Vec<(u64, u32)> = slow_u;
        for (k, b) in dsu.slow_bytes.iter() {
            all_u.push((*k, slab.len() as u32));
            slab.push(b.len() as u8);
            slab.extend_from_slice(b);
        }
        all_u.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| slab_get(&slab, a.1).cmp(slab_get(&slab, b.1)))
        });
        all_u.dedup_by(|a, b| a.0 == b.0 && slab_get(&slab, a.1) == slab_get(&slab, b.1));
        if all_u.windows(2).any(|w| w[0].0 == w[1].0) {
            eprintln!("[warn] slow user id hash collision");
        }
        let mut names = KeyMap::with_capacity(all_u.len().max(1));
        let mut bytes: Vec<Box<[u8]>> = Vec::with_capacity(all_u.len());
        for (k, off) in all_u {
            let (_, ins) = names.get_or_insert(k, bytes.len() as u32);
            if ins {
                bytes.push(slab_get(&slab, off).into());
            }
        }
        (names, bytes)
    };

    let t_reg = t1.elapsed() - t_parse;

    // ---- phase 2: dedup per event-id shard
    let t2 = Instant::now();
    let dsu_r = &dsu;
    let evb = &ev_blocks;
    struct P2 {
        out: Scatter<PRec>,
        // interleaved [key, winner ingest, winner index] slots; key 0 = empty
        tab: Vec<[u64; 3]>,
        // dense: [shard_tag, (ingest<<23)|(idx+1)] direct-indexed slots;
        // stale tags from previous shards read as empty (no clearing).
        dtab: Vec<[u64; 2]>,
        // dense overflow hash (idx cap / huge ingest): [counter+1, ingest, i]
        dhash: Vec<[u64; 3]>,
        dfill: usize,
        // bit i set => record i of this shard is the current table winner;
        // emit becomes a sequential bit scan instead of a probe per record.
        win: Vec<u64>,
    }
    let dense_shift = dense_e.map(|(_, s, _)| s);
    let p2_states = par_tasks(
        1 << bits.s1,
        threads,
        |_| P2 {
            out: Scatter::new(1 << bits.p),
            tab: Vec::new(),
            dtab: Vec::new(),
            dhash: Vec::new(),
            dfill: 0,
            win: Vec::new(),
        },
        |st, s| {
            let n: usize = evb
                .iter()
                .map(|w| w[s].iter().map(|b| b.n as usize).sum::<usize>())
                .sum();
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two().max(64);
            if st.tab.len() < cap {
                st.tab = vec![[0u64; 3]; cap];
            } else {
                st.tab[..cap].fill([0, 0, 0]);
            }
            let nw = n.div_ceil(64);
            if st.win.len() < nw {
                st.win = vec![0u64; nw];
            } else {
                st.win[..nw].fill(0);
            }
            let mask = cap - 1;
            let tab = &mut st.tab[..cap];
            let win = &mut st.win[..nw];
            // Dense direct table: covers counters [s<<shift, (s+1)<<shift).
            let (dsize, dmask, dtag) = match dense_shift {
                Some(sh) => (1usize << sh, (1u64 << sh) - 1, s as u64 + 1),
                None => (0, 0, 0),
            };
            if dsize > 0 {
                if st.dtab.len() < dsize {
                    st.dtab = vec![[0u64; 2]; dsize];
                }
                if st.dhash.len() < 64 {
                    st.dhash = vec![[0u64; 3]; 64];
                }
                st.dhash.iter_mut().for_each(|e| *e = [0; 3]);
                st.dfill = 0;
            }
            let dl = st.dtab.len().min(dsize);
            let dtab = &mut st.dtab[..dl];
            let mut i: u32 = 0;
            for w in evb.iter() {
                for b in w[s].iter() {
                    let recs = b.as_slice();
                    'rec: for (j8, r) in recs.iter().enumerate() {
                        #[cfg(target_arch = "x86_64")]
                        if let Some(nx) = recs.get(j8 + 8) {
                            unsafe {
                                let p = if nx.ingest < 0 {
                                    dtab.as_ptr().add((nx.key & dmask) as usize) as *const i8
                                } else {
                                    tab.as_ptr().add((mix64(nx.key) as usize) & mask) as *const i8
                                };
                                core::arch::x86_64::_mm_prefetch(p, core::arch::x86_64::_MM_HINT_T0);
                            }
                        }
                        let mut ri = r.ingest;
                        if ri < 0 {
                            // Dense record: counter-direct slot or overflow hash.
                            ri &= i64::MAX;
                            let ing = ri as u64;
                            if ing < (1u64 << 41) && (i as u64) < ((1u64 << 23) - 1) {
                                let e = unsafe { dtab.get_unchecked_mut((r.key & dmask) as usize) };
                                if e[0] != dtag {
                                    e[0] = dtag;
                                    e[1] = (ing << 23) | (i as u64 + 1);
                                    win[i as usize >> 6] |= 1u64 << (i & 63);
                                } else if ing < (e[1] >> 23) {
                                    let old = ((e[1] & ((1u64 << 23) - 1)) - 1) as usize;
                                    win[old >> 6] &= !(1u64 << (old & 63));
                                    win[i as usize >> 6] |= 1u64 << (i & 63);
                                    e[1] = (ing << 23) | (i as u64 + 1);
                                }
                                i += 1;
                                continue;
                            }
                            let k = r.key + 1;
                            // Existing key probes are cheap; only a fresh
                            // insert may need the table to grow first.
                            loop {
                                let dmask2 = st.dhash.len() - 1;
                                let mut j = (mix64(k) as usize) & dmask2;
                                let mut full = false;
                                loop {
                                    let e = unsafe { st.dhash.get_unchecked_mut(j) };
                                    if e[0] == 0 {
                                        full = true;
                                        break;
                                    }
                                    if e[0] == k {
                                        if ri < e[1] as i64 {
                                            let old = e[2] as usize;
                                            win[old >> 6] &= !(1u64 << (old & 63));
                                            win[i as usize >> 6] |= 1u64 << (i & 63);
                                            e[1] = ri as u64;
                                            e[2] = i as u64;
                                        }
                                        i += 1;
                                        continue 'rec;
                                    }
                                    j = (j + 1) & dmask2;
                                }
                                if full && (st.dfill + 1) * 2 <= st.dhash.len() {
                                    let e = unsafe { st.dhash.get_unchecked_mut(j) };
                                    *e = [k, ri as u64, i as u64];
                                    st.dfill += 1;
                                    win[i as usize >> 6] |= 1u64 << (i & 63);
                                    i += 1;
                                    continue 'rec;
                                }
                                // dhash too full: rehash at 2x and retry.
                                let nl = st.dhash.len() * 2;
                                let old = std::mem::replace(&mut st.dhash, vec![[0u64; 3]; nl]);
                                let nm = st.dhash.len() - 1;
                                for e in old {
                                    if e[0] != 0 {
                                        let mut j = (mix64(e[0]) as usize) & nm;
                                        while st.dhash[j][0] != 0 {
                                            j = (j + 1) & nm;
                                        }
                                        st.dhash[j] = e;
                                    }
                                }
                            }
                        }
                        let k = r.key;
                        let mut j = (mix64(k) as usize) & mask;
                        loop {
                            let e = unsafe { tab.get_unchecked_mut(j) };
                            if e[0] == 0 {
                                *e = [k, r.ingest as u64, i as u64];
                                win[i as usize >> 6] |= 1u64 << (i & 63);
                                break;
                            }
                            if e[0] == k {
                                if r.ingest < e[1] as i64 {
                                    let old = e[2] as usize;
                                    win[old >> 6] &= !(1u64 << (old & 63));
                                    win[i as usize >> 6] |= 1u64 << (i & 63);
                                    e[1] = r.ingest as u64;
                                    e[2] = i as u64;
                                }
                                break;
                            }
                            j = (j + 1) & mask;
                        }
                        i += 1;
                    }
                }
            }
            // Winner emit: sequential re-scan of the records (the table is
            // cache-resident), guided by the winner bitmap instead of a
            // table probe per record.
            let mut i: u32 = 0;
            for w in evb.iter() {
                for b in w[s].iter() {
                    for r in b.as_slice() {
                        if (win[i as usize >> 6] >> (i & 63)) & 1 == 1 && r.user != 0 {
                            let canon = dsu_r.resolve(r.user);
                            st.out.push(
                                arena_ref,
                                shard_of(canon, bits.p),
                                PRec { canon, ev: r.ev },
                            );
                        }
                        i += 1;
                    }
                }
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
    let cats = &catalog.categories;
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
            rows: Vec::with_capacity(BATCH_ROWS + 4096),
            worker: w,
            writer: None,
            rows_written: 0,
            err: None,
            grid: vec![CampDayAgg::default(); num_camps * num_days],
            camp_counts: vec![0; num_camps],
            in_active: vec![false; num_camps],
            active: Vec::with_capacity(64),
            day_cat_rev: vec![0; num_cats],
            part_durs: Vec::new(),
        },
        |st, p| {
            let pbl: Vec<&[Blk<PRec>]> = pb.iter().map(|t| t[p].as_slice()).collect();
            let cbl: Vec<&[Blk<CkRec>]> = cb.iter().map(|t| t[p].as_slice()).collect();
            let pt = Instant::now();
            process_partition(st, ctx_r, &pbl, &cbl);
            st.part_durs.push((
                p,
                pt.elapsed().as_secs_f64(),
                pbl.iter()
                    .map(|b| b.iter().map(|x| x.n as usize).sum::<usize>())
                    .sum(),
            ));
            if st.rows.len() >= BATCH_ROWS {
                flush_rows(st, ud, cats);
            }
        },
    );
    let t_sess = t3.elapsed();
    if std::env::var("HELEX_PTAIL").is_ok() {
        let mut d: Vec<_> = p3_states
            .iter()
            .flat_map(|s| s.part_durs.iter().copied())
            .collect();
        d.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let tot: f64 = d.iter().map(|x| x.1).sum();
        for &(p, dur, nrec) in d.iter().take(8) {
            eprintln!("[ptail] part={p} dur={dur:.3}s recs={nrec}");
        }
        eprintln!("[ptail] sum={tot:.3}s top8={:.3}s parts={}", d.iter().take(8).map(|x| x.1).sum::<f64>(), d.len());
    }

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
                    flush_rows(&mut st, ud, cats);
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
        write_user_daily_part(&user_dir.join("part-0.parquet"), &[], &catalog.categories)?;
    }
    let mut camp_rows: Vec<CampaignDailyRow> = Vec::new();
    for (cell, agg) in grid.iter().enumerate() {
        if agg.sessions > 0 || agg.assisted_sessions > 0 {
            let cid = cell / num_days;
            camp_rows.push(CampaignDailyRow {
                campaign_id: catalog.campaign_ids[cid].clone(),
                channel: catalog.campaign_channels[cid].clone(),
                day: day0 + (cell % num_days) as i32,
                sessions: agg.sessions,
                purchasing_sessions: agg.purchasing_sessions,
                revenue_cents: agg.revenue_cents,
                assisted_sessions: agg.assisted_sessions,
            });
        }
    }
    write_campaign_daily(out_dir, &camp_rows)?;
    let t_out = t4.elapsed();

    eprintln!(
        "[bench] threads={} cat={:.3}s dsu={:.3}s parse={:.3}s reg={:.3}s dedup={:.3}s sess={:.3}s out={:.3}s total={:.3}s user_rows={} camp_rows={}",
        threads,
        t_cat.as_secs_f64(),
        t_dsu.as_secs_f64(),
        t_parse.as_secs_f64(),
        t_reg.as_secs_f64(),
        t_dedup.as_secs_f64(),
        t_sess.as_secs_f64(),
        t_out.as_secs_f64(),
        t0.elapsed().as_secs_f64(),
        total_user_rows,
        camp_rows.len()
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
