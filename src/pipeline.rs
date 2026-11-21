use std::cmp::Reverse;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use crate::catalog::Catalog;
use crate::csv_dialect::{parse_price_cents, split_fields_csv};
use crate::dsu::csv_paths;
use crate::io_pool::{parallel_for, process_csv_files_parallel, worker_count};
use crate::mem_guard::should_spill_events;
use crate::parquet_out::{
    write_campaign_daily, write_user_daily_part, CampaignDailyRow, UserDailyRow,
};
use crate::sso::{hash64_bytes, SmallBytes};
use crate::timestamp::{parse_f1_fast, parse_timestamp};

const NUM_SHARDS: usize = 256;
const DAY_MS: i64 = 86_400_000;
const LATE_LIMIT_MS: i64 = 86_400_000;
const SESSION_GAP_MS: i64 = 1_800_000;
const CLICK_REACH_MS: i64 = 7 * DAY_MS;

// Packed event: rel_ms (u32, window-relative) in high bits, then
// price_cents:24 | cat_id:7 | is_purchase:1 in low bits.
#[inline]
fn pack_ev(rel_ms: u32, price_cents: u32, cat_id: u16, is_purchase: bool) -> u64 {
    (u64::from(rel_ms) << 32)
        | ((u64::from(price_cents) & 0xFF_FFFF) << 8)
        | ((u64::from(cat_id) & 0x7F) << 1)
        | u64::from(is_purchase)
}

#[inline]
fn ev_ts(ws: i64, ev: u64) -> i64 {
    ws + (ev >> 32) as i64
}

#[inline]
fn ev_price(ev: u64) -> u32 {
    ((ev >> 8) & 0xFF_FFFF) as u32
}

#[inline]
fn ev_cat(ev: u64) -> u16 {
    ((ev >> 1) & 0x7F) as u16
}

#[inline]
fn ev_purch(ev: u64) -> bool {
    ev & 1 != 0
}

// 16B survivor record: dense person index (canon_shard<<24 | local) + packed event.
struct Rec12 {
    ev: u64,
    person_idx: u32,
    _pad: u32,
}

// Click candidate partitioned by user shard; resolved to person_idx after the
// user->person map is built.
struct ClickCand {
    user: SmallBytes,
    ts: i64,
    click_id: SmallBytes,
    camp_id: u16,
}

struct ResClick {
    person_idx: u32,
    ts: i64,
    click_id: SmallBytes,
    camp_id: u16,
}

struct ClickItem {
    ts: i64,
    click_id: SmallBytes,
    camp_id: u16,
}

#[derive(Default, Clone, Copy)]
struct CampDayAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

const ID_TAG: u64 = 1 << 63;
const FLAG_INVALID: u8 = 1;
const EMPTY_LOC: u32 = u32::MAX;

// 48-byte dedup record. event_id/user_id encode to exact u64 keys when they
// match the generator's shape (prefix byte + <=8 ASCII payload bytes):
//   key = be(payload) | ID_TAG   for  prefix + len in 2..=9
//   key = be(field)             for  len <= 8 with b0 < 0x80
// Otherwise hash64 + the raw bytes live in the shard side arena at *_loc
// ([u32 len][bytes]). u64 equality is exact for inline keys; byte-compare is
// only needed when a *_loc side reference is present.
#[repr(C)]
#[derive(Clone, Copy)]
struct EvRecord {
    key: u64,
    ingest_ms: i64,
    user_key: u64,
    ev: u64,
    id_loc: u32,
    user_loc: u32,
    flags: u8,
    _pad: [u8; 7],
}
const _: () = assert!(std::mem::size_of::<EvRecord>() == 48);

struct ShardBuf {
    recs: Vec<EvRecord>,
    side: Vec<u8>,
}

impl ShardBuf {
    fn new() -> Self {
        Self {
            recs: Vec::new(),
            side: Vec::new(),
        }
    }

    #[inline]
    fn put_side(&mut self, raw: &[u8]) -> u32 {
        let off = self.side.len() as u32;
        self.side
            .extend_from_slice(&(raw.len() as u32).to_le_bytes());
        self.side.extend_from_slice(raw);
        off
    }
}

#[inline]
fn field_key(f: &[u8], prefix: u8) -> (u64, bool) {
    let n = f.len();
    if n >= 2 && n <= 9 && f[0] == prefix && f[1] < 0x80 && memchr::memchr(0, &f[1..]).is_none()
    {
        let mut b = [0u8; 8];
        b[..n - 1].copy_from_slice(&f[1..]);
        (u64::from_be_bytes(b) | ID_TAG, false)
    } else if n >= 1 && n <= 8 && f[0] < 0x80 && memchr::memchr(0, f).is_none() {
        let mut b = [0u8; 8];
        b[..n].copy_from_slice(f);
        (u64::from_be_bytes(b), false)
    } else {
        (hash64_bytes(f), true)
    }
}

#[inline]
fn key_bytes<'a>(
    key: u64,
    prefix: u8,
    side: &'a [u8],
    loc: u32,
    tmp: &'a mut [u8; 9],
) -> &'a [u8] {
    if loc != u32::MAX {
        let o = loc as usize;
        let len = u32::from_le_bytes(side[o..o + 4].try_into().unwrap()) as usize;
        return &side[o + 4..o + 4 + len];
    }
    let b = (key & !ID_TAG).to_be_bytes();
    let n = 8 - b.iter().rev().take_while(|&&x| x == 0).count();
    if key & ID_TAG != 0 {
        tmp[0] = prefix;
        tmp[1..1 + n].copy_from_slice(&b[..n]);
        &tmp[..1 + n]
    } else {
        tmp[..n].copy_from_slice(&b[..n]);
        &tmp[..n]
    }
}

#[inline]
fn mix64(x: u64) -> u64 {
    x.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

#[inline]
fn validate_event_copy<'a>(
    fields: [&'a [u8]; 7],
    ingest_ms: i64,
    catalog: &Catalog,
) -> Option<(&'a [u8], u64)> {
    let user_raw = fields[1];
    if user_raw.is_empty() {
        return None;
    }
    let product_raw = fields[5];
    if product_raw.is_empty() {
        return None;
    }
    let (is_purchase, price_cents) = match fields[4] {
        b"view" | b"cart" => (false, 0u32),
        b"purchase" => (true, parse_price_cents(fields[6])?),
        _ => return None,
    };
    let ts = parse_timestamp(fields[2])?;
    if ts < catalog.window_start || ts >= catalog.window_end {
        return None;
    }
    if ingest_ms - ts > LATE_LIMIT_MS {
        return None;
    }
    let cat_id = if is_purchase {
        catalog.category_of(product_raw)
    } else {
        0
    };
    let rel_ms = u32::try_from(ts - catalog.window_start).ok()?;
    Some((user_raw, pack_ev(rel_ms, price_cents, cat_id, is_purchase)))
}

// Open-addressing dedup table: keys[i] is the record's u64 key, vals[i] packs
// [buf:8 | rec:24]. Winner is the copy with minimum ingest_ms; emit skips it
// if that copy is invalid. Inline (loc == MAX) keys compare by value alone.
struct DedupTab {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
}

impl DedupTab {
    fn new() -> Self {
        Self {
            keys: Vec::new(),
            vals: Vec::new(),
            mask: 0,
        }
    }

    #[inline]
    fn reset(&mut self, est: usize) {
        let cap = (est * 2).next_power_of_two().max(64);
        if self.keys.len() != cap {
            self.keys.resize(cap, 0);
            self.vals.resize(cap, EMPTY_LOC);
        }
        self.vals.fill(EMPTY_LOC);
        self.mask = cap - 1;
    }
}

// Phase A: insert all records into the shard dedup table, then sweep winners.
// Returns (winner locs [buf:8|rec:24], staged user_keys per user-shard,
// slow users as (key, bytes)). Winners are only the valid copies; invalid
// winners are dropped here so emit never sees them.
fn dedup_insert_collect(
    bufs: &[ShardBuf],
    tab: &mut DedupTab,
    ins_nanos: &std::sync::atomic::AtomicU64,
) -> (Vec<u32>, Vec<Vec<u64>>, Vec<(u64, SmallBytes)>) {
    let est: usize = bufs.iter().map(|b| b.recs.len()).sum();
    tab.reset(est);
    let mut tmp_a = [0u8; 9];
    let mut tmp_b = [0u8; 9];
    let t_ins = std::time::Instant::now();
    for (buf_idx, sb) in bufs.iter().enumerate() {
        for (rec_idx, rec) in sb.recs.iter().enumerate() {
            let key = rec.key;
            let mut i = (mix64(key) as usize) & tab.mask;
            loop {
                let v = tab.vals[i];
                if v == EMPTY_LOC {
                    tab.keys[i] = key;
                    tab.vals[i] = ((buf_idx as u32) << 24) | rec_idx as u32;
                    break;
                }
                if tab.keys[i] == key {
                    let eb = &bufs[(v >> 24) as usize];
                    let erec = &eb.recs[(v & 0xFF_FFFF) as usize];
                    let same = if rec.id_loc == u32::MAX && erec.id_loc == u32::MAX {
                        true
                    } else {
                        key_bytes(rec.key, b'e', &sb.side, rec.id_loc, &mut tmp_a)
                            == key_bytes(
                                erec.key,
                                b'e',
                                &eb.side,
                                erec.id_loc,
                                &mut tmp_b,
                            )
                    };
                    if same {
                        if rec.ingest_ms < erec.ingest_ms {
                            tab.vals[i] = ((buf_idx as u32) << 24) | rec_idx as u32;
                        }
                        break;
                    }
                }
                i = (i + 1) & tab.mask;
            }
        }
    }
    ins_nanos.fetch_add(
        t_ins.elapsed().as_nanos() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );

    let mut winners: Vec<u32> = Vec::with_capacity(tab.vals.len() / 2);
    let mut u_stg: Vec<Vec<u64>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
    let mut uslow: Vec<(u64, SmallBytes)> = Vec::new();
    for &v in tab.vals.iter() {
        if v == EMPTY_LOC {
            continue;
        }
        let sb = &bufs[(v >> 24) as usize];
        let rec = &sb.recs[(v & 0xFF_FFFF) as usize];
        if rec.flags & FLAG_INVALID != 0 {
            continue;
        }
        winners.push(v);
        let uk = rec.user_key;
        if rec.user_loc == u32::MAX && uk != 0 {
            u_stg[(uk >> 56) as usize].push(uk);
        } else {
            let bytes =
                key_bytes(uk, b'u', &sb.side, rec.user_loc, &mut tmp_a);
            uslow.push((uk, SmallBytes::from_slice(bytes)));
        }
    }
    (winners, u_stg, uslow)
}

// Phase E: emit winners as 16B (person_idx | ev) records into person
// partitions (partition = person_idx >> 24 = canon shard).
fn emit_shard(
    winners: &[u32],
    bufs: &[ShardBuf],
    umaps: &[UMap],
    slow_umaps: &[Vec<SlowUser>],
    parts: &mut Vec<Vec<Rec12>>,
    emit_nanos: &std::sync::atomic::AtomicU64,
) {
    let t_emit = std::time::Instant::now();
    let mut tmp_a = [0u8; 9];
    for &v in winners {
        let sb = &bufs[(v >> 24) as usize];
        let rec = &sb.recs[(v & 0xFF_FFFF) as usize];
        let uk = rec.user_key;
        let person_idx = if rec.user_loc == u32::MAX && uk != 0 {
            umaps[(uk >> 56) as usize].get(uk)
        } else {
            let bytes = key_bytes(uk, b'u', &sb.side, rec.user_loc, &mut tmp_a);
            slow_umaps[(uk >> 56) as usize]
                .iter()
                .find(|s| s.key == uk && s.bytes.as_slice() == bytes)
                .map(|s| s.person_idx)
        };
        let Some(person_idx) = person_idx else {
            continue;
        };
        parts[(person_idx >> 24) as usize].push(Rec12 {
            ev: rec.ev,
            person_idx,
            _pad: 0,
        });
    }
    emit_nanos.fetch_add(
        t_emit.elapsed().as_nanos() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
}

// Open-address u64 -> person_idx map per user shard. user_key==0 means empty
// (fast-path user keys are always nonzero; slow users live in slow_umaps).
struct UMap {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
}

impl UMap {
    fn with_capacity(cap: usize) -> Self {
        let c = (cap * 2).next_power_of_two().max(64);
        Self {
            keys: vec![0u64; c],
            vals: vec![0u32; c],
            mask: c - 1,
        }
    }

    #[inline]
    fn insert(&mut self, key: u64, val: u32) {
        let mut i = (mix64(key) as usize) & self.mask;
        while self.keys[i] != 0 && self.keys[i] != key {
            i = (i + 1) & self.mask;
        }
        self.keys[i] = key;
        self.vals[i] = val;
    }

    #[inline]
    fn get(&self, key: u64) -> Option<u32> {
        let mut i = (mix64(key) as usize) & self.mask;
        loop {
            let k = self.keys[i];
            if k == 0 {
                return None;
            }
            if k == key {
                return Some(self.vals[i]);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// Canonical-person registry for one canon shard. person_idx = (shard<<24)|local
// where local indexes canon_bytes. kind 0 = key-encoded (u64 equality is exact),
// kind 1 = side bytes (hash64 key + byte-compare).
struct CanonShard {
    keys: Vec<u64>,
    kinds: Vec<u8>,
    local: Vec<u32>,
    canon_bytes: Vec<SmallBytes>,
    mask: usize,
}

impl CanonShard {
    fn new(cap: usize) -> Self {
        let c = (cap * 2).next_power_of_two().max(64);
        Self {
            keys: vec![0u64; c],
            kinds: vec![0u8; c],
            local: vec![0u32; c],
            canon_bytes: Vec::with_capacity(cap),
            mask: c - 1,
        }
    }

    #[inline]
    fn intern(&mut self, canon_key: u64, canon: &[u8], slow: bool) -> u32 {
        let mut i = (mix64(canon_key) as usize) & self.mask;
        loop {
            if self.keys[i] == 0 {
                let idx = self.canon_bytes.len() as u32;
                self.keys[i] = canon_key;
                self.kinds[i] = slow as u8;
                self.local[i] = idx;
                self.canon_bytes.push(SmallBytes::from_slice(canon));
                return idx;
            }
            if self.keys[i] == canon_key
                && (self.kinds[i] == 0
                    || self.canon_bytes[self.local[i] as usize].as_slice() == canon)
            {
                return self.local[i];
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline]
    fn get(&self, canon_key: u64, canon: &[u8], slow: bool) -> Option<u32> {
        let mut i = (mix64(canon_key) as usize) & self.mask;
        loop {
            if self.keys[i] == 0 {
                return None;
            }
            if self.keys[i] == canon_key
                && (!slow
                    || (self.kinds[i] == 1
                        && self.canon_bytes[self.local[i] as usize].as_slice()
                            == canon))
            {
                return Some(self.local[i]);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// Slow-path user entry: (hash64 key, raw user bytes, person_idx).
struct SlowUser {
    key: u64,
    bytes: SmallBytes,
    person_idx: u32,
}

// Pending canon fill: canon_bytes empty = key-encoded canon, user_bytes
// empty = key-encoded user (goes to UMap, else to slow_umaps).
struct PendSu {
    canon_key: u64,
    user_key: u64,
    canon_bytes: SmallBytes,
    user_bytes: SmallBytes,
}

fn process_partition(
    surv_slices: &[&[Rec12]],
    click_slices: &[&[ResClick]],
    canon: &[SmallBytes],
    catalog: &Catalog,
    ws: i64,
    day0: i32,
    num_days: usize,
) -> (Vec<UserDailyRow>, Vec<(u32, CampDayAgg)>) {
    let n = canon.len();
    let mut ev_by_p: Vec<Vec<u64>> = (0..n).map(|_| Vec::new()).collect();
    for slice in surv_slices {
        for r in slice.iter() {
            ev_by_p[(r.person_idx & 0xFF_FFFF) as usize].push(r.ev);
        }
    }
    let mut ck_by_p: Vec<Vec<ClickItem>> = (0..n).map(|_| Vec::new()).collect();
    for slice in click_slices {
        for c in slice.iter() {
            ck_by_p[(c.person_idx & 0xFF_FFFF) as usize].push(ClickItem {
                ts: c.ts,
                click_id: c.click_id.clone(),
                camp_id: c.camp_id,
            });
        }
    }

    let num_cats = catalog.categories.len();
    let num_camps_with_org = catalog.campaign_ids.len();
    let mut camp_grid = vec![CampDayAgg::default(); num_camps_with_org * num_days];
    let mut user_rows: Vec<UserDailyRow> = Vec::with_capacity(n * 2);
    let mut camp_counts = vec![0u32; num_camps_with_org];
    let mut in_active = vec![false; num_camps_with_org];
    let mut active_camps: Vec<u16> = Vec::with_capacity(64);
    let mut day_cat_rev = vec![0u64; num_cats];

    for ((i, mut evs), mut clicks) in ev_by_p
        .into_iter()
        .enumerate()
        .zip(ck_by_p.into_iter())
    {
        let person = canon[i].clone();
        if evs.is_empty() {
            continue;
        }
        evs.sort_unstable();

        if !clicks.is_empty() {
            clicks.sort_unstable_by(|a, b| {
                a.ts.cmp(&b.ts)
                    .then_with(|| Reverse(&a.click_id).cmp(&Reverse(&b.click_id)))
            });
            clicks.dedup_by(|a, b| a.ts == b.ts && a.click_id == b.click_id);
        }

        let mut left = 0usize;
        let mut right = 0usize;
        let mut cur_day: i32 = i32::MIN;
        let mut day_sessions: i64 = 0;
        let mut day_events: i64 = 0;
        let mut day_rev: i64 = 0;
        day_cat_rev.fill(0);

        let mut idx = 0usize;
        while idx < evs.len() {
            let sess_start = ev_ts(ws, evs[idx]);
            let mut prev_ts = sess_start;
            let mut sess_events: i64 = 0;
            let mut sess_rev: i64 = 0;
            let mut sess_has_purchase = false;

            let sess_day = sess_start.div_euclid(DAY_MS) as i32;
            if cur_day != i32::MIN && sess_day != cur_day {
                let top_cat = if day_rev > 0 {
                    let mut best_cat = 0u16;
                    let mut best_rev = 0u64;
                    for (cid, &r) in day_cat_rev.iter().enumerate() {
                        if r > best_rev {
                            best_rev = r;
                            best_cat = cid as u16;
                        }
                    }
                    day_cat_rev.fill(0);
                    Some(best_cat)
                } else {
                    None
                };
                user_rows.push(UserDailyRow {
                    user_id: person.clone(),
                    day: cur_day,
                    sessions: day_sessions,
                    events: day_events,
                    revenue_cents: day_rev,
                    top_category: top_cat,
                });
                day_sessions = 0;
                day_events = 0;
                day_rev = 0;
            }
            cur_day = sess_day;

            while idx < evs.len() {
                let e = evs[idx];
                let ts = ev_ts(ws, e);
                if ts - prev_ts > SESSION_GAP_MS {
                    break;
                }
                prev_ts = ts;
                sess_events += 1;
                if ev_purch(e) {
                    sess_has_purchase = true;
                    let p = i64::from(ev_price(e));
                    sess_rev += p;
                    day_cat_rev[ev_cat(e) as usize] += u64::from(ev_price(e));
                }
                idx += 1;
            }

            day_sessions += 1;
            day_events += sess_events;
            day_rev += sess_rev;

            while right < clicks.len() && clicks[right].ts <= sess_start {
                let cid = clicks[right].camp_id;
                if !in_active[cid as usize] {
                    in_active[cid as usize] = true;
                    active_camps.push(cid);
                }
                camp_counts[cid as usize] += 1;
                right += 1;
            }
            let min_click_ts = sess_start - CLICK_REACH_MS;
            while left < right && clicks[left].ts < min_click_ts {
                let cid = clicks[left].camp_id;
                camp_counts[cid as usize] -= 1;
                left += 1;
            }

            let day_i = (sess_day - day0) as usize;
            if left == right {
                let agg =
                    &mut camp_grid[catalog.organic_camp_id as usize * num_days + day_i];
                agg.sessions += 1;
                if sess_has_purchase {
                    agg.purchasing_sessions += 1;
                    agg.revenue_cents += sess_rev;
                }
            } else {
                let win_camp = clicks[right - 1].camp_id;
                {
                    let agg = &mut camp_grid[win_camp as usize * num_days + day_i];
                    agg.sessions += 1;
                    if sess_has_purchase {
                        agg.purchasing_sessions += 1;
                        agg.revenue_cents += sess_rev;
                    }
                }
                let mut k = 0usize;
                while k < active_camps.len() {
                    let cid = active_camps[k];
                    if camp_counts[cid as usize] == 0 {
                        in_active[cid as usize] = false;
                        active_camps.swap_remove(k);
                    } else {
                        camp_grid[cid as usize * num_days + day_i].assisted_sessions += 1;
                        k += 1;
                    }
                }
            }
        }

        if cur_day != i32::MIN {
            let top_cat = if day_rev > 0 {
                let mut best_cat = 0u16;
                let mut best_rev = 0u64;
                for (cid, &r) in day_cat_rev.iter().enumerate() {
                    if r > best_rev {
                        best_rev = r;
                        best_cat = cid as u16;
                    }
                }
                day_cat_rev.fill(0);
                Some(best_cat)
            } else {
                None
            };
            user_rows.push(UserDailyRow {
                user_id: person,
                day: cur_day,
                sessions: day_sessions,
                events: day_events,
                revenue_cents: day_rev,
                top_category: top_cat,
            });
        }

        for &cid in &active_camps {
            camp_counts[cid as usize] = 0;
            in_active[cid as usize] = false;
        }
        active_camps.clear();
    }

    let mut sparse: Vec<(u32, CampDayAgg)> = Vec::new();
    for (i, agg) in camp_grid.iter().enumerate() {
        if agg.sessions != 0 || agg.assisted_sessions != 0 {
            sparse.push((i as u32, *agg));
        }
    }
    (user_rows, sparse)
}

type PartResult = (Vec<UserDailyRow>, Vec<(u32, CampDayAgg)>);

pub fn run_pipeline(
    data_dir: &Path,
    out_dir: &Path,
    scratch_dir: &Path,
    debug_bench: bool,
) -> io::Result<()> {
    let t0 = std::time::Instant::now();
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
    let day0 = ws.div_euclid(DAY_MS) as i32;
    let num_days = ((catalog.window_end - 1).div_euclid(DAY_MS) as i32 - day0) as usize + 1;
    let t_cat = t0.elapsed();

    let t1 = std::time::Instant::now();
    let event_paths = csv_paths(&data_dir.join("events"));
    let total_event_bytes: u64 = event_paths
        .iter()
        .filter_map(|p| p.metadata().ok().map(|m| m.len()))
        .sum();
    let spill = should_spill_events(total_event_bytes);
    let _est_events = ((total_event_bytes / 68) as usize).max(16);

    let catalog_ref = &catalog;

    // DSU build (links ingest + union) overlaps the events shard ingest:
    // canonicalization is only needed at dedup emission, not during parse.
    let (dsu, shard_inputs, t_ingest, t_dsu_wall) = std::thread::scope(|scope| -> io::Result<_> {
        let dsu_handle = scope.spawn(|| {
            let tt = std::time::Instant::now();
            (crate::dsu::load_links(data_dir, threads), tt)
        });
        let inputs: Vec<Vec<ShardBuf>> = if !spill {
        let block_sz = if total_event_bytes >= (64 << 20) {
            4 << 20
        } else {
            1 << 20
        };
        let recs_per_shard = (total_event_bytes as usize / 72 / NUM_SHARDS) + 256;
        let worker_shards = process_csv_files_parallel(
            &event_paths,
            block_sz,
            threads,
            move |_| {
                (0..NUM_SHARDS)
                    .map(|_| ShardBuf {
                        recs: Vec::with_capacity(recs_per_shard),
                        side: Vec::new(),
                    })
                    .collect::<Vec<_>>()
            },
            |shards, line| {
                let fields = split_fields_csv::<7>(line);
                let Some(fields) = fields else {
                    return;
                };
                let event_id = fields[0];
                if event_id.is_empty() {
                    return;
                }
                let Some(ingest_ms) = parse_f1_fast(fields[3]) else {
                    return;
                };
                let (key, slow_id) = field_key(event_id, b'e');
                let sb = &mut shards[(mix64(key) >> 56) as usize];
                let id_loc = if slow_id {
                    sb.put_side(event_id)
                } else {
                    u32::MAX
                };
                let (user_key, user_loc, ev, flags) =
                    match validate_event_copy(fields, ingest_ms, catalog_ref) {
                        Some((user, ev)) => {
                            let (uk, slow_u) = field_key(user, b'u');
                            let ul = if slow_u {
                                sb.put_side(user)
                            } else {
                                u32::MAX
                            };
                            (uk, ul, ev, 0)
                        }
                        None => (0, u32::MAX, 0, FLAG_INVALID),
                    };
                sb.recs.push(EvRecord {
                    key,
                    ingest_ms,
                    user_key,
                    ev,
                    id_loc,
                    user_loc,
                    flags,
                    _pad: [0; 7],
                });
            },
        )?;
        let mut flat: Vec<Vec<ShardBuf>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
        for mut ws in worker_shards {
            for (s, buf) in ws.drain(..).enumerate() {
                if !buf.recs.is_empty() {
                    flat[s].push(buf);
                }
            }
        }
        flat
    } else {
        let spill_dir = scratch_dir.join("helex_ev_shards");
        let _ = fs::remove_dir_all(&spill_dir);
        fs::create_dir_all(&spill_dir)?;
        let writers: Vec<Mutex<BufWriter<File>>> = (0..NUM_SHARDS)
            .map(|s| {
                File::create(spill_dir.join(format!("s{s:03}.bin")))
                    .map(|f| Mutex::new(BufWriter::with_capacity(64 * 1024, f)))
            })
            .collect::<io::Result<_>>()?;
        let writers_ref = &writers;
        let block_sz = if total_event_bytes >= (64 << 20) {
            4 << 20
        } else {
            1 << 20
        };
        let _ = process_csv_files_parallel(
            &event_paths,
            block_sz,
            threads,
            |_| ShardBuf::new(),
            |tmp, line| {
                let fields = split_fields_csv::<7>(line);
                let Some(fields) = fields else {
                    return;
                };
                let event_id = fields[0];
                if event_id.is_empty() {
                    return;
                }
                let Some(ingest_ms) = parse_f1_fast(fields[3]) else {
                    return;
                };
                let (key, slow_id) = field_key(event_id, b'e');
                let shard = (mix64(key) >> 56) as usize;
                let id_loc = if slow_id {
                    tmp.put_side(event_id)
                } else {
                    u32::MAX
                };
                let (user_key, user_loc, ev, flags) =
                    match validate_event_copy(fields, ingest_ms, catalog_ref) {
                        Some((user, ev)) => {
                            let (uk, slow_u) = field_key(user, b'u');
                            let ul = if slow_u {
                                tmp.put_side(user)
                            } else {
                                u32::MAX
                            };
                            (uk, ul, ev, 0)
                        }
                        None => (0, u32::MAX, 0, FLAG_INVALID),
                    };
                let rec = EvRecord {
                    key,
                    ingest_ms,
                    user_key,
                    ev,
                    id_loc,
                    user_loc,
                    flags,
                    _pad: [0; 7],
                };
                let rec_bytes: &[u8] = unsafe {
                    std::slice::from_raw_parts(
                        &rec as *const EvRecord as *const u8,
                        std::mem::size_of::<EvRecord>(),
                    )
                };
                let mut w = writers_ref[shard].lock().unwrap();
                w.write_all(rec_bytes).expect("spill write failed");
                w.write_all(&(tmp.side.len() as u32).to_le_bytes())
                    .expect("spill write failed");
                w.write_all(&tmp.side).expect("spill write failed");
                tmp.side.clear();
            },
        )?;
        for w in writers {
            w.into_inner().unwrap().flush()?;
        }
        let flat: Vec<Vec<ShardBuf>> = (0..NUM_SHARDS)
            .map(|s| {
                let p = spill_dir.join(format!("s{s:03}.bin"));
                fs::read(&p).unwrap_or_default()
            })
            .map(|b| {
                if b.is_empty() {
                    Vec::new()
                } else {
                    let mut sb = ShardBuf::new();
                    let mut pos = 0usize;
                    while pos + 52 <= b.len() {
                        let mut rec: EvRecord = unsafe {
                            std::ptr::read_unaligned(b[pos..].as_ptr() as *const EvRecord)
                        };
                        pos += 48;
                        let side_len =
                            u32::from_le_bytes(b[pos..pos + 4].try_into().unwrap())
                                as usize;
                        pos += 4;
                        let base = sb.side.len() as u32;
                        sb.side.extend_from_slice(&b[pos..pos + side_len]);
                        pos += side_len;
                        if rec.id_loc != u32::MAX {
                            rec.id_loc += base;
                        }
                        if rec.user_loc != u32::MAX {
                            rec.user_loc += base;
                        }
                        sb.recs.push(rec);
                    }
                    vec![sb]
                }
            })
            .collect();
        let _ = fs::remove_dir_all(&spill_dir);
        flat
        };
        let t_ingest = t1.elapsed();
        let (dsu_res, t_dsu_start) = dsu_handle.join().unwrap();
        Ok((dsu_res?, inputs, t_ingest, t_dsu_start.elapsed()))
    })?;
    let dsu_ref = &dsu;
    let t2 = t1;
    let ins_nanos = std::sync::atomic::AtomicU64::new(0);
    let emit_nanos = std::sync::atomic::AtomicU64::new(0);
    let mut t_clicks_wall = std::time::Duration::ZERO;

    // Dedup (CPU/RAM-bound) and click CSV ingest (I/O+parse-bound) overlap:
    // dedup runs on a scoped worker while clicks stream through the pool.
    let shard_inputs_ref = &shard_inputs;
    let ins_ref = &ins_nanos;
    let emit_ref = &emit_nanos;
    let mut click_parts_res: Option<io::Result<Vec<Vec<Vec<ClickCand>>>>> = None;
    let click_min_ts = catalog.window_start - CLICK_REACH_MS;
    type DedupOut = (
        Vec<Vec<Vec<Rec12>>>,
        Vec<CanonShard>,
        Vec<UMap>,
        Vec<Vec<SlowUser>>,
        std::time::Duration,
        std::time::Duration,
    );
    let (survivors, canon_shards, umaps, slow_umaps, t_dedup_wall, t_map_wall): DedupOut =
        std::thread::scope(|scope| {
        let dedup_handle = scope.spawn(|| {
            let td = std::time::Instant::now();
            // Phase A: per-shard dedup insert + winner/user-key collect.
            type AOut = (Vec<u32>, Vec<Vec<u64>>, Vec<(u64, SmallBytes)>);
            let a_slots: Vec<Mutex<Option<AOut>>> = (0..NUM_SHARDS)
                .map(|_| Mutex::new(None))
                .collect();
            {
                let a_ref = &a_slots;
                parallel_for(NUM_SHARDS, threads, |s| {
                    let bufs = &shard_inputs_ref[s];
                    if bufs.is_empty() {
                        return;
                    }
                    thread_local! {
                        static DEDUP_TAB: std::cell::RefCell<DedupTab> =
                            std::cell::RefCell::new(DedupTab::new());
                    }
                    let out = DEDUP_TAB.with(|t| {
                        dedup_insert_collect(bufs, &mut t.borrow_mut(), ins_ref)
                    });
                    *a_ref[s].lock().unwrap() = Some(out);
                });
            }
            let t_a = td.elapsed();

            let mut winners: Vec<Vec<u32>> = Vec::with_capacity(NUM_SHARDS);
            let mut u_lists: Vec<Vec<u64>> =
                (0..NUM_SHARDS).map(|_| Vec::new()).collect();
            let mut slow_lists: Vec<Vec<(u64, SmallBytes)>> =
                (0..NUM_SHARDS).map(|_| Vec::new()).collect();
            for m in a_slots {
                let Some((w, u_stg, uslow)) = m.into_inner().unwrap() else {
                    winners.push(Vec::new());
                    continue;
                };
                winners.push(w);
                for (u, v) in u_stg.into_iter().enumerate() {
                    if !v.is_empty() {
                        u_lists[u].extend_from_slice(&v);
                    }
                }
                for (k, b) in uslow {
                    slow_lists[(k >> 56) as usize].push((k, b));
                }
            }

            // Phase B: unique user_keys per user-shard -> canonical person ->
            // pend staging keyed by canon shard.
            let pend_fast: Vec<Mutex<Vec<(u64, u64)>>> = (0..NUM_SHARDS)
                .map(|_| Mutex::new(Vec::new()))
                .collect();
            let pend_slow: Vec<Mutex<Vec<PendSu>>> = (0..NUM_SHARDS)
                .map(|_| Mutex::new(Vec::new()))
                .collect();
            {
                let pf = &pend_fast;
                let ps = &pend_slow;
                let uref = &u_lists;
                let sref = &slow_lists;
                parallel_for(NUM_SHARDS, threads, |u| {
                    let mut keys = uref[u].clone();
                    keys.sort_unstable();
                    keys.dedup();
                    let mut tmp = [0u8; 9];
                    let mut push_pend = |user_key: u64, ubytes: SmallBytes| {
                        let bytes = if ubytes.as_slice().is_empty() {
                            key_bytes(user_key, b'u', &[], u32::MAX, &mut tmp)
                        } else {
                            ubytes.as_slice()
                        };
                        let canon = dsu_ref.resolve(bytes);
                        let (ck, cslow) = field_key(canon, b'u');
                        let c = (ck >> 56) as usize;
                        if !cslow && ck != 0 && ubytes.as_slice().is_empty() {
                            pf[c].lock().unwrap().push((ck, user_key));
                        } else {
                            ps[c].lock().unwrap().push(PendSu {
                                canon_key: ck,
                                user_key,
                                canon_bytes: if cslow || ck == 0 {
                                    SmallBytes::from_slice(canon)
                                } else {
                                    SmallBytes::from_slice(&[])
                                },
                                user_bytes: ubytes,
                            });
                        }
                    };
                    for key in keys {
                        push_pend(key, SmallBytes::from_slice(&[]));
                    }
                    for (k, b) in sref[u].iter() {
                        push_pend(*k, b.clone());
                    }
                });
            }

            // Phase C: per canon shard, intern canonical persons -> user fills.
            struct COut {
                shard: CanonShard,
                ufill: Vec<Vec<(u64, u32)>>,
                sfill: Vec<Vec<(u64, SmallBytes, u32)>>,
            }
            let c_slots: Vec<Mutex<Option<COut>>> = (0..NUM_SHARDS)
                .map(|_| Mutex::new(None))
                .collect();
            {
                let c_ref = &c_slots;
                let pf = &pend_fast;
                let ps = &pend_slow;
                parallel_for(NUM_SHARDS, threads, |c| {
                    let mut fast = std::mem::take(&mut *pf[c].lock().unwrap());
                    fast.sort_unstable();
                    let mut slow = std::mem::take(&mut *ps[c].lock().unwrap());
                    slow.sort_unstable_by(|a, b| a.canon_key.cmp(&b.canon_key));
                    let mut shard = CanonShard::new((fast.len() + slow.len()) / 2 + 8);
                    let mut ufill: Vec<Vec<(u64, u32)>> =
                        (0..NUM_SHARDS).map(|_| Vec::new()).collect();
                    let mut sfill: Vec<Vec<(u64, SmallBytes, u32)>> =
                        (0..NUM_SHARDS).map(|_| Vec::new()).collect();
                    let mut tmp = [0u8; 9];
                    let mut i = 0usize;
                    while i < fast.len() {
                        let ck = fast[i].0;
                        let cb =
                            key_bytes(ck, b'u', &[], u32::MAX, &mut tmp).to_vec();
                        let local = shard.intern(ck, &cb, false);
                        let pidx = ((c as u32) << 24) | local;
                        while i < fast.len() && fast[i].0 == ck {
                            ufill[(fast[i].1 >> 56) as usize].push((fast[i].1, pidx));
                            i += 1;
                        }
                    }
                    let mut j = 0usize;
                    while j < slow.len() {
                        let ck = slow[j].canon_key;
                        let cslow = !slow[j].canon_bytes.as_slice().is_empty();
                        let cb = if cslow {
                            slow[j].canon_bytes.clone()
                        } else {
                            SmallBytes::from_slice(key_bytes(
                                ck,
                                b'u',
                                &[],
                                u32::MAX,
                                &mut tmp,
                            ))
                        };
                        let local = shard.intern(ck, cb.as_slice(), cslow);
                        let pidx = ((c as u32) << 24) | local;
                        while j < slow.len()
                            && slow[j].canon_key == ck
                            && (slow[j].canon_bytes.as_slice().is_empty()
                                || slow[j].canon_bytes.as_slice() == cb.as_slice())
                        {
                            let e = &slow[j];
                            let u = (e.user_key >> 56) as usize;
                            if e.user_bytes.as_slice().is_empty() {
                                ufill[u].push((e.user_key, pidx));
                            } else {
                                sfill[u].push((
                                    e.user_key,
                                    e.user_bytes.clone(),
                                    pidx,
                                ));
                            }
                            j += 1;
                        }
                    }
                    *c_ref[c].lock().unwrap() = Some(COut {
                        shard,
                        ufill,
                        sfill,
                    });
                });
            }

            // Phase D: merge fills, build umaps (parallel) + slow_umaps.
            let mut canon_shards: Vec<CanonShard> = Vec::with_capacity(NUM_SHARDS);
            let mut ufill: Vec<Vec<(u64, u32)>> =
                (0..NUM_SHARDS).map(|_| Vec::new()).collect();
            let mut sfill: Vec<Vec<(u64, SmallBytes, u32)>> =
                (0..NUM_SHARDS).map(|_| Vec::new()).collect();
            for m in c_slots {
                let out = m.into_inner().unwrap().unwrap();
                canon_shards.push(out.shard);
                for (u, v) in out.ufill.into_iter().enumerate() {
                    if !v.is_empty() {
                        ufill[u].extend_from_slice(&v);
                    }
                }
                for (u, v) in out.sfill.into_iter().enumerate() {
                    if !v.is_empty() {
                        sfill[u].extend(v);
                    }
                }
            }
            let umap_slots: Vec<Mutex<Option<UMap>>> = (0..NUM_SHARDS)
                .map(|_| Mutex::new(None))
                .collect();
            {
                let uref = &ufill;
                let us_ref = &umap_slots;
                parallel_for(NUM_SHARDS, threads, |u| {
                    let mut m = UMap::with_capacity(uref[u].len() + 8);
                    for &(k, v) in uref[u].iter() {
                        m.insert(k, v);
                    }
                    *us_ref[u].lock().unwrap() = Some(m);
                });
            }
            let umaps: Vec<UMap> = umap_slots
                .into_iter()
                .map(|m| m.into_inner().unwrap().unwrap())
                .collect();
            let slow_umaps: Vec<Vec<SlowUser>> = sfill
                .into_iter()
                .map(|fills| {
                    fills
                        .into_iter()
                        .map(|(k, b, p)| SlowUser {
                            key: k,
                            bytes: b,
                            person_idx: p,
                        })
                        .collect()
                })
                .collect();
            let t_map = td.elapsed() - t_a;

            // Phase E: emit winners to person partitions.
            let p_slots: Vec<Mutex<Vec<Vec<Rec12>>>> = (0..NUM_SHARDS)
                .map(|_| Mutex::new((0..NUM_SHARDS).map(|_| Vec::new()).collect()))
                .collect();
            {
                let p_ref = &p_slots;
                let w_ref = &winners;
                let um_ref = &umaps;
                let su_ref = &slow_umaps;
                parallel_for(NUM_SHARDS, threads, |s| {
                    if w_ref[s].is_empty() {
                        return;
                    }
                    let mut parts: Vec<Vec<Rec12>> =
                        (0..NUM_SHARDS).map(|_| Vec::new()).collect();
                    emit_shard(
                        &w_ref[s],
                        &shard_inputs_ref[s][..],
                        um_ref,
                        su_ref,
                        &mut parts,
                        emit_ref,
                    );
                    *p_ref[s].lock().unwrap() = parts;
                });
            }
            let survivors: Vec<Vec<Vec<Rec12>>> = p_slots
                .into_iter()
                .map(|m| m.into_inner().unwrap())
                .collect();
            (
                survivors,
                canon_shards,
                umaps,
                slow_umaps,
                td.elapsed(),
                t_map,
            )
        });
        let tc = std::time::Instant::now();
        click_parts_res = Some((|| -> io::Result<Vec<Vec<Vec<ClickCand>>>> {
        let click_paths = csv_paths(&data_dir.join("clicks"));
        let total_click_bytes: u64 = click_paths
            .iter()
            .filter_map(|p| p.metadata().ok().map(|m| m.len()))
            .sum();
        let block_sz = if total_click_bytes >= (64 << 20) {
            4 << 20
        } else {
            1 << 20
        };
        let window_end = catalog.window_end;
        let worker_parts = process_csv_files_parallel(
            &click_paths,
            block_sz,
            threads,
            |_| {
                (0..NUM_SHARDS).map(|_| Vec::<ClickCand>::new()).collect::<Vec<_>>()
            },
            |parts, line| {
                let fields = split_fields_csv::<4>(line);
                let Some(fields) = fields else {
                    return;
                };
                let click_id = fields[0];
                let user_raw = fields[1];
                if click_id.is_empty() || user_raw.is_empty() {
                    return;
                }
                let Some(camp_id) = catalog_ref.campaign_of(fields[2]) else {
                    return;
                };
                let Some(ts) = parse_timestamp(fields[3]) else {
                    return;
                };
                if ts < click_min_ts || ts >= window_end {
                    return;
                }
                let (uk, _slow_u) = field_key(user_raw, b'u');
                parts[(uk >> 56) as usize].push(ClickCand {
                    user: SmallBytes::from_slice(user_raw),
                    ts,
                    click_id: SmallBytes::from_slice(click_id),
                    camp_id,
                });
            },
        )?;
            let mut flat: Vec<Vec<Vec<ClickCand>>> =
                (0..NUM_SHARDS).map(|_| Vec::new()).collect();
            for mut wp in worker_parts {
                for (p, bucket) in wp.drain(..).enumerate() {
                    if !bucket.is_empty() {
                        flat[p].push(bucket);
                    }
                }
            }
            Ok(flat)
        })());
        t_clicks_wall = tc.elapsed();
        dedup_handle.join().unwrap()
    });
    drop(shard_inputs);
    let t_events = t2.elapsed();
    let click_parts = click_parts_res.unwrap()?;

    // Resolve click candidates (partitioned by user shard) to person_idx.
    let t_cr = std::time::Instant::now();
    let res_slots: Vec<Mutex<Vec<Vec<ResClick>>>> = (0..NUM_SHARDS)
        .map(|_| Mutex::new((0..NUM_SHARDS).map(|_| Vec::new()).collect()))
        .collect();
    {
        let cp_ref = &click_parts;
        let um_ref = &umaps;
        let su_ref = &slow_umaps;
        let cs_ref = &canon_shards;
        let rs_ref = &res_slots;
        parallel_for(NUM_SHARDS, threads, |u| {
            let mut res_parts: Vec<Vec<ResClick>> =
                (0..NUM_SHARDS).map(|_| Vec::new()).collect();
            // Click users absent from events still resolve through links:
            // user -> dsu canon -> canon shard lookup.
            let canon_lookup = |user: &[u8]| -> Option<u32> {
                let canon = dsu_ref.resolve(user);
                let (ck, cslow) = field_key(canon, b'u');
                let c = (ck >> 56) as usize;
                cs_ref[c]
                    .get(ck, canon, cslow || ck == 0)
                    .map(|local| ((c as u32) << 24) | local)
            };
            for cand_list in cp_ref[u].iter() {
                for cand in cand_list {
                    let user = cand.user.as_slice();
                    let (uk, uslow) = field_key(user, b'u');
                    let pidx = if !uslow && uk != 0 {
                        um_ref[u].get(uk).or_else(|| canon_lookup(user))
                    } else {
                        su_ref[u]
                            .iter()
                            .find(|s| s.key == uk && s.bytes.as_slice() == user)
                            .map(|s| s.person_idx)
                            .or_else(|| canon_lookup(user))
                    };
                    let Some(person_idx) = pidx else {
                        continue;
                    };
                    res_parts[(person_idx >> 24) as usize].push(ResClick {
                        person_idx,
                        ts: cand.ts,
                        click_id: cand.click_id.clone(),
                        camp_id: cand.camp_id,
                    });
                }
            }
            *rs_ref[u].lock().unwrap() = res_parts;
        });
    }
    let mut click_res: Vec<Vec<Vec<ResClick>>> =
        (0..NUM_SHARDS).map(|_| Vec::new()).collect();
    for m in res_slots {
        for (p, v) in m.into_inner().unwrap().into_iter().enumerate() {
            if !v.is_empty() {
                click_res[p].push(v);
            }
        }
    }
    let t_cres = t_cr.elapsed();

    let t4 = std::time::Instant::now();
    let part_slots: Vec<Mutex<Option<PartResult>>> =
        (0..NUM_SHARDS).map(|_| Mutex::new(None)).collect();
    let survivors_ref = &survivors;
    let click_res_ref = &click_res;
    let canon_ref = &canon_shards;
    let part_slots_ref = &part_slots;
    parallel_for(NUM_SHARDS, threads, |p| {
        let surv_slices: Vec<&[Rec12]> = survivors_ref
            .iter()
            .filter_map(|sp| sp.get(p).map(|v| v.as_slice()))
            .filter(|s| !s.is_empty())
            .collect();
        if surv_slices.is_empty() {
            return;
        }
        let click_slices: Vec<&[ResClick]> = click_res_ref[p]
            .iter()
            .map(|v| v.as_slice())
            .filter(|s| !s.is_empty())
            .collect();
        let res = process_partition(
            &surv_slices,
            &click_slices,
            &canon_ref[p].canon_bytes,
            catalog_ref,
            ws,
            day0,
            num_days,
        );
        *part_slots_ref[p].lock().unwrap() = Some(res);
    });
    let all_part_results: Vec<PartResult> = part_slots
        .into_iter()
        .filter_map(|m| m.into_inner().unwrap())
        .collect();
    let t_sess = t4.elapsed();
    std::mem::forget(survivors);
    std::mem::forget(click_parts);
    std::mem::forget(click_res);

    let t5 = std::time::Instant::now();
    let user_dir = out_dir.join("user_daily");
    fs::create_dir_all(&user_dir)?;

    let num_camps_with_org = catalog.campaign_ids.len();
    let mut camp_grid_global =
        vec![CampDayAgg::default(); num_camps_with_org * num_days];
    let mut total_user_rows = 0usize;
    for (rows, sparse) in &all_part_results {
        total_user_rows += rows.len();
        for &(cell, agg) in sparse {
            let e = &mut camp_grid_global[cell as usize];
            e.sessions += agg.sessions;
            e.purchasing_sessions += agg.purchasing_sessions;
            e.revenue_cents += agg.revenue_cents;
            e.assisted_sessions += agg.assisted_sessions;
        }
    }

    let non_empty: Vec<usize> = all_part_results
        .iter()
        .enumerate()
        .filter(|(_, (rows, _))| !rows.is_empty())
        .map(|(i, _)| i)
        .collect();
    if non_empty.is_empty() {
        write_user_daily_part(
            &user_dir.join("part-0.parquet"),
            &[],
            &catalog.categories,
        )?;
    } else {
        let ngroups = (threads * 2).min(non_empty.len());
        let mut groups: Vec<Vec<usize>> = (0..ngroups).map(|_| Vec::new()).collect();
        for (k, &i) in non_empty.iter().enumerate() {
            groups[k % ngroups].push(i);
        }
        let user_dir_ref = &user_dir;
        let results_ref = &all_part_results;
        let cats_ref = &catalog.categories;
        let groups_ref = &groups;
        parallel_for(groups.len(), threads, |k| {
            let slices: Vec<&[UserDailyRow]> = groups_ref[k]
                .iter()
                .map(|&i| results_ref[i].0.as_slice())
                .collect();
            let path = user_dir_ref.join(format!("part-{k}.parquet"));
            write_user_daily_part(&path, &slices, cats_ref)
                .expect("user_daily write failed");
        });
    }

    let mut camp_rows: Vec<CampaignDailyRow> =
        Vec::with_capacity(num_camps_with_org * num_days / 4);
    for (cell, agg) in camp_grid_global.iter().enumerate() {
        if agg.sessions > 0 || agg.assisted_sessions > 0 {
            let cid = cell / num_days;
            let day = day0 + (cell % num_days) as i32;
            camp_rows.push(CampaignDailyRow {
                campaign_id: catalog.campaign_ids[cid].clone(),
                channel: catalog.campaign_channels[cid].clone(),
                day,
                sessions: agg.sessions,
                purchasing_sessions: agg.purchasing_sessions,
                revenue_cents: agg.revenue_cents,
                assisted_sessions: agg.assisted_sessions,
            });
        }
    }
    write_campaign_daily(out_dir, &camp_rows)?;
    let t_parquet = t5.elapsed();

    let avx2: u8 = {
        #[cfg(target_arch = "x86_64")]
        {
            std::arch::is_x86_feature_detected!("avx2") as u8
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            0
        }
    };
    let _ = debug_bench;
    eprintln!(
        "[bench] threads={} avx2={} spill={} cat={:.3}s dsu={:.3}s ingest={:.3}s dedup={:.3}s ins_cpu={:.3}s map={:.3}s emit_cpu={:.3}s clicks={:.3}s cres={:.3}s events={:.3}s sess={:.3}s parquet={:.3}s total={:.3}s user_rows={} camp_rows={}",
        threads,
        avx2,
        spill,
        t_cat.as_secs_f64(),
        t_dsu_wall.as_secs_f64(),
        t_ingest.as_secs_f64(),
        t_dedup_wall.as_secs_f64(),
        ins_nanos.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e9,
        t_map_wall.as_secs_f64(),
        emit_nanos.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e9,
        t_clicks_wall.as_secs_f64(),
        t_cres.as_secs_f64(),
        t_events.as_secs_f64(),
        t_sess.as_secs_f64(),
        t_parquet.as_secs_f64(),
        t0.elapsed().as_secs_f64(),
        total_user_rows,
        camp_rows.len()
    );
    std::mem::forget(all_part_results);
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
        fs::write(data.join("campaigns.csv"), "campaign_id,channel\nc_1,search\n").unwrap();
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
        assert!(out.join("user_daily/part-0.parquet").is_file());
        assert!(out.join("campaign_daily/part-0.parquet").is_file());
        let _ = fs::remove_dir_all(&dir);
    }
}
