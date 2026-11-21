use std::cmp::Reverse;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use hashbrown::HashTable;

use crate::catalog::Catalog;
use crate::csv_dialect::{parse_fixed_fields, parse_price_cents, split_fields_unquoted};
use crate::dsu::{csv_paths, UnionFind};
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

struct Survivor {
    person: SmallBytes,
    person_hash: u64,
    ev: u64,
}

struct ClickCand {
    person: SmallBytes,
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

const LONG_TAG: u8 = 0xFF;
const INVALID_TAG: u8 = 0xFE;
const INVALID_LOC: u64 = u64::MAX;

// Fixed 64-byte dedup record: ids/users <=15B inline, longer values spill to
// the per-shard side arena encoded as off:u32|len:u32.
#[repr(C)]
#[derive(Clone, Copy)]
struct EvRecord {
    ev_hash: u64,
    ingest_ms: i64,
    id_len: u8,
    user_len: u8,
    _pad: [u8; 6],
    id: [u8; 16],
    user: [u8; 16],
    ev: u64,
}
const _: () = assert!(std::mem::size_of::<EvRecord>() == 64);

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
    fn put_field(&mut self, raw: &[u8], inline: &mut [u8; 16], len_tag: &mut u8) {
        if raw.len() <= 15 {
            inline[..raw.len()].copy_from_slice(raw);
            *len_tag = raw.len() as u8;
        } else {
            let off = self.side.len() as u32;
            self.side.extend_from_slice(raw);
            inline[..4].copy_from_slice(&off.to_le_bytes());
            inline[4..8].copy_from_slice(&(raw.len() as u32).to_le_bytes());
            *len_tag = LONG_TAG;
        }
    }
}

#[inline]
fn field_bytes<'a>(buf: &'a ShardBuf, raw: &'a [u8; 16], len_tag: u8) -> &'a [u8] {
    if len_tag == LONG_TAG {
        let off = u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(raw[4..8].try_into().unwrap()) as usize;
        &buf.side[off..off + len]
    } else {
        &raw[..len_tag as usize]
    }
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

struct ShardDedupSlot {
    ev_hash: u64,
    min_ingest_ts: i64,
    id_len: u32,
    rec_loc: u64,
    valid_loc: u64,
}

#[inline]
fn rec_loc(buf_idx: usize, rec_idx: usize) -> u64 {
    ((buf_idx as u64) << 32) | rec_idx as u64
}

fn dedup_one_shard(
    bufs: &[ShardBuf],
    dsu: &UnionFind,
    table: &mut HashTable<usize>,
    slots: &mut Vec<ShardDedupSlot>,
    parts: &mut Vec<Vec<Survivor>>,
) {
    table.clear();
    slots.clear();
    for (buf_idx, shard_buf) in bufs.iter().enumerate() {
        for (rec_idx, rec) in shard_buf.recs.iter().enumerate() {
            let ev_id = field_bytes(shard_buf, &rec.id, rec.id_len);
            let id_len = ev_id.len() as u32;

            if let Some(&slot_idx) = table.find(rec.ev_hash, |&idx| {
                let s = &slots[idx];
                if s.ev_hash != rec.ev_hash || s.id_len != id_len {
                    return false;
                }
                let sb = &bufs[(s.rec_loc >> 32) as usize];
                let srec = &sb.recs[(s.rec_loc & 0xFFFF_FFFF) as usize];
                field_bytes(sb, &srec.id, srec.id_len) == ev_id
            }) {
                let s = &mut slots[slot_idx];
                if rec.ingest_ms < s.min_ingest_ts {
                    s.min_ingest_ts = rec.ingest_ms;
                    s.valid_loc = if rec.user_len == INVALID_TAG {
                        INVALID_LOC
                    } else {
                        rec_loc(buf_idx, rec_idx)
                    };
                }
            } else {
                let idx = slots.len();
                slots.push(ShardDedupSlot {
                    ev_hash: rec.ev_hash,
                    min_ingest_ts: rec.ingest_ms,
                    id_len,
                    rec_loc: rec_loc(buf_idx, rec_idx),
                    valid_loc: if rec.user_len == INVALID_TAG {
                        INVALID_LOC
                    } else {
                        rec_loc(buf_idx, rec_idx)
                    },
                });
                table.insert_unique(rec.ev_hash, idx, |&i| slots[i].ev_hash);
            }
        }
    }

    for s in slots.iter() {
        if s.valid_loc != INVALID_LOC {
            let buf = &bufs[(s.valid_loc >> 32) as usize];
            let rec = &buf.recs[(s.valid_loc & 0xFFFF_FFFF) as usize];
            let person = dsu.resolve(field_bytes(buf, &rec.user, rec.user_len));
            let person_hash = hash64_bytes(person);
            let part = (person_hash >> 56) as usize;
            parts[part].push(Survivor {
                person: SmallBytes::from_slice(person),
                person_hash,
                ev: rec.ev,
            });
        }
    }
}

struct PersonTable {
    table: HashTable<u32>,
    keys: Vec<SmallBytes>,
    events: Vec<Vec<u64>>,
    clicks: Vec<Vec<ClickItem>>,
}

impl PersonTable {
    fn new(cap: usize) -> Self {
        Self {
            table: HashTable::with_capacity(cap),
            keys: Vec::with_capacity(cap),
            events: Vec::with_capacity(cap),
            clicks: Vec::with_capacity(cap),
        }
    }

    #[inline]
    fn push_event(&mut self, person: SmallBytes, person_hash: u64, ev: u64) {
        let keys = &self.keys;
        if let Some(&idx) = self
            .table
            .find(person_hash, |&i| keys[i as usize] == person)
        {
            self.events[idx as usize].push(ev);
        } else {
            let idx = self.keys.len() as u32;
            self.keys.push(person);
            self.events.push(vec![ev]);
            self.clicks.push(Vec::new());
            let keys = &self.keys;
            self.table
                .insert_unique(person_hash, idx, |&i| keys[i as usize].hash64());
        }
    }

    #[inline]
    fn person_idx(&self, person: &SmallBytes, person_hash: u64) -> Option<u32> {
        self.table
            .find(person_hash, |&i| self.keys[i as usize] == *person)
            .copied()
    }
}

fn process_partition(
    surv_slices: &[&[Survivor]],
    click_slices: &[&[ClickCand]],
    catalog: &Catalog,
    ws: i64,
    day0: i32,
    num_days: usize,
) -> (Vec<UserDailyRow>, Vec<(u32, CampDayAgg)>) {
    let est: usize = surv_slices.iter().map(|s| s.len()).sum::<usize>() / 24 + 16;
    let mut pt = PersonTable::new(est);
    for slice in surv_slices {
        for s in slice.iter() {
            pt.push_event(s.person.clone(), s.person_hash, s.ev);
        }
    }
    for slice in click_slices {
        for c in slice.iter() {
            let ph = c.person.hash64();
            if let Some(idx) = pt.person_idx(&c.person, ph) {
                pt.clicks[idx as usize].push(ClickItem {
                    ts: c.ts,
                    click_id: c.click_id.clone(),
                    camp_id: c.camp_id,
                });
            }
        }
    }

    let num_cats = catalog.categories.len();
    let num_camps_with_org = catalog.campaign_ids.len();
    let mut camp_grid = vec![CampDayAgg::default(); num_camps_with_org * num_days];
    let mut user_rows: Vec<UserDailyRow> = Vec::with_capacity(pt.keys.len() * 2);
    let mut camp_counts = vec![0u32; num_camps_with_org];
    let mut in_active = vec![false; num_camps_with_org];
    let mut active_camps: Vec<u16> = Vec::with_capacity(64);
    let mut day_cat_rev = vec![0u64; num_cats];

    for ((person, mut evs), mut clicks) in
        pt.keys.into_iter().zip(pt.events).zip(pt.clicks)
    {
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
    let dsu = crate::dsu::load_links(data_dir, threads)?;
    let t_dsu = t1.elapsed();

    let t2 = std::time::Instant::now();
    let event_paths = csv_paths(&data_dir.join("events"));
    let total_event_bytes: u64 = event_paths
        .iter()
        .filter_map(|p| p.metadata().ok().map(|m| m.len()))
        .sum();
    let spill = should_spill_events(total_event_bytes);
    let _est_events = ((total_event_bytes / 68) as usize).max(16);

    let catalog_ref = &catalog;
    let dsu_ref = &dsu;

    let shard_inputs: Vec<Vec<ShardBuf>> = if !spill {
        let block_sz = if total_event_bytes >= (64 << 20) {
            4 << 20
        } else {
            1 << 20
        };
        let worker_shards = process_csv_files_parallel(
            &event_paths,
            block_sz,
            threads,
            |_| {
                (0..NUM_SHARDS)
                    .map(|_| ShardBuf::new())
                    .collect::<Vec<_>>()
            },
            |shards, line, ascii| {
                let fields = if ascii && memchr::memchr(b'"', line).is_none() {
                    split_fields_unquoted::<7>(line)
                } else {
                    parse_fixed_fields::<7>(line)
                };
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
                let ev_hash = hash64_bytes(event_id);
                let sb = &mut shards[(ev_hash >> 56) as usize];
                let mut rec = EvRecord {
                    ev_hash,
                    ingest_ms,
                    id_len: 0,
                    user_len: INVALID_TAG,
                    _pad: [0; 6],
                    id: [0; 16],
                    user: [0; 16],
                    ev: 0,
                };
                sb.put_field(event_id, &mut rec.id, &mut rec.id_len);
                if let Some((user, ev)) = validate_event_copy(fields, ingest_ms, catalog_ref) {
                    sb.put_field(user, &mut rec.user, &mut rec.user_len);
                    rec.ev = ev;
                }
                sb.recs.push(rec);
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
            |tmp, line, ascii| {
                let fields = if ascii && memchr::memchr(b'"', line).is_none() {
                    split_fields_unquoted::<7>(line)
                } else {
                    parse_fixed_fields::<7>(line)
                };
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
                let ev_hash = hash64_bytes(event_id);
                let shard = (ev_hash >> 56) as usize;
                let mut rec = EvRecord {
                    ev_hash,
                    ingest_ms,
                    id_len: 0,
                    user_len: INVALID_TAG,
                    _pad: [0; 6],
                    id: [0; 16],
                    user: [0; 16],
                    ev: 0,
                };
                tmp.put_field(event_id, &mut rec.id, &mut rec.id_len);
                if let Some((user, ev)) = validate_event_copy(fields, ingest_ms, catalog_ref) {
                    tmp.put_field(user, &mut rec.user, &mut rec.user_len);
                    rec.ev = ev;
                }
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
                    while pos + 68 <= b.len() {
                        let mut rec: EvRecord = unsafe {
                            std::ptr::read_unaligned(b[pos..].as_ptr() as *const EvRecord)
                        };
                        pos += 64;
                        let side_len =
                            u32::from_le_bytes(b[pos..pos + 4].try_into().unwrap())
                                as usize;
                        pos += 4;
                        let base = sb.side.len() as u32;
                        sb.side.extend_from_slice(&b[pos..pos + side_len]);
                        pos += side_len;
                        if rec.id_len == LONG_TAG {
                            let off =
                                u32::from_le_bytes(rec.id[..4].try_into().unwrap()) + base;
                            rec.id[..4].copy_from_slice(&off.to_le_bytes());
                        }
                        if rec.user_len == LONG_TAG {
                            let off =
                                u32::from_le_bytes(rec.user[..4].try_into().unwrap())
                                    + base;
                            rec.user[..4].copy_from_slice(&off.to_le_bytes());
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

    let surv_slots: Vec<Mutex<Option<Vec<Vec<Survivor>>>>> = (0..NUM_SHARDS)
        .map(|_| Mutex::new(None))
        .collect();
    let shard_inputs_ref = &shard_inputs;
    let surv_slots_ref = &surv_slots;
    parallel_for(NUM_SHARDS, threads, |s| {
        let bufs = &shard_inputs_ref[s];
        if bufs.is_empty() {
            return;
        }
        let est = bufs.iter().map(|b| b.recs.len()).sum::<usize>().max(64);
        let mut table = HashTable::with_capacity(est * 2);
        let mut slots: Vec<ShardDedupSlot> = Vec::with_capacity(est);
        let mut parts: Vec<Vec<Survivor>> =
            (0..NUM_SHARDS).map(|_| Vec::new()).collect();
        dedup_one_shard(bufs, dsu_ref, &mut table, &mut slots, &mut parts);
        *surv_slots_ref[s].lock().unwrap() = Some(parts);
    });
    let survivors: Vec<Vec<Vec<Survivor>>> = surv_slots
        .into_iter()
        .map(|m| m.into_inner().unwrap().unwrap_or_default())
        .collect();
    drop(shard_inputs);

    // Bloom filter over canonical persons with >=1 surviving event; rejects
    // clicks whose user has no events before they touch partition vectors.
    const BLOOM_BITS: usize = 1 << 24;
    let mut bloom = vec![0u64; BLOOM_BITS / 64];
    for sp in &survivors {
        for part in sp {
            for s in part {
                let h = s.person_hash;
                let m = BLOOM_BITS - 1;
                let b1 = (h as usize) & m;
                let b2 = ((h >> 32) as usize ^ (h as usize >> 16)) & m;
                let b3 = (h.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as usize & m;
                bloom[b1 >> 6] |= 1u64 << (b1 & 63);
                bloom[b2 >> 6] |= 1u64 << (b2 & 63);
                bloom[b3 >> 6] |= 1u64 << (b3 & 63);
            }
        }
    }
    let t_events = t2.elapsed();

    let t3 = std::time::Instant::now();
    let click_min_ts = catalog.window_start - CLICK_REACH_MS;
    let click_parts: Vec<Vec<Vec<ClickCand>>> = {
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
        let bloom_ref = &bloom;
        let worker_parts = process_csv_files_parallel(
            &click_paths,
            block_sz,
            threads,
            |_| {
                (0..NUM_SHARDS).map(|_| Vec::<ClickCand>::new()).collect::<Vec<_>>()
            },
            |parts, line, ascii| {
                let fields = if ascii && memchr::memchr(b'"', line).is_none() {
                    split_fields_unquoted::<4>(line)
                } else {
                    parse_fixed_fields::<4>(line)
                };
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
                let person = SmallBytes::from_slice(dsu_ref.resolve(user_raw));
                let ph = person.hash64();
                let m = BLOOM_BITS - 1;
                let b1 = (ph as usize) & m;
                let b2 = ((ph >> 32) as usize ^ (ph as usize >> 16)) & m;
                let b3 = (ph.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as usize & m;
                if bloom_ref[b1 >> 6] & (1u64 << (b1 & 63)) == 0
                    || bloom_ref[b2 >> 6] & (1u64 << (b2 & 63)) == 0
                    || bloom_ref[b3 >> 6] & (1u64 << (b3 & 63)) == 0
                {
                    return;
                }
                parts[(ph >> 56) as usize].push(ClickCand {
                    person,
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
        flat
    };
    let t_clicks = t3.elapsed();

    let t4 = std::time::Instant::now();
    let part_slots: Vec<Mutex<Option<PartResult>>> =
        (0..NUM_SHARDS).map(|_| Mutex::new(None)).collect();
    let survivors_ref = &survivors;
    let click_parts_ref = &click_parts;
    let part_slots_ref = &part_slots;
    parallel_for(NUM_SHARDS, threads, |p| {
        let surv_slices: Vec<&[Survivor]> = survivors_ref
            .iter()
            .filter_map(|sp| sp.get(p).map(|v| v.as_slice()))
            .filter(|s| !s.is_empty())
            .collect();
        if surv_slices.is_empty() {
            return;
        }
        let click_slices: Vec<&[ClickCand]> = click_parts_ref[p]
            .iter()
            .map(|v| v.as_slice())
            .filter(|s| !s.is_empty())
            .collect();
        let res = process_partition(
            &surv_slices,
            &click_slices,
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

    if debug_bench {
        eprintln!(
            "[DEBUG_BENCH] threads={} spill={} cat={:.3}s dsu={:.3}s events={:.3}s clicks={:.3}s sess={:.3}s parquet={:.3}s total={:.3}s (user_rows={}, camp_rows={})",
            threads,
            spill,
            t_cat.as_secs_f64(),
            t_dsu.as_secs_f64(),
            t_events.as_secs_f64(),
            t_clicks.as_secs_f64(),
            t_sess.as_secs_f64(),
            t_parquet.as_secs_f64(),
            t0.elapsed().as_secs_f64(),
            total_user_rows,
            camp_rows.len()
        );
    }
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
