use std::cmp::Reverse;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use hashbrown::HashTable;
use rustc_hash::FxHashMap;

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

#[derive(Clone, Copy)]
struct ValidEvent {
    ts: i64,
    price_cents: u32,
    is_purchase: bool,
    cat_id: u16,
}

struct Survivor {
    person: SmallBytes,
    person_hash: u64,
    ev: ValidEvent,
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

struct ShardDedupSlot {
    ev_hash: u64,
    buf_idx: u32,
    id_off: u32,
    id_len: u32,
    min_ingest_ts: i64,
    valid_pos: u64,
}

#[inline]
fn append_shard_record(
    buf: &mut Vec<u8>,
    ev_hash: u64,
    ingest_ms: i64,
    event_id: &[u8],
    valid: Option<(&[u8], ValidEvent)>,
) {
    buf.extend_from_slice(&ev_hash.to_le_bytes());
    buf.extend_from_slice(&ingest_ms.to_le_bytes());
    buf.extend_from_slice(&(event_id.len() as u32).to_le_bytes());
    buf.extend_from_slice(event_id);
    match valid {
        None => buf.push(0u8),
        Some((person, ev)) => {
            buf.push(1u8);
            buf.extend_from_slice(&(person.len() as u32).to_le_bytes());
            buf.extend_from_slice(person);
            buf.extend_from_slice(&ev.ts.to_le_bytes());
            buf.extend_from_slice(&ev.price_cents.to_le_bytes());
            let packed_cat = (ev.cat_id << 1) | u16::from(ev.is_purchase);
            buf.extend_from_slice(&packed_cat.to_le_bytes());
        }
    }
}

#[inline]
fn validate_event_copy<'a>(
    fields: [&'a [u8]; 7],
    ingest_ms: i64,
    catalog: &Catalog,
    _dsu: &'a UnionFind,
) -> Option<(&'a [u8], ValidEvent)> {
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
    Some((
        user_raw,
        ValidEvent {
            ts,
            price_cents,
            is_purchase,
            cat_id,
        },
    ))
}

const INVALID_POS: u64 = u64::MAX;

#[inline]
fn pack_pos(buf_idx: usize, off: usize) -> u64 {
    ((buf_idx as u64) << 32) | off as u64
}

fn dedup_one_shard(
    bufs: &[Vec<u8>],
    dsu: &UnionFind,
    table: &mut HashTable<usize>,
    slots: &mut Vec<ShardDedupSlot>,
    parts: &mut Vec<Vec<Survivor>>,
) {
    table.clear();
    slots.clear();
    for (buf_idx, shard_buf) in bufs.iter().enumerate() {
        let mut pos = 0usize;
        while pos < shard_buf.len() {
            let ev_hash =
                u64::from_le_bytes(shard_buf[pos..pos + 8].try_into().unwrap());
            let ingest_ms =
                i64::from_le_bytes(shard_buf[pos + 8..pos + 16].try_into().unwrap());
            let id_len =
                u32::from_le_bytes(shard_buf[pos + 16..pos + 20].try_into().unwrap())
                    as usize;
            let id_off = pos + 20;
            let ev_id = &shard_buf[id_off..id_off + id_len];
            pos = id_off + id_len;
            let tag = shard_buf[pos];
            pos += 1;
            let valid_pos = if tag == 0 {
                INVALID_POS
            } else {
                let v_off = pos;
                let p_len = u32::from_le_bytes(shard_buf[pos..pos + 4].try_into().unwrap())
                    as usize;
                pos += 4 + p_len + 8 + 4 + 2;
                pack_pos(buf_idx, v_off)
            };

            if let Some(&slot_idx) = table.find(ev_hash, |&idx| {
                let s = &slots[idx];
                if s.ev_hash != ev_hash || s.id_len as usize != id_len {
                    return false;
                }
                let src = &bufs[s.buf_idx as usize];
                &src[s.id_off as usize..s.id_off as usize + s.id_len as usize] == ev_id
            }) {
                let s = &mut slots[slot_idx];
                if ingest_ms < s.min_ingest_ts {
                    s.min_ingest_ts = ingest_ms;
                    s.valid_pos = valid_pos;
                }
            } else {
                let idx = slots.len();
                slots.push(ShardDedupSlot {
                    ev_hash,
                    buf_idx: buf_idx as u32,
                    id_off: id_off as u32,
                    id_len: id_len as u32,
                    min_ingest_ts: ingest_ms,
                    valid_pos,
                });
                table.insert_unique(ev_hash, idx, |&i| slots[i].ev_hash);
            }
        }
    }

    for s in slots.iter() {
        if s.valid_pos != INVALID_POS {
            let buf_idx = (s.valid_pos >> 32) as usize;
            let mut p = (s.valid_pos & 0xFFFF_FFFF) as usize;
            let shard_buf = &bufs[buf_idx];
            let p_len =
                u32::from_le_bytes(shard_buf[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            let person = dsu.resolve(&shard_buf[p..p + p_len]);
            p += p_len;
            let ts = i64::from_le_bytes(shard_buf[p..p + 8].try_into().unwrap());
            let price_cents =
                u32::from_le_bytes(shard_buf[p + 8..p + 12].try_into().unwrap());
            let packed_cat =
                u16::from_le_bytes(shard_buf[p + 12..p + 14].try_into().unwrap());
            let person_hash = hash64_bytes(person);
            let part = (person_hash >> 56) as usize;
            parts[part].push(Survivor {
                person: SmallBytes::from_slice(person),
                person_hash,
                ev: ValidEvent {
                    ts,
                    price_cents,
                    is_purchase: (packed_cat & 1) != 0,
                    cat_id: packed_cat >> 1,
                },
            });
        }
    }
}

struct PersonTable {
    table: HashTable<u32>,
    keys: Vec<SmallBytes>,
    events: Vec<Vec<ValidEvent>>,
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
    fn push_event(&mut self, person: SmallBytes, person_hash: u64, ev: ValidEvent) {
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
) -> (Vec<UserDailyRow>, FxHashMap<(u16, i32), CampDayAgg>) {
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
    let mut camp_daily_map: FxHashMap<(u16, i32), CampDayAgg> =
        FxHashMap::with_capacity_and_hasher(num_camps_with_org * 4, Default::default());
    let mut user_rows: Vec<UserDailyRow> = Vec::with_capacity(pt.keys.len() * 2);
    let mut camp_counts = vec![0u32; num_camps_with_org];
    let mut in_active = vec![false; num_camps_with_org];
    let mut active_camps: Vec<u16> = Vec::with_capacity(64);
    let mut day_cat_rev = vec![0u64; num_cats];

    for ((person, mut evs), mut clicks) in
        pt.keys.into_iter().zip(pt.events).zip(pt.clicks)
    {
        evs.sort_unstable_by_key(|e| e.ts);

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
            let sess_start = evs[idx].ts;
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
                if e.ts - prev_ts > SESSION_GAP_MS {
                    break;
                }
                prev_ts = e.ts;
                sess_events += 1;
                if e.is_purchase {
                    sess_has_purchase = true;
                    let p = i64::from(e.price_cents);
                    sess_rev += p;
                    day_cat_rev[e.cat_id as usize] += e.price_cents as u64;
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

            if left == right {
                let agg = camp_daily_map
                    .entry((catalog.organic_camp_id, sess_day))
                    .or_default();
                agg.sessions += 1;
                if sess_has_purchase {
                    agg.purchasing_sessions += 1;
                    agg.revenue_cents += sess_rev;
                }
            } else {
                let win_camp = clicks[right - 1].camp_id;
                {
                    let agg = camp_daily_map.entry((win_camp, sess_day)).or_default();
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
                        camp_daily_map
                            .entry((cid, sess_day))
                            .or_default()
                            .assisted_sessions += 1;
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

    (user_rows, camp_daily_map)
}

type PartResult = (Vec<UserDailyRow>, FxHashMap<(u16, i32), CampDayAgg>);

pub fn run_pipeline(
    data_dir: &Path,
    out_dir: &Path,
    scratch_dir: &Path,
    debug_bench: bool,
) -> io::Result<()> {
    let t0 = std::time::Instant::now();
    let threads = worker_count();
    let catalog = Catalog::load(data_dir)?;
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

    let shard_inputs: Vec<Vec<Vec<u8>>> = if !spill {
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
                (0..NUM_SHARDS).map(|_| Vec::<u8>::new()).collect::<Vec<_>>()
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
                let shard = (ev_hash >> 56) as usize;
                let valid = validate_event_copy(fields, ingest_ms, catalog_ref, dsu_ref);
                append_shard_record(&mut shards[shard], ev_hash, ingest_ms, event_id, valid);
            },
        )?;
        let mut flat: Vec<Vec<Vec<u8>>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
        for mut ws in worker_shards {
            for (s, buf) in ws.drain(..).enumerate() {
                if !buf.is_empty() {
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
            |_| Vec::<u8>::with_capacity(96),
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
                let valid = validate_event_copy(fields, ingest_ms, catalog_ref, dsu_ref);
                tmp.clear();
                append_shard_record(tmp, ev_hash, ingest_ms, event_id, valid);
                writers_ref[shard]
                    .lock()
                    .unwrap()
                    .write_all(tmp)
                    .expect("spill write failed");
            },
        )?;
        for w in writers {
            w.into_inner().unwrap().flush()?;
        }
        let flat: Vec<Vec<Vec<u8>>> = (0..NUM_SHARDS)
            .map(|s| {
                let p = spill_dir.join(format!("s{s:03}.bin"));
                fs::read(&p).unwrap_or_default()
            })
            .map(|b| {
                if b.is_empty() {
                    Vec::new()
                } else {
                    vec![b]
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
        let est = (bufs.iter().map(|b| b.len()).sum::<usize>() / 44).max(64);
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
                let part = (person.hash64() >> 56) as usize;
                parts[part].push(ClickCand {
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
        let res = process_partition(&surv_slices, &click_slices, catalog_ref);
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

    let mut camp_daily_map: FxHashMap<(u16, i32), CampDayAgg> =
        FxHashMap::with_capacity_and_hasher(4096, Default::default());
    let mut total_user_rows = 0usize;
    for (rows, map) in &all_part_results {
        total_user_rows += rows.len();
        for (&k, &agg) in map.iter() {
            let e = camp_daily_map.entry(k).or_default();
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

    let mut camp_rows: Vec<CampaignDailyRow> = Vec::with_capacity(camp_daily_map.len());
    for ((cid, day), agg) in camp_daily_map {
        if agg.sessions > 0 || agg.assisted_sessions > 0 {
            camp_rows.push(CampaignDailyRow {
                campaign_id: catalog.campaign_ids[cid as usize].clone(),
                channel: catalog.campaign_channels[cid as usize].clone(),
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
