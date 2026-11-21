use std::cmp::Reverse;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use hashbrown::HashTable;
use rustc_hash::FxHashMap;

use crate::catalog::Catalog;
use crate::csv_dialect::{iter_data_lines, parse_fixed_fields, parse_price_cents};
use crate::dsu::{csv_paths, UnionFind};
use crate::mem_guard::should_spill_events;
use crate::parquet_out::{
    write_campaign_daily, write_user_daily_part, CampaignDailyRow, UserDailyRow,
};
use crate::sso::{hash64_bytes, SmallBytes};
use crate::timestamp::parse_timestamp;

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

#[derive(Clone)]
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
    id_off: usize,
    id_len: usize,
    min_ingest_ts: i64,
    valid_off: usize,
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
    dsu: &'a UnionFind,
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
    let person = dsu.resolve(user_raw);
    let cat_id = if is_purchase {
        catalog.category_of(product_raw)
    } else {
        0
    };
    Some((
        person,
        ValidEvent {
            ts,
            price_cents,
            is_purchase,
            cat_id,
        },
    ))
}

fn dedup_one_shard(
    shard_buf: &[u8],
    table: &mut HashTable<usize>,
    slots: &mut Vec<ShardDedupSlot>,
    mut emit: impl FnMut(&[u8], ValidEvent),
) {
    table.clear();
    slots.clear();
    let mut pos = 0usize;
    while pos < shard_buf.len() {
        let ev_hash = u64::from_le_bytes(shard_buf[pos..pos + 8].try_into().unwrap());
        let ingest_ms = i64::from_le_bytes(shard_buf[pos + 8..pos + 16].try_into().unwrap());
        let id_len = u32::from_le_bytes(shard_buf[pos + 16..pos + 20].try_into().unwrap()) as usize;
        let id_off = pos + 20;
        let ev_id = &shard_buf[id_off..id_off + id_len];
        pos = id_off + id_len;
        let tag = shard_buf[pos];
        pos += 1;
        let valid_off = if tag == 0 {
            usize::MAX
        } else {
            let v_off = pos;
            let p_len = u32::from_le_bytes(shard_buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4 + p_len + 8 + 4 + 2;
            v_off
        };

        if let Some(&slot_idx) = table.find(ev_hash, |&idx| {
            let s = &slots[idx];
            s.ev_hash == ev_hash && &shard_buf[s.id_off..s.id_off + s.id_len] == ev_id
        }) {
            let s = &mut slots[slot_idx];
            if ingest_ms < s.min_ingest_ts {
                s.min_ingest_ts = ingest_ms;
                s.valid_off = valid_off;
            }
        } else {
            let idx = slots.len();
            slots.push(ShardDedupSlot {
                ev_hash,
                id_off,
                id_len,
                min_ingest_ts: ingest_ms,
                valid_off,
            });
            table.insert_unique(ev_hash, idx, |&i| slots[i].ev_hash);
        }
    }

    for s in slots.iter() {
        if s.valid_off != usize::MAX {
            let mut p = s.valid_off;
            let p_len = u32::from_le_bytes(shard_buf[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            let person = &shard_buf[p..p + p_len];
            p += p_len;
            let ts = i64::from_le_bytes(shard_buf[p..p + 8].try_into().unwrap());
            let price_cents = u32::from_le_bytes(shard_buf[p + 8..p + 12].try_into().unwrap());
            let packed_cat = u16::from_le_bytes(shard_buf[p + 12..p + 14].try_into().unwrap());
            emit(
                person,
                ValidEvent {
                    ts,
                    price_cents,
                    is_purchase: (packed_cat & 1) != 0,
                    cat_id: packed_cat >> 1,
                },
            );
        }
    }
}

struct PersonMap {
    table: HashTable<u32>,
    keys: Vec<SmallBytes>,
    events: Vec<Vec<ValidEvent>>,
    clicks: Vec<Vec<ClickItem>>,
}

impl PersonMap {
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: HashTable::with_capacity(cap),
            keys: Vec::with_capacity(cap),
            events: Vec::with_capacity(cap),
            clicks: Vec::with_capacity(cap),
        }
    }

    #[inline]
    fn push_event(&mut self, person: &[u8], ev: ValidEvent) {
        let h = hash64_bytes(person);
        let keys = &self.keys;
        if let Some(&idx) = self.table.find(h, |&i| keys[i as usize].eq_slice(person)) {
            self.events[idx as usize].push(ev);
        } else {
            let idx = self.keys.len() as u32;
            self.keys.push(SmallBytes::from_slice(person));
            self.events.push(vec![ev]);
            self.clicks.push(Vec::new());
            let keys = &self.keys;
            self.table
                .insert_unique(h, idx, |&i| keys[i as usize].hash64());
        }
    }

    #[inline]
    fn find_active_person(&self, person: &[u8]) -> Option<u32> {
        if self.keys.is_empty() {
            return None;
        }
        let h = hash64_bytes(person);
        self.table
            .find(h, |&i| self.keys[i as usize].eq_slice(person))
            .copied()
    }
}

pub fn run_pipeline(
    data_dir: &Path,
    out_dir: &Path,
    scratch_dir: &Path,
    debug_bench: bool,
) -> io::Result<()> {
    let t0 = std::time::Instant::now();
    let catalog = Catalog::load(data_dir)?;
    let t_cat = t0.elapsed();

    let t1 = std::time::Instant::now();
    let dsu = crate::dsu::load_links(data_dir)?;
    let t_dsu = t1.elapsed();

    let t2 = std::time::Instant::now();
    let event_paths = csv_paths(&data_dir.join("events"));
    let total_event_bytes: u64 = event_paths
        .iter()
        .filter_map(|p| p.metadata().ok().map(|m| m.len()))
        .sum();
    let spill = should_spill_events(total_event_bytes);
    let est_events = ((total_event_bytes / 68) as usize).max(16);
    let est_people = (est_events / 24).max(16);

    let mut people = PersonMap::with_capacity(est_people);
    let mut dedup_table: HashTable<usize> =
        HashTable::with_capacity((est_events / NUM_SHARDS * 2).max(64));
    let mut dedup_slots: Vec<ShardDedupSlot> =
        Vec::with_capacity((est_events / NUM_SHARDS + 64).max(64));

    if !spill {
        let per_shard_bytes = ((total_event_bytes as usize) / NUM_SHARDS / 2).max(256);
        let mut ram_shards: Vec<Vec<u8>> = (0..NUM_SHARDS)
            .map(|_| Vec::with_capacity(per_shard_bytes))
            .collect();

        for path in event_paths {
            let bytes = fs::read(path)?;
            iter_data_lines(&bytes, |line| {
                let Some(fields) = parse_fixed_fields::<7>(line) else {
                    return;
                };
                let event_id = fields[0];
                if event_id.is_empty() {
                    return;
                }
                let Some(ingest_ms) = parse_timestamp(fields[3]) else {
                    return;
                };
                let ev_hash = hash64_bytes(event_id);
                let shard = (ev_hash >> 56) as usize;
                let valid = validate_event_copy(fields, ingest_ms, &catalog, &dsu);
                append_shard_record(&mut ram_shards[shard], ev_hash, ingest_ms, event_id, valid);
            });
        }

        for shard in &mut ram_shards {
            let buf = std::mem::take(shard);
            if !buf.is_empty() {
                dedup_one_shard(&buf, &mut dedup_table, &mut dedup_slots, |person, ev| {
                    people.push_event(person, ev);
                });
            }
        }
    } else {
        let spill_dir = scratch_dir.join("helex_ev_shards");
        let _ = fs::remove_dir_all(&spill_dir);
        fs::create_dir_all(&spill_dir)?;
        let mut writers: Vec<BufWriter<File>> = (0..NUM_SHARDS)
            .map(|s| {
                File::create(spill_dir.join(format!("s{s:03}.bin")))
                    .map(|f| BufWriter::with_capacity(32 * 1024, f))
            })
            .collect::<io::Result<_>>()?;
        let mut tmp_rec = Vec::with_capacity(128);

        for path in event_paths {
            let bytes = fs::read(path)?;
            iter_data_lines(&bytes, |line| {
                let Some(fields) = parse_fixed_fields::<7>(line) else {
                    return;
                };
                let event_id = fields[0];
                if event_id.is_empty() {
                    return;
                }
                let Some(ingest_ms) = parse_timestamp(fields[3]) else {
                    return;
                };
                let ev_hash = hash64_bytes(event_id);
                let shard = (ev_hash >> 56) as usize;
                let valid = validate_event_copy(fields, ingest_ms, &catalog, &dsu);
                tmp_rec.clear();
                append_shard_record(&mut tmp_rec, ev_hash, ingest_ms, event_id, valid);
                writers[shard].write_all(&tmp_rec).expect("spill write failed");
            });
        }
        for mut w in writers {
            w.flush()?;
        }

        for s in 0..NUM_SHARDS {
            let p = spill_dir.join(format!("s{s:03}.bin"));
            let bytes = fs::read(&p)?;
            let _ = fs::remove_file(&p);
            if !bytes.is_empty() {
                dedup_one_shard(&bytes, &mut dedup_table, &mut dedup_slots, |person, ev| {
                    people.push_event(person, ev);
                });
            }
        }
        let _ = fs::remove_dir_all(&spill_dir);
    }
    drop(dedup_table);
    drop(dedup_slots);
    let t_events = t2.elapsed();

    let t3 = std::time::Instant::now();
    let click_min_ts = catalog.window_start - CLICK_REACH_MS;

    if !people.keys.is_empty() {
        for path in csv_paths(&data_dir.join("clicks")) {
            let bytes = fs::read(path)?;
            iter_data_lines(&bytes, |line| {
                let Some(fields) = parse_fixed_fields::<4>(line) else {
                    return;
                };
                let click_id = fields[0];
                let user_raw = fields[1];
                if click_id.is_empty() || user_raw.is_empty() {
                    return;
                }
                let person = dsu.resolve(user_raw);
                let Some(p_idx) = people.find_active_person(person) else {
                    return;
                };
                let Some(camp_id) = catalog.campaign_of(fields[2]) else {
                    return;
                };
                let Some(ts) = parse_timestamp(fields[3]) else {
                    return;
                };
                if ts < click_min_ts || ts >= catalog.window_end {
                    return;
                }
                people.clicks[p_idx as usize].push(ClickItem {
                    ts,
                    click_id: SmallBytes::from_slice(click_id),
                    camp_id,
                });
            });
        }
    }
    let t_clicks = t3.elapsed();

    let t4 = std::time::Instant::now();
    let num_cats = catalog.categories.len();
    let num_camps_with_org = catalog.campaign_ids.len();
    let mut camp_daily_map: FxHashMap<(u16, i32), CampDayAgg> =
        FxHashMap::with_capacity_and_hasher(num_camps_with_org * 16, Default::default());

    let mut user_rows: Vec<UserDailyRow> = Vec::with_capacity(people.keys.len() * 4);
    let mut camp_counts = vec![0u32; num_camps_with_org];
    let mut in_active = vec![false; num_camps_with_org];
    let mut active_camps: Vec<u16> = Vec::with_capacity(64);
    let mut day_cat_rev = vec![0u64; num_cats];

    for ((person, mut evs), mut clicks) in people
        .keys
        .into_iter()
        .zip(people.events)
        .zip(people.clicks)
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
    let t_sess = t4.elapsed();

    let t5 = std::time::Instant::now();
    let user_dir = out_dir.join("user_daily");
    fs::create_dir_all(&user_dir)?;
    write_user_daily_part(
        &user_dir.join("part-0.parquet"),
        &user_rows,
        &catalog.categories,
    )?;

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
            "[DEBUG_BENCH] spill={} cat={:.3}s dsu={:.3}s events={:.3}s clicks={:.3}s sess={:.3}s parquet={:.3}s total={:.3}s (user_rows={}, camp_rows={})",
            spill,
            t_cat.as_secs_f64(),
            t_dsu.as_secs_f64(),
            t_events.as_secs_f64(),
            t_clicks.as_secs_f64(),
            t_sess.as_secs_f64(),
            t_parquet.as_secs_f64(),
            t0.elapsed().as_secs_f64(),
            user_rows.len(),
            camp_rows.len()
        );
    }

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
