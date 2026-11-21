use std::cmp::Reverse;
use std::fs;
use std::io;
use std::path::Path;

use rustc_hash::FxHashMap;

use crate::catalog::Catalog;
use crate::csv_dialect::{iter_data_lines, parse_fixed_fields, parse_price_cents};
use crate::dsu::{csv_paths, UnionFind};
use crate::parquet_out::{
    write_campaign_daily, write_user_daily_part, CampaignDailyRow, UserDailyRow,
};
use crate::timestamp::parse_timestamp;

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

struct EventDedupEntry {
    min_ingest_ts: i64,
    payload: Option<(Box<[u8]>, ValidEvent)>,
}

#[derive(Clone)]
struct ClickItem {
    ts: i64,
    click_id: Box<[u8]>,
    camp_id: u16,
}

#[derive(Default, Clone, Copy)]
struct CampDayAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

#[inline]
fn validate_event_copy<'a>(
    fields: [&'a [u8]; 7],
    ingest_ms: i64,
    catalog: &Catalog,
    dsu: &'a UnionFind,
) -> Option<(Box<[u8]>, ValidEvent)> {
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
    let person: Box<[u8]> = dsu.resolve(user_raw).into();
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

pub fn run_pipeline(
    data_dir: &Path,
    out_dir: &Path,
    _scratch_dir: &Path,
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
    let est_events = ((total_event_bytes / 68) as usize).max(16);
    let mut event_dedup: FxHashMap<Box<[u8]>, EventDedupEntry> =
        FxHashMap::with_capacity_and_hasher(est_events, Default::default());

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
            if let Some(entry) = event_dedup.get_mut(event_id) {
                if ingest_ms < entry.min_ingest_ts {
                    entry.min_ingest_ts = ingest_ms;
                    entry.payload = validate_event_copy(fields, ingest_ms, &catalog, &dsu);
                }
            } else {
                let payload = validate_event_copy(fields, ingest_ms, &catalog, &dsu);
                event_dedup.insert(
                    event_id.into(),
                    EventDedupEntry {
                        min_ingest_ts: ingest_ms,
                        payload,
                    },
                );
            }
        });
    }

    let mut person_events: FxHashMap<Box<[u8]>, Vec<ValidEvent>> =
        FxHashMap::with_capacity_and_hasher(event_dedup.len() / 16 + 64, Default::default());
    for (_, entry) in event_dedup {
        if let Some((person, ev)) = entry.payload {
            person_events.entry(person).or_default().push(ev);
        }
    }
    let t_events = t2.elapsed();

    let t3 = std::time::Instant::now();
    let click_min_ts = catalog.window_start - CLICK_REACH_MS;
    let mut person_clicks: FxHashMap<Box<[u8]>, Vec<ClickItem>> =
        FxHashMap::with_capacity_and_hasher(person_events.len(), Default::default());

    if !person_events.is_empty() {
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
                if !person_events.contains_key(person) {
                    return;
                }
                let Some(camp_id) = catalog.campaign_of(fields[2]) else {
                    return;
                };
                let Some(ts) = parse_timestamp(fields[3]) else {
                    return;
                };
                if ts < click_min_ts || ts >= catalog.window_end {
                    return;
                }
                person_clicks
                    .entry(person.into())
                    .or_default()
                    .push(ClickItem {
                        ts,
                        click_id: click_id.into(),
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

    let mut user_rows: Vec<UserDailyRow> = Vec::with_capacity(person_events.len() * 4);
    let mut camp_counts = vec![0u32; num_camps_with_org];
    let mut in_active = vec![false; num_camps_with_org];
    let mut active_camps: Vec<u16> = Vec::with_capacity(64);
    let mut day_cat_rev = vec![0u64; num_cats];

    for (person, mut evs) in person_events {
        evs.sort_unstable_by_key(|e| e.ts);

        let mut clicks = person_clicks.remove(&person).unwrap_or_default();
        if !clicks.is_empty() {
            clicks.sort_unstable_by(|a, b| {
                a.ts.cmp(&b.ts).then_with(|| Reverse(&a.click_id).cmp(&Reverse(&b.click_id)))
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
    write_user_daily_part(&user_dir.join("part-0.parquet"), &user_rows, &catalog.categories)?;

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
            "[DEBUG_BENCH] cat={:.3}s dsu={:.3}s events={:.3}s clicks={:.3}s sess={:.3}s parquet={:.3}s total={:.3}s (user_rows={}, camp_rows={})",
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

