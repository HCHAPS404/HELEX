use std::env;
use std::fs;

const GIB: u64 = 1024 * 1024 * 1024;

fn read_cgroup_u64(path: &str) -> Option<u64> {
    let text = fs::read_to_string(path).ok()?;
    let trimmed = text.trim();
    if trimmed == "max" || trimmed.is_empty() {
        return None;
    }
    let val = trimmed.parse::<u64>().ok()?;
    if val >= (1u64 << 60) {
        None
    } else {
        Some(val)
    }
}

pub fn detect_memory_limit_bytes() -> u64 {
    let mut limit = 24 * GIB;
    if let Some(v2) = read_cgroup_u64("/sys/fs/cgroup/memory.max") {
        limit = limit.min(v2);
    }
    if let Some(v1) = read_cgroup_u64("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        limit = limit.min(v1);
    }
    if let Ok(mem_gb) = env::var("MEM_GB") {
        if let Ok(gb) = mem_gb.trim().parse::<u64>() {
            if gb > 0 {
                limit = limit.min(gb * GIB);
            }
        }
    }
    limit
}

pub fn should_spill_events(total_event_csv_bytes: u64) -> bool {
    if env::var("FORCE_SPILL").is_ok_and(|v| v == "1") {
        return true;
    }
    let mem_limit = detect_memory_limit_bytes();
    // Resident cost ~= 3.3x CSV bytes (48B records + survivors + person
    // tables + mapped input pages). mem_limit/2 keeps ~2x headroom: at 24 GiB
    // the spill triggers only above ~12 GiB of CSV (~176M events), so the
    // 20M/60M workloads stay fully resident.
    total_event_csv_bytes > mem_limit / 2
}
