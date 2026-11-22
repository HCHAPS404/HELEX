//! Linked-ID resolution: user key -> canonical person key (the byte-wise
//! smallest ID of its linked set).
//!
//! Built in parallel with all threads before event parsing:
//!   1. link files are split into line-aligned ranges and parsed in parallel;
//!      endpoint keys are bucketed into NB hash buckets;
//!   2. each bucket interns its keys into its own small (L2-sized) map;
//!   3. edges are remapped to dense u32 vertex ids in parallel;
//!   4. union-find runs serially on a compact u32 parent array only;
//!   5. component minimums, then per-bucket resolve tables (parallel).
//! Only members whose canon differs from themselves are stored. A lookup is
//! one probe in its bucket's interleaved [key, canon] table.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtOrd};
use std::sync::Mutex;

use crate::csv_scan::{for_each_record, Masks, SCAN_PAD};
use crate::keys::{fast_key_bytes, id_key_padded, is_slow, mix64, shard_of, KeyMap, TAG};

const NB_BITS: u32 = 6;
const NB: usize = 1 << NB_BITS;
const RANGE: usize = 256 << 10;

struct Table {
    tab: Vec<[u64; 2]>,
    mask: usize,
}

pub struct Dsu {
    tables: Vec<Table>,
    empty: bool,
    /// Slow-form member keys with their bytes (for output / registry).
    pub slow_bytes: Vec<(u64, Box<[u8]>)>,
}

impl Dsu {
    #[inline(always)]
    pub fn prefetch(&self, user_key: u64) {
        #[cfg(target_arch = "x86_64")]
        if !self.empty {
            let t = unsafe { self.tables.get_unchecked(shard_of(user_key, NB_BITS)) };
            if !t.tab.is_empty() {
                unsafe {
                    core::arch::x86_64::_mm_prefetch(
                        t.tab.as_ptr().add((mix64(user_key) as usize) & t.mask) as *const i8,
                        core::arch::x86_64::_MM_HINT_T0,
                    );
                }
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = user_key;
    }

    #[inline(always)]
    pub fn resolve(&self, user_key: u64) -> u64 {
        if self.empty {
            return user_key;
        }
        let t = unsafe { self.tables.get_unchecked(shard_of(user_key, NB_BITS)) };
        if t.tab.is_empty() {
            return user_key;
        }
        let mut i = (mix64(user_key) as usize) & t.mask;
        loop {
            let e = unsafe { t.tab.get_unchecked(i) };
            if e[0] == user_key {
                return e[1];
            }
            if e[0] == 0 {
                return user_key;
            }
            i = (i + 1) & t.mask;
        }
    }
}

pub fn csv_paths(folder: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = fs::read_dir(folder)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("csv") && p.is_file())
        .collect();
    out.sort();
    out
}

#[inline]
fn key_bytes<'a>(k: u64, slow: &'a HashMap<u64, Box<[u8]>>, tmp: &'a mut [u8; 9]) -> &'a [u8] {
    if is_slow(k) {
        &slow[&k]
    } else {
        let n = fast_key_bytes(k, b'u', tmp);
        &tmp[..n]
    }
}

/// Byte-wise order of two user ids given their keys.
#[inline]
fn cmp_keys(a: u64, b: u64, slow: &HashMap<u64, Box<[u8]>>) -> Ordering {
    if !is_slow(a) && !is_slow(b) && (a & TAG) == (b & TAG) {
        // same fast form: big-endian zero-padded payloads (no NULs) order
        // exactly like the bytes, and tag-form ids share the prefix byte.
        return (a & !TAG).cmp(&(b & !TAG));
    }
    let mut ta = [0u8; 9];
    let mut tb = [0u8; 9];
    key_bytes(a, slow, &mut ta).cmp(key_bytes(b, slow, &mut tb))
}

fn par<F: Fn(usize) + Sync>(n: usize, threads: usize, f: F) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for _ in 0..threads.min(n).max(1) {
            sc.spawn(|| loop {
                let i = next.fetch_add(1, AtOrd::Relaxed);
                if i >= n {
                    break;
                }
                f(i);
            });
        }
    });
}

struct ParseOut {
    edges: Vec<(u64, u64)>,
    buckets: Vec<Vec<u64>>,
    slow: Vec<(u64, Box<[u8]>)>,
}

pub fn load_links(data_dir: &Path, threads: usize) -> io::Result<Dsu> {
    let paths = csv_paths(&data_dir.join("links"));
    // read files into padded buffers (links are small: tens of MB)
    let mut files: Vec<(Vec<u8>, usize, usize)> = Vec::new(); // (buf, len, data_start)
    for p in &paths {
        let mut b = fs::read(p)?;
        let len = b.len();
        let Some(nl) = memchr::memchr(b'\n', &b) else {
            continue;
        };
        if nl + 1 >= len {
            continue;
        }
        b.resize(len + SCAN_PAD, 0);
        files.push((b, len, nl + 1));
    }
    let mut ranges: Vec<(usize, usize, usize)> = Vec::new(); // (file, start, end)
    for (fi, (_, len, ds)) in files.iter().enumerate() {
        let mut s = *ds;
        while s < *len {
            let e = (s + RANGE).min(*len);
            ranges.push((fi, s, e));
            s = e;
        }
    }
    if ranges.is_empty() {
        return Ok(Dsu {
            tables: Vec::new(),
            empty: true,
            slow_bytes: Vec::new(),
        });
    }

    // 1. parse ranges in parallel (same line ownership rule as the event
    //    chunks: a range owns the lines that start inside it)
    let outs: Vec<Mutex<Option<ParseOut>>> = (0..ranges.len()).map(|_| Mutex::new(None)).collect();
    par(ranges.len(), threads, |ri| {
        let (fi, start, end) = ranges[ri];
        let (buf, len, ds) = &files[fi];
        let from = if start == *ds { start } else { start - 1 };
        let mut po = ParseOut {
            edges: Vec::with_capacity((end - start) / 20 + 8),
            buckets: (0..NB).map(|_| Vec::new()).collect(),
            slow: Vec::new(),
        };
        let mut masks = Masks::default();
        for_each_record::<2>(&buf[from..*len], start == *ds, end - from, &mut masks, |f| {
            let (a, b) = (f[0], f[1]);
            if a.is_empty() || b.is_empty() {
                return;
            }
            let (ka, sa) = unsafe { id_key_padded(a, b'u') };
            let (kb, sb) = unsafe { id_key_padded(b, b'u') };
            if sa {
                po.slow.push((ka, Box::<[u8]>::from(a)));
            }
            if sb {
                po.slow.push((kb, Box::<[u8]>::from(b)));
            }
            po.edges.push((ka, kb));
            po.buckets[shard_of(ka, NB_BITS)].push(ka);
            po.buckets[shard_of(kb, NB_BITS)].push(kb);
        });
        *outs[ri].lock().unwrap() = Some(po);
    });
    let outs: Vec<ParseOut> = outs.into_iter().map(|m| m.into_inner().unwrap().unwrap()).collect();
    let mut slow: HashMap<u64, Box<[u8]>> = HashMap::new();
    for o in &outs {
        for (k, b) in &o.slow {
            slow.entry(*k).or_insert_with(|| b.clone());
        }
    }

    // 2. intern per bucket (parallel): unique keys + local map
    struct Bucket {
        map: KeyMap,
        keys: Vec<u64>,
    }
    let bks: Vec<Mutex<Option<Bucket>>> = (0..NB).map(|_| Mutex::new(None)).collect();
    par(NB, threads, |b| {
        let n: usize = outs.iter().map(|o| o.buckets[b].len()).sum();
        let mut map = KeyMap::with_capacity(n.max(8));
        let mut keys = Vec::with_capacity(n / 2 + 8);
        for o in &outs {
            for &k in &o.buckets[b] {
                let (_, ins) = map.get_or_insert(k, keys.len() as u32);
                if ins {
                    keys.push(k);
                }
            }
        }
        *bks[b].lock().unwrap() = Some(Bucket { map, keys });
    });
    let bks: Vec<Bucket> = bks.into_iter().map(|m| m.into_inner().unwrap().unwrap()).collect();
    let mut base = vec![0u32; NB + 1];
    for b in 0..NB {
        base[b + 1] = base[b] + bks[b].keys.len() as u32;
    }
    let n = base[NB] as usize;
    let gid = |k: u64| -> u32 {
        let b = shard_of(k, NB_BITS);
        base[b] + bks[b].map.get(k).unwrap()
    };

    // 3. remap edges to dense ids (parallel)
    let dense: Vec<Mutex<Vec<(u32, u32)>>> = (0..outs.len()).map(|_| Mutex::new(Vec::new())).collect();
    par(outs.len(), threads, |i| {
        let v: Vec<(u32, u32)> = outs[i].edges.iter().map(|&(a, b)| (gid(a), gid(b))).collect();
        *dense[i].lock().unwrap() = v;
    });

    // 4. union-find (serial, compact u32 array)
    let mut parent: Vec<u32> = (0..n as u32).collect();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        let mut root = x;
        while parent[root as usize] != root {
            root = parent[root as usize];
        }
        while parent[x as usize] != root {
            let next = parent[x as usize];
            parent[x as usize] = root;
            x = next;
        }
        root
    }
    for d in &dense {
        for &(a, b) in d.lock().unwrap().iter() {
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    for i in 0..n as u32 {
        let r = find(&mut parent, i);
        parent[i as usize] = r;
    }

    // 5. component minimum per root (byte compare only across key forms)
    let mut minkey: Vec<u64> = vec![0; n];
    for b in 0..NB {
        for (li, &k) in bks[b].keys.iter().enumerate() {
            let r = parent[base[b] as usize + li] as usize;
            let m = minkey[r];
            if m == 0 || cmp_keys(k, m, &slow) == Ordering::Less {
                minkey[r] = k;
            }
        }
    }

    // 6. per-bucket resolve tables (parallel)
    let tables: Vec<Mutex<Option<Table>>> = (0..NB).map(|_| Mutex::new(None)).collect();
    par(NB, threads, |b| {
        let keys = &bks[b].keys;
        let canon = |li: usize| minkey[parent[base[b] as usize + li] as usize];
        let cnt = keys
            .iter()
            .enumerate()
            .filter(|&(li, &k)| canon(li) != k)
            .count();
        let t = if cnt == 0 {
            Table {
                tab: Vec::new(),
                mask: 0,
            }
        } else {
            let cap = (cnt * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            let mut tab = vec![[0u64; 2]; cap];
            for (li, &k) in keys.iter().enumerate() {
                let c = canon(li);
                if c != k {
                    let mut j = (mix64(k) as usize) & mask;
                    while tab[j][0] != 0 {
                        j = (j + 1) & mask;
                    }
                    tab[j] = [k, c];
                }
            }
            Table { tab, mask }
        };
        *tables[b].lock().unwrap() = Some(t);
    });
    let tables: Vec<Table> = tables.into_iter().map(|m| m.into_inner().unwrap().unwrap()).collect();
    let empty = tables.iter().all(|t| t.tab.is_empty());
    Ok(Dsu {
        tables,
        empty,
        slow_bytes: slow.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::id_key;

    #[test]
    fn chain_and_bytewise_min() {
        let dir = std::env::temp_dir().join(format!("helex_dsu_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("links")).unwrap();
        let mut body = String::from(
            "user_id_a,user_id_b\nU1,u2\nu2,u3\nu3,A0\nlong_user_name_x,u3\n\"q9\",\"u77\"\nu5,u5\n",
        );
        for i in 0..40000 {
            body.push_str(&format!("v{:05},v{:05}\n", i, i + 1));
        }
        body.push_str("ua,ub\nzz_very_long_name_1,zz_very_long_name_0\nzz_very_long_name_1,zzz");
        fs::write(dir.join("links/part-0.csv"), &body).unwrap();
        fs::write(dir.join("links/part-1.csv"), "user_id_a,user_id_b\nw2,w1\nw3,w2\n").unwrap();
        for threads in [1, 4] {
            let d = load_links(&dir, threads).unwrap();
            let k = |s: &[u8]| id_key(s, b'u').0;
            assert_eq!(d.resolve(k(b"u3")), k(b"A0"));
            assert_eq!(d.resolve(k(b"U1")), k(b"A0"));
            assert_eq!(d.resolve(k(b"long_user_name_x")), k(b"A0"));
            assert_eq!(d.resolve(k(b"absent")), k(b"absent"));
            assert_eq!(d.resolve(k(b"v40000")), k(b"v00000"));
            assert_eq!(d.resolve(k(b"v12345")), k(b"v00000"));
            assert_eq!(d.resolve(k(b"u77")), k(b"q9"));
            assert_eq!(d.resolve(k(b"ub")), k(b"ua"));
            assert_eq!(d.resolve(k(b"u5")), k(b"u5"));
            assert_eq!(d.resolve(k(b"zzz")), k(b"zz_very_long_name_0"));
            assert_eq!(d.resolve(k(b"w3")), k(b"w1"));
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
