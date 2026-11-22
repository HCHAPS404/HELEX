use std::cmp::Ordering;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::csv_dialect::{iter_data_lines, split_fields_csv};
use crate::keys::{fast_key_bytes, id_key_padded, is_slow, mix64, KeyMap};
#[cfg(test)]
use crate::keys::id_key;

/// Linked-ID resolution on u64 user keys: user key -> canonical person key
/// (the byte-wise smallest ID of its linked set).
pub struct Dsu {
    /// Bloom-style bit filter over member keys: most users are unlinked and
    /// skip the map probe.
    filter: Vec<u64>,
    fbits: u32,
    map: KeyMap,
    canon: Vec<u64>,
    /// Slow-form member keys with their bytes (for output / registry).
    pub slow_bytes: Vec<(u64, Box<[u8]>)>,
    /// Dense canon: user_index -> canon key for linked dense users.
    /// `None` when dense keys are off or the index space collided.
    utab: Option<Vec<u64>>,
}

impl Dsu {
    #[inline(always)]
    pub fn resolve(&self, user_key: u64) -> u64 {
        if self.canon.is_empty() {
            return user_key;
        }
        let h = (mix64(user_key) >> (64 - self.fbits)) as usize;
        if unsafe { *self.filter.get_unchecked(h >> 6) } & (1u64 << (h & 63)) == 0 {
            return user_key;
        }
        match self.map.get(user_key) {
            Some(i) => unsafe { *self.canon.get_unchecked(i as usize) },
            None => user_key,
        }
    }

    /// Canon for a dense-decoded user: the alias-indexed table answers
    /// linked members; `byte_key` is returned for unlinked users. `None`
    /// means the caller must use `resolve` (dense canon unavailable).
    #[inline(always)]
    pub fn resolve_dense(&self, counter: u64, byte_key: u64) -> Option<u64> {
        let utab = self.utab.as_ref()?;
        let u = (counter >> crate::counters::ALIAS_BITS) as usize;
        Some(if u < utab.len() && utab[u] != 0 {
            utab[u]
        } else {
            byte_key
        })
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

struct Uf {
    keys: Vec<u64>,
    parent: Vec<u32>,
    size: Vec<u32>,
    min_of: Vec<u32>,
    slow: Vec<Option<Box<[u8]>>>,
}

impl Uf {
    fn find(&mut self, mut x: u32) -> u32 {
        let mut root = x;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while self.parent[x as usize] != root {
            let next = self.parent[x as usize];
            self.parent[x as usize] = root;
            x = next;
        }
        root
    }

    fn cmp_ids(&self, a: u32, b: u32) -> Ordering {
        let mut ba = [0u8; 9];
        let mut bb = [0u8; 9];
        let sa: &[u8] = match &self.slow[a as usize] {
            Some(s) => s,
            None => {
                let n = fast_key_bytes(self.keys[a as usize], b'u', &mut ba);
                &ba[..n]
            }
        };
        let sb: &[u8] = match &self.slow[b as usize] {
            Some(s) => s,
            None => {
                let n = fast_key_bytes(self.keys[b as usize], b'u', &mut bb);
                &bb[..n]
            }
        };
        sa.cmp(sb)
    }

    fn union(&mut self, a: u32, b: u32) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        let (root, child) = if self.size[ra as usize] < self.size[rb as usize] {
            (rb, ra)
        } else {
            (ra, rb)
        };
        self.parent[child as usize] = root;
        self.size[root as usize] += self.size[child as usize];
    }

    /// Component minimum by byte-wise ID order, computed in one pass after
    /// all unions (each vertex compared once against the running min).
    fn compute_mins(&mut self) {
        for i in 0..self.keys.len() as u32 {
            let r = self.find(i);
            let m = self.min_of[r as usize];
            if self.cmp_ids(i, m) == Ordering::Less {
                self.min_of[r as usize] = i;
            }
        }
    }
}

/// Iterate `lines` (a range already snapped to whole lines, no header).
fn iter_lines_raw(lines: &[u8], mut f: impl FnMut(&[u8])) {
    let mut pos = 0usize;
    while pos < lines.len() {
        match memchr::memchr(b'\n', &lines[pos..]) {
            Some(rel) => {
                f(&lines[pos..pos + rel]);
                pos += rel + 1;
            }
            None => {
                f(&lines[pos..]);
                break;
            }
        }
    }
}

const PAD_END: usize = 16;
const PARTS: usize = 64;

#[inline(always)]
fn part_of(k: u64) -> usize {
    (mix64(k) >> 58) as usize
}

/// Parsed link edges plus a slab of raw bytes for slow keys.
/// Endpoint keys are also scattered into `buckets` by partition so the
/// interning stage can dedup each partition on its own L2-resident map.
struct LinkChunk {
    edges: Vec<(u64, u64)>,
    buckets: Vec<Vec<u64>>,
    slow: Vec<(u64, u32)>,
    slab: Vec<u8>,
    /// (user_index, byte_key) for endpoints that decode to a dense counter;
    /// used to build the post-DSU dense canon table.
    dense: Vec<(u64, u64)>,
}

impl LinkChunk {
    fn parse(&mut self, lines: &[u8], skip_header: bool, ukey: Option<u64>) {
        let mut handle = |line: &[u8]| {
            if let Some([a, b]) = split_fields_csv::<2>(line) {
                if !a.is_empty() && !b.is_empty() {
                    let (ka, sa) = unsafe { id_key_padded(a, b'u') };
                    let (kb, sb) = unsafe { id_key_padded(b, b'u') };
                    if let Some(k) = ukey {
                        if let Some(c) = crate::counters::decode(a, b'u', k, crate::counters::USER_CAP) {
                            self.dense.push((c >> crate::counters::ALIAS_BITS, ka));
                        }
                        if let Some(c) = crate::counters::decode(b, b'u', k, crate::counters::USER_CAP) {
                            self.dense.push((c >> crate::counters::ALIAS_BITS, kb));
                        }
                    }
                    if sa {
                        self.slow.push((ka, self.slab.len() as u32));
                        self.slab.push(a.len() as u8);
                        self.slab.extend_from_slice(a);
                    }
                    if sb {
                        self.slow.push((kb, self.slab.len() as u32));
                        self.slab.push(b.len() as u8);
                        self.slab.extend_from_slice(b);
                    }
                    self.buckets[part_of(ka)].push(ka);
                    self.buckets[part_of(kb)].push(kb);
                    self.edges.push((ka, kb));
                }
            }
        };
        if skip_header {
            iter_data_lines(lines, &mut handle);
        } else {
            iter_lines_raw(lines, &mut handle);
        }
    }
}

fn slab_get(slab: &[u8], off: u32) -> &[u8] {
    let n = slab[off as usize] as usize;
    &slab[off as usize + 1..off as usize + 1 + n]
}

pub fn load_links(data_dir: &Path, threads: usize, ukey: Option<u64>) -> io::Result<Dsu> {
    let paths = csv_paths(&data_dir.join("links"));
    let mut file_bufs: Vec<Vec<u8>> = Vec::with_capacity(paths.len());
    let mut _total_lines = 0usize;
    for p in &paths {
        let mut bytes = fs::read(p)?;
        // Padding so fields near EOF stay readable by the wide-load key path.
        bytes.extend_from_slice(&[0; PAD_END]);
        _total_lines += memchr::memchr_iter(b'\n', &bytes).count();
        file_bufs.push(bytes);
    }

    // Split every file into chunks snapped to line boundaries.
    let chunks_per_file = (threads * 2).max(4);
    let mut tasks: Vec<(usize, usize, usize, bool)> = Vec::new(); // (file, start, end, has_header)
    for (fi, bytes) in file_bufs.iter().enumerate() {
        let logical = bytes.len() - PAD_END;
        let mut s = 0usize;
        let mut first = true;
        for c in 0..chunks_per_file {
            if s >= logical {
                break;
            }
            let nominal = logical * (c + 1) / chunks_per_file;
            let e = if nominal >= logical {
                logical
            } else {
                match memchr::memchr(b'\n', &bytes[nominal..logical]) {
                    Some(rel) => nominal + rel + 1,
                    None => logical,
                }
            };
            tasks.push((fi, s, e, first));
            first = false;
            s = e;
        }
    }

    // Parallel chunk parse -> per-task edge lists (deterministic task order).
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results: Vec<LinkChunk> = std::thread::scope(|sc| {
        let bufs_ref = &file_bufs;
        let tasks_ref = &tasks;
        let mut handles = Vec::new();
        for _ in 0..threads.max(1) {
            let next_ref = &next;
            handles.push(sc.spawn(move || {
                let mut out: Vec<(usize, LinkChunk)> = Vec::new();
                loop {
                    let ti = next_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if ti >= tasks_ref.len() {
                        break;
                    }
                    let (fi, s, e, hdr) = tasks_ref[ti];
                    let mut c = LinkChunk {
                        edges: Vec::new(),
                        buckets: vec![Vec::new(); PARTS],
                        slow: Vec::new(),
                        slab: Vec::new(),
                        dense: Vec::new(),
                    };
                    c.parse(&bufs_ref[fi][s..e], hdr, ukey);
                    out.push((ti, c));
                }
                out
            }));
        }
        let mut per_task: Vec<LinkChunk> = Vec::with_capacity(tasks.len());
        let mut merged: Vec<(usize, LinkChunk)> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        merged.sort_by_key(|(ti, _)| *ti);
        for (_, c) in merged {
            per_task.push(c);
        }
        per_task
    });

    // Slow registry: stable sort by key -> first entry per key is the
    // first-seen bytes, matching intern()'s keep-first semantics.
    let mut slow_list: Vec<(u64, u32)> = Vec::new();
    let mut slab: Vec<u8> = Vec::new();
    for c in &results {
        let base = slab.len() as u32;
        slab.extend_from_slice(&c.slab);
        slow_list.extend(c.slow.iter().map(|&(k, o)| (k, o + base)));
    }
    slow_list.sort_by_key(|&(k, _)| k);
    let slab_ref = &slab;
    let slow_bytes_for = |k: u64| -> &[u8] {
        let i = slow_list.partition_point(|&(kk, _)| kk < k);
        if i < slow_list.len() && slow_list[i].0 == k {
            slab_get(slab_ref, slow_list[i].1)
        } else {
            &[]
        }
    };

    // Per-partition dedup: each partition's keys live in their own
    // L2-resident map; local index -> global index via partition base.
    struct Part {
        map: KeyMap,
        uniques: Vec<u64>,
        base: u32,
    }
    let results_ref = &results;
    let parts: Vec<Part> = {
        let next_p = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|sc| {
            let mut handles = Vec::new();
            for _ in 0..threads.max(1) {
                let next_ref = &next_p;
                let rr = results_ref;
                handles.push(sc.spawn(move || {
                    let mut out: Vec<(usize, KeyMap, Vec<u64>)> = Vec::new();
                    loop {
                        let p = next_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if p >= PARTS {
                            break;
                        }
                        let total: usize = rr.iter().map(|c| c.buckets[p].len()).sum();
                        let mut map = KeyMap::with_capacity(total.max(1));
                        let mut uniques: Vec<u64> = Vec::with_capacity(total / 2 + 1);
                        for c in rr.iter() {
                            for &k in &c.buckets[p] {
                                let (idx, ins) = map.get_or_insert(k, uniques.len() as u32);
                                if ins {
                                    debug_assert_eq!(idx as usize, uniques.len());
                                    uniques.push(k);
                                }
                            }
                        }
                        out.push((p, map, uniques));
                    }
                    out
                }));
            }
            let mut merged: Vec<(usize, KeyMap, Vec<u64>)> = handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect();
            merged.sort_by_key(|(p, _, _)| *p);
            let mut parts: Vec<Part> = Vec::with_capacity(PARTS);
            let mut base = 0u32;
            for (_, map, uniques) in merged {
                parts.push(Part {
                    map,
                    base,
                    uniques,
                });
                base += parts.last().unwrap().uniques.len() as u32;
            }
            parts
        })
    };
    let n_vertices: usize = parts.iter().map(|p| p.uniques.len()).sum();

    // Global vertex arrays (index = part.base + local position).
    let mut keys: Vec<u64> = Vec::with_capacity(n_vertices);
    let mut slow_v: Vec<Option<Box<[u8]>>> = Vec::with_capacity(n_vertices);
    for p in &parts {
        for &k in &p.uniques {
            keys.push(k);
            slow_v.push(if is_slow(k) {
                Some(slow_bytes_for(k).into())
            } else {
                None
            });
        }
    }

    // Remap edges to (ia, ib) pairs: parallel over task chunks, partition
    // map probes are L2-resident now.
    let parts_ref = &parts;
    let pair_chunks: Vec<Vec<(u32, u32)>> = {
        let next_t = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|sc| {
            let mut handles = Vec::new();
            for _ in 0..threads.max(1) {
                let next_ref = &next_t;
                let rr = results_ref;
                let pr = parts_ref;
                handles.push(sc.spawn(move || {
                    let mut out: Vec<(usize, Vec<(u32, u32)>)> = Vec::new();
                    loop {
                        let ti = next_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if ti >= rr.len() {
                            break;
                        }
                        let mut pairs = Vec::with_capacity(rr[ti].edges.len());
                        for &(ka, kb) in &rr[ti].edges {
                            let pa = &pr[part_of(ka)];
                            let pb = &pr[part_of(kb)];
                            pairs.push((
                                pa.base + pa.map.get(ka).unwrap(),
                                pb.base + pb.map.get(kb).unwrap(),
                            ));
                        }
                        out.push((ti, pairs));
                    }
                    out
                }));
            }
            let mut merged: Vec<(usize, Vec<(u32, u32)>)> = handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect();
            merged.sort_by_key(|(t, _)| *t);
            merged.into_iter().map(|(_, v)| v).collect()
        })
    };

    let mut uf = Uf {
        keys,
        parent: (0..n_vertices as u32).collect(),
        size: vec![1; n_vertices],
        min_of: (0..n_vertices as u32).collect(),
        slow: slow_v,
    };
    for pairs in &pair_chunks {
        for &(ia, ib) in pairs {
            if ia != ib {
                uf.union(ia, ib);
            }
        }
    }
    uf.compute_mins();
    let n = uf.keys.len();
    let mut map = KeyMap::with_capacity(n.max(1));
    let mut canon: Vec<u64> = Vec::new();
    let fbits = ((n.max(1) * 16).next_power_of_two().trailing_zeros()).clamp(12, 30);
    let mut filter = vec![0u64; (1usize << fbits) / 64];
    for i in 0..n as u32 {
        let r = uf.find(i);
        let m = uf.min_of[r as usize];
        if m != i {
            let k = uf.keys[i as usize];
            map.insert(k, canon.len() as u32);
            canon.push(uf.keys[m as usize]);
            let h = (mix64(k) >> (64 - fbits)) as usize;
            filter[h >> 6] |= 1u64 << (h & 63);
        }
    }
    let slow_bytes = uf
        .keys
        .iter()
        .zip(uf.slow.iter_mut())
        .filter_map(|(&k, s)| s.take().map(|b| (k, b)))
        .collect::<Vec<_>>();
    debug_assert!(slow_bytes.iter().all(|(k, _)| is_slow(*k)));

    // Dense canon table: user_index (counter >> ALIAS_BITS) -> canon key.
    // Disabled on any index collision (two members of different components
    // sharing an index) or when the index space is too large.
    let utab = if ukey.is_some() {
        let pairs: Vec<(u64, u64)> = results
            .iter()
            .flat_map(|c| c.dense.iter().copied())
            .collect();
        let max_idx = pairs.iter().map(|p| p.0).max().unwrap_or(0);
        if max_idx >= (1 << 26) {
            None
        } else {
            let mut t = vec![0u64; max_idx as usize + 1];
            let mut conflict = false;
            for &(u, k) in &pairs {
                let c = match map.get(k) {
                    Some(i) => canon[i as usize],
                    None => k,
                };
                if t[u as usize] != 0 && t[u as usize] != c {
                    conflict = true;
                    break;
                }
                t[u as usize] = c;
            }
            if conflict {
                None
            } else {
                Some(t)
            }
        }
    } else {
        None
    };
    Ok(Dsu {
        filter,
        fbits,
        map,
        canon,
        utab,
        slow_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_and_bytewise_min() {
        let dir = std::env::temp_dir().join(format!("helex_dsu_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("links")).unwrap();
        let mut body =
            String::from("user_id_a,user_id_b\nU1,u2\nu2,u3\nu3,A0\nlong_user_name_x,u3\n");
        for i in 0..400 {
            body.push_str(&format!("v{:04},v{:04}\n", i, i + 1));
        }
        fs::write(dir.join("links/part-0.csv"), body).unwrap();
        let d = load_links(&dir).unwrap();
        let k = |s: &[u8]| id_key(s, b'u').0;
        assert_eq!(d.resolve(k(b"u3")), k(b"A0"));
        assert_eq!(d.resolve(k(b"U1")), k(b"A0"));
        assert_eq!(d.resolve(k(b"long_user_name_x")), k(b"A0"));
        assert_eq!(d.resolve(k(b"absent")), k(b"absent"));
        assert_eq!(d.resolve(k(b"v0400")), k(b"v0000"));
        let _ = fs::remove_dir_all(&dir);
    }
}
