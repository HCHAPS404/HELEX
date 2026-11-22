use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::csv_scan::{for_each_record, Masks};
use crate::keys::{fast_key_bytes, id_key_padded, is_slow, mix64, KeyMap, TAG};

/// Linked-ID resolution on u64 user keys: user key -> canonical person key
/// (the byte-wise smallest ID of its linked set). Only members whose canon
/// differs from themselves are stored, in an interleaved [key, canon] table
/// so a lookup costs one cache line (and can be prefetched).
pub struct Dsu {
    tab: Vec<[u64; 2]>,
    mask: usize,
    /// Slow-form member keys with their bytes (for output / registry).
    pub slow_bytes: Vec<(u64, Box<[u8]>)>,
}

impl Dsu {
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (mix64(k) as usize) & self.mask
    }

    #[inline(always)]
    pub fn prefetch(&self, user_key: u64) {
        #[cfg(target_arch = "x86_64")]
        if !self.tab.is_empty() {
            unsafe {
                core::arch::x86_64::_mm_prefetch(
                    self.tab.as_ptr().add(self.slot(user_key)) as *const i8,
                    core::arch::x86_64::_MM_HINT_T0,
                );
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = user_key;
    }

    #[inline(always)]
    pub fn resolve(&self, user_key: u64) -> u64 {
        if self.tab.is_empty() {
            return user_key;
        }
        let mut i = self.slot(user_key);
        loop {
            let e = unsafe { self.tab.get_unchecked(i) };
            if e[0] == user_key {
                return e[1];
            }
            if e[0] == 0 {
                return user_key;
            }
            i = (i + 1) & self.mask;
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

type Parsed = (Vec<(u64, u64)>, Vec<(u64, Box<[u8]>)>);

fn parse_links_file(p: &Path) -> io::Result<Parsed> {
    let mut bytes = fs::read(p)?;
    let len = bytes.len();
    bytes.resize(len + crate::csv_scan::SCAN_PAD, 0); // padding for wide loads
    let data = &bytes[..len];
    let mut pairs = Vec::with_capacity(len / 22 + 16);
    let mut slow = Vec::new();
    let mut masks = Masks::default();
    // skip header: records start after the first newline
    for_each_record::<2>(data, false, len, &mut masks, |f| {
        let (a, b) = (f[0], f[1]);
        if a.is_empty() || b.is_empty() {
            return;
        }
        let (ka, sa) = unsafe { id_key_padded(a, b'u') };
        let (kb, sb) = unsafe { id_key_padded(b, b'u') };
        if sa {
            slow.push((ka, Box::<[u8]>::from(a)));
        }
        if sb {
            slow.push((kb, Box::<[u8]>::from(b)));
        }
        pairs.push((ka, kb));
    });
    Ok((pairs, slow))
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

pub fn load_links(data_dir: &Path) -> io::Result<Dsu> {
    let paths = csv_paths(&data_dir.join("links"));
    let parsed: Vec<io::Result<Parsed>> = std::thread::scope(|sc| {
        let hs: Vec<_> = paths
            .iter()
            .map(|p| sc.spawn(move || parse_links_file(p)))
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut all_pairs: Vec<Vec<(u64, u64)>> = Vec::with_capacity(parsed.len());
    let mut slow: HashMap<u64, Box<[u8]>> = HashMap::new();
    for r in parsed {
        let (pairs, s) = r?;
        all_pairs.push(pairs);
        for (k, b) in s {
            slow.entry(k).or_insert(b);
        }
    }
    let npairs: usize = all_pairs.iter().map(|v| v.len()).sum();

    // union-find over interned keys
    let mut idx = KeyMap::with_capacity(npairs.max(8) * 2);
    let mut keys: Vec<u64> = Vec::with_capacity(npairs * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(npairs * 2);
    let mut size: Vec<u32> = Vec::with_capacity(npairs * 2);
    let mut minkey: Vec<u64> = Vec::with_capacity(npairs * 2);

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

    for pairs in &all_pairs {
        for (i, &(ka, kb)) in pairs.iter().enumerate() {
            if let Some(&(na, nb)) = pairs.get(i + 16) {
                idx.prefetch(na);
                idx.prefetch(nb);
            }
            let mut intern = |k: u64| -> u32 {
                let next = keys.len() as u32;
                let (v, ins) = idx.get_or_insert(k, next);
                if ins {
                    keys.push(k);
                    parent.push(next);
                    size.push(1);
                    minkey.push(k);
                }
                v
            };
            let ia = intern(ka);
            let ib = intern(kb);
            if ia == ib {
                continue;
            }
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra == rb {
                continue;
            }
            let (root, child) = if size[ra as usize] < size[rb as usize] {
                (rb, ra)
            } else {
                (ra, rb)
            };
            parent[child as usize] = root;
            size[root as usize] += size[child as usize];
            let rm = minkey[root as usize];
            let cm = minkey[child as usize];
            if cmp_keys(cm, rm, &slow) == Ordering::Less {
                minkey[root as usize] = cm;
            }
        }
    }

    let n = keys.len();
    let mut cnt = 0usize;
    let mut canon_of: Vec<u64> = vec![0; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        canon_of[i] = minkey[r as usize];
        if canon_of[i] != keys[i] {
            cnt += 1;
        }
    }
    let (tab, mask) = if cnt == 0 {
        (Vec::new(), 0)
    } else {
        let cap = (cnt * 2).next_power_of_two().max(16);
        let mut tab = vec![[0u64; 2]; cap];
        let mask = cap - 1;
        for i in 0..n {
            let k = keys[i];
            if canon_of[i] != k {
                let mut j = (mix64(k) as usize) & mask;
                while tab[j][0] != 0 {
                    j = (j + 1) & mask;
                }
                tab[j] = [k, canon_of[i]];
            }
        }
        (tab, mask)
    };
    let slow_bytes = slow.into_iter().collect();
    Ok(Dsu {
        tab,
        mask,
        slow_bytes,
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
        for i in 0..400 {
            body.push_str(&format!("v{:04},v{:04}\n", i, i + 1));
        }
        body.push_str("ua,ub\nzz_very_long_name_1,zz_very_long_name_0\nzz_very_long_name_1,zzz");
        fs::write(dir.join("links/part-0.csv"), body).unwrap();
        let d = load_links(&dir).unwrap();
        let k = |s: &[u8]| id_key(s, b'u').0;
        assert_eq!(d.resolve(k(b"u3")), k(b"A0"));
        assert_eq!(d.resolve(k(b"U1")), k(b"A0"));
        assert_eq!(d.resolve(k(b"long_user_name_x")), k(b"A0"));
        assert_eq!(d.resolve(k(b"absent")), k(b"absent"));
        assert_eq!(d.resolve(k(b"v0400")), k(b"v0000"));
        assert_eq!(d.resolve(k(b"u77")), k(b"q9"));
        assert_eq!(d.resolve(k(b"ub")), k(b"ua"));
        assert_eq!(d.resolve(k(b"u5")), k(b"u5"));
        assert_eq!(d.resolve(k(b"zzz")), k(b"zz_very_long_name_0"));
        let _ = fs::remove_dir_all(&dir);
    }
}
