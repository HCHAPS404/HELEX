use std::cmp::Ordering;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::csv_dialect::{iter_data_lines, split_fields_csv};
use crate::keys::{fast_key_bytes, id_key, is_slow, KeyMap};

/// Linked-ID resolution on u64 user keys: user key -> canonical person key
/// (the byte-wise smallest ID of its linked set).
pub struct Dsu {
    map: KeyMap,
    canon: Vec<u64>,
    /// Slow-form member keys with their bytes (for output / registry).
    pub slow_bytes: Vec<(u64, Box<[u8]>)>,
}

impl Dsu {
    #[inline(always)]
    pub fn resolve(&self, user_key: u64) -> u64 {
        if self.canon.is_empty() {
            return user_key;
        }
        match self.map.get(user_key) {
            Some(i) => unsafe { *self.canon.get_unchecked(i as usize) },
            None => user_key,
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

struct Uf {
    idx: KeyMap,
    keys: Vec<u64>,
    parent: Vec<u32>,
    size: Vec<u32>,
    min_of: Vec<u32>,
    slow: Vec<Option<Box<[u8]>>>,
}

impl Uf {
    fn intern(&mut self, k: u64, raw: &[u8], slow: bool) -> u32 {
        let next = self.keys.len() as u32;
        let (i, inserted) = self.idx.get_or_insert(k, next);
        if inserted {
            self.keys.push(k);
            self.parent.push(next);
            self.size.push(1);
            self.min_of.push(next);
            self.slow.push(if slow { Some(raw.into()) } else { None });
        }
        i
    }

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
        let rm = self.min_of[root as usize];
        let cm = self.min_of[child as usize];
        if self.cmp_ids(cm, rm) == Ordering::Less {
            self.min_of[root as usize] = cm;
        }
    }
}

pub fn load_links(data_dir: &Path) -> io::Result<Dsu> {
    let paths = csv_paths(&data_dir.join("links"));
    let mut uf = Uf {
        idx: KeyMap::with_capacity(1024),
        keys: Vec::new(),
        parent: Vec::new(),
        size: Vec::new(),
        min_of: Vec::new(),
        slow: Vec::new(),
    };
    for p in paths {
        let bytes = fs::read(&p)?;
        iter_data_lines(&bytes, |line| {
            if let Some([a, b]) = split_fields_csv::<2>(line) {
                if !a.is_empty() && !b.is_empty() {
                    let (ka, sa) = id_key(a, b'u');
                    let (kb, sb) = id_key(b, b'u');
                    let ia = uf.intern(ka, a, sa);
                    let ib = uf.intern(kb, b, sb);
                    if ia != ib {
                        uf.union(ia, ib);
                    }
                }
            }
        });
    }
    let n = uf.keys.len();
    let mut map = KeyMap::with_capacity(n.max(1));
    let mut canon: Vec<u64> = Vec::new();
    for i in 0..n as u32 {
        let r = uf.find(i);
        let m = uf.min_of[r as usize];
        if m != i {
            map.insert(uf.keys[i as usize], canon.len() as u32);
            canon.push(uf.keys[m as usize]);
        }
    }
    let slow_bytes = uf
        .keys
        .iter()
        .zip(uf.slow.iter_mut())
        .filter_map(|(&k, s)| s.take().map(|b| (k, b)))
        .collect::<Vec<_>>();
    debug_assert!(slow_bytes.iter().all(|(k, _)| is_slow(*k)));
    Ok(Dsu {
        map,
        canon,
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
