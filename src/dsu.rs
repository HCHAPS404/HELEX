use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use hashbrown::HashTable;

use crate::csv_dialect::{iter_data_lines, parse_fixed_fields};
use crate::sso::{hash64_bytes, SmallBytes};

pub struct UnionFind {
    table: HashTable<u32>,
    strings: Vec<SmallBytes>,
    parent: Vec<u32>,
    size: Vec<u32>,
    min_id_idx: Vec<u32>,
}

impl UnionFind {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            table: HashTable::with_capacity(cap),
            strings: Vec::with_capacity(cap),
            parent: Vec::with_capacity(cap),
            size: Vec::with_capacity(cap),
            min_id_idx: Vec::with_capacity(cap),
        }
    }

    #[inline]
    pub fn intern(&mut self, s: &[u8]) -> u32 {
        let hash = hash64_bytes(s);
        let strings = &self.strings;
        if let Some(&idx) = self
            .table
            .find(hash, |&idx| strings[idx as usize].eq_slice(s))
        {
            return idx;
        }
        let idx = self.strings.len() as u32;
        self.strings.push(SmallBytes::from_slice(s));
        self.parent.push(idx);
        self.size.push(1);
        self.min_id_idx.push(idx);
        let strings = &self.strings;
        self.table
            .insert_unique(hash, idx, |&i| strings[i as usize].hash64());
        idx
    }

    #[inline]
    pub fn find_index(&mut self, index: u32) -> u32 {
        let mut root = index;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        let mut cur = index;
        while self.parent[cur as usize] != root {
            let next = self.parent[cur as usize];
            self.parent[cur as usize] = root;
            cur = next;
        }
        root
    }

    #[inline]
    pub fn union_bytes(&mut self, a: &[u8], b: &[u8]) {
        if a == b {
            self.intern(a);
            return;
        }
        let ia = self.intern(a);
        let ib = self.intern(b);
        let ra = self.find_index(ia);
        let rb = self.find_index(ib);
        if ra == rb {
            return;
        }
        let sa = self.size[ra as usize];
        let sb = self.size[rb as usize];
        let (root, child) = if sa < sb { (rb, ra) } else { (ra, rb) };
        self.parent[child as usize] = root;
        self.size[root as usize] = sa + sb;

        let root_min = self.min_id_idx[root as usize];
        let child_min = self.min_id_idx[child as usize];
        if self.strings[child_min as usize] < self.strings[root_min as usize] {
            self.min_id_idx[root as usize] = child_min;
        }
    }

    pub fn finalize(&mut self) {
        for idx in 0..self.parent.len() as u32 {
            let root = self.find_index(idx);
            self.min_id_idx[idx as usize] = self.min_id_idx[root as usize];
        }
    }

    #[inline]
    pub fn resolve<'a>(&'a self, raw_id: &'a [u8]) -> &'a [u8] {
        if self.strings.is_empty() {
            return raw_id;
        }
        let hash = hash64_bytes(raw_id);
        if let Some(&idx) = self
            .table
            .find(hash, |&i| self.strings[i as usize].eq_slice(raw_id))
        {
            let min_idx = self.min_id_idx[idx as usize];
            self.strings[min_idx as usize].as_slice()
        } else {
            raw_id
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

pub fn load_links(data_dir: &Path) -> io::Result<UnionFind> {
    let paths = csv_paths(&data_dir.join("links"));
    let total_bytes: u64 = paths
        .iter()
        .filter_map(|p| p.metadata().ok().map(|m| m.len()))
        .sum();
    let est_ids = ((total_bytes / 12) as usize).clamp(16, 4_000_000);
    let mut uf = UnionFind::with_capacity(est_ids);
    for path in paths {
        let bytes = fs::read(path)?;
        iter_data_lines(&bytes, |line| {
            if let Some([a, b]) = parse_fixed_fields::<2>(line) {
                if !a.is_empty() && !b.is_empty() {
                    uf.union_bytes(a, b);
                }
            }
        });
    }
    uf.finalize();
    Ok(uf)
}

#[cfg(test)]
mod tests {
    use super::UnionFind;

    #[test]
    fn keeps_bytewise_minimum_across_chain_and_ghosts() {
        let mut uf = UnionFind::with_capacity(16);
        uf.union_bytes(b"U1", b"u2");
        uf.union_bytes(b"u2", b"u3");
        uf.union_bytes(b"u3", b"A0");
        uf.finalize();

        assert_eq!(uf.resolve(b"u3"), b"A0");
        assert_eq!(uf.resolve(b"U1"), b"A0");
        assert_eq!(uf.resolve(b"absent"), b"absent");
    }

    #[test]
    fn long_chain_400_hops_no_stack_overflow() {
        let mut uf = UnionFind::with_capacity(512);
        for i in 0..400usize {
            let a = format!("u{i:04}");
            let b = format!("u{:04}", i + 1);
            uf.union_bytes(a.as_bytes(), b.as_bytes());
        }
        uf.finalize();
        assert_eq!(uf.resolve(b"u0400"), b"u0000");
    }
}
