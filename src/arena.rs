//! Block arena + per-worker scatter buffers.
//!
//! Records are appended into fixed 8 KiB blocks carved from large anonymous
//! mappings (huge-page advised). Blocks never move (no Vec realloc/memmove),
//! and blocks of a consumed partition go back to a free list so the next
//! phase writes into pages that are already faulted in instead of zeroing
//! fresh memory.

use std::sync::Mutex;

pub const BLOCK_BYTES: usize = 8 << 10;
const REGION_BYTES: usize = 256 << 20;

struct Inner {
    regions: Vec<memmap2::MmapMut>,
    cur: *mut u8,
    left: usize,
    free: Vec<*mut u8>,
}

unsafe impl Send for Inner {}

pub struct Arena {
    inner: Mutex<Inner>,
}

impl Arena {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                regions: Vec::new(),
                cur: std::ptr::null_mut(),
                left: 0,
                free: Vec::new(),
            }),
        }
    }

    /// Returns `n` blocks (appended to `out`).
    pub fn take(&self, n: usize, out: &mut Vec<*mut u8>) {
        let mut g = self.inner.lock().unwrap();
        for _ in 0..n {
            if let Some(b) = g.free.pop() {
                out.push(b);
                continue;
            }
            if g.left < BLOCK_BYTES {
                let mut m = memmap2::MmapOptions::new()
                    .len(REGION_BYTES)
                    .map_anon()
                    .expect("arena mmap failed");
                #[cfg(target_os = "linux")]
                if !std::env::var("HELEX_THP").is_ok_and(|v| v == "0") {
                    let _ = m.advise(memmap2::Advice::HugePage);
                }
                g.cur = m.as_mut_ptr();
                g.left = REGION_BYTES;
                g.regions.push(m);
            }
            let b = g.cur;
            g.cur = unsafe { g.cur.add(BLOCK_BYTES) };
            g.left -= BLOCK_BYTES;
            out.push(b);
        }
    }

    pub fn release(&self, blocks: impl IntoIterator<Item = *mut u8>) {
        let mut g = self.inner.lock().unwrap();
        g.free.extend(blocks);
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        // Mappings are reclaimed by process exit; skip the munmap cost.
        let regions = std::mem::take(&mut self.inner.get_mut().unwrap().regions);
        std::mem::forget(regions);
    }
}

/// A filled block: pointer + record count.
#[derive(Clone, Copy)]
pub struct Blk<T> {
    pub ptr: *mut T,
    pub n: u32,
}
unsafe impl<T> Send for Blk<T> {}
unsafe impl<T> Sync for Blk<T> {}

impl<T> Blk<T> {
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.n as usize) }
    }
}

/// Per-worker scatter of `T` records into `parts` partitions.
pub struct Scatter<T: Copy> {
    cur: Vec<*mut T>,
    fill: Vec<u32>,
    pub done: Vec<Vec<Blk<T>>>,
    spare: Vec<*mut u8>,
}
unsafe impl<T: Copy> Send for Scatter<T> {}

impl<T: Copy> Scatter<T> {
    pub const CAP: u32 = (BLOCK_BYTES / std::mem::size_of::<T>()) as u32;

    pub fn new(parts: usize) -> Self {
        Self {
            cur: vec![std::ptr::null_mut(); parts],
            fill: vec![Self::CAP; parts],
            done: (0..parts).map(|_| Vec::new()).collect(),
            spare: Vec::new(),
        }
    }

    #[inline(always)]
    pub fn push(&mut self, arena: &Arena, part: usize, rec: T) {
        let n = unsafe { *self.fill.get_unchecked(part) };
        if n == Self::CAP {
            self.roll(arena, part);
        }
        unsafe {
            let n = *self.fill.get_unchecked(part);
            self.cur.get_unchecked(part).add(n as usize).write(rec);
            *self.fill.get_unchecked_mut(part) = n + 1;
        }
    }

    #[cold]
    #[inline(never)]
    fn roll(&mut self, arena: &Arena, part: usize) {
        let p = self.cur[part];
        if !p.is_null() {
            self.done[part].push(Blk {
                ptr: p,
                n: Self::CAP,
            });
        }
        if self.spare.is_empty() {
            arena.take(32, &mut self.spare);
        }
        self.cur[part] = self.spare.pop().unwrap() as *mut T;
        self.fill[part] = 0;
    }

    /// Flushes partial blocks; returns per-partition block lists.
    pub fn finish(mut self, arena: &Arena) -> Vec<Vec<Blk<T>>> {
        for part in 0..self.cur.len() {
            let p = self.cur[part];
            if !p.is_null() && self.fill[part] > 0 {
                self.done[part].push(Blk {
                    ptr: p,
                    n: self.fill[part],
                });
            } else if !p.is_null() {
                self.spare.push(p as *mut u8);
            }
        }
        arena.release(self.spare.drain(..));
        std::mem::take(&mut self.done)
    }
}
