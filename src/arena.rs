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

/// Scatter with per-partition staging and non-temporal block writes: K
/// records are gathered in a small cache-resident buffer, then written to the
/// block as whole cache lines with streaming stores (no read-for-ownership of
/// the destination lines). `K * size_of::<T>()` must be a multiple of 64.
pub struct StageScatter<T: Copy, const K: usize> {
    inner: Scatter<T>,
    stage: Vec<[T; K]>,
    stage_n: Vec<u8>,
}
unsafe impl<T: Copy, const K: usize> Send for StageScatter<T, K> {}

impl<T: Copy, const K: usize> StageScatter<T, K> {
    const CAPK: u32 = (Scatter::<T>::CAP / K as u32) * K as u32;

    pub fn new(parts: usize, zero: T) -> Self {
        assert!((K * std::mem::size_of::<T>()) % 64 == 0);
        Self {
            inner: Scatter::new(parts),
            stage: vec![[zero; K]; parts],
            stage_n: vec![0; parts],
        }
    }

    #[inline(always)]
    pub fn push(&mut self, arena: &Arena, part: usize, rec: T) {
        unsafe {
            let n = *self.stage_n.get_unchecked(part) as usize;
            *self.stage.get_unchecked_mut(part).get_unchecked_mut(n) = rec;
            if n + 1 < K {
                *self.stage_n.get_unchecked_mut(part) = (n + 1) as u8;
            } else {
                *self.stage_n.get_unchecked_mut(part) = 0;
                self.flush_full(arena, part);
            }
        }
    }

    #[inline(always)]
    unsafe fn flush_full(&mut self, arena: &Arena, part: usize) {
        let fill = *self.inner.fill.get_unchecked(part);
        if fill + K as u32 > Self::CAPK {
            self.inner.roll(arena, part);
        }
        let fill = *self.inner.fill.get_unchecked(part);
        let dst = self.inner.cur.get_unchecked(part).add(fill as usize) as *mut u8;
        let src = self.stage.get_unchecked(part).as_ptr() as *const u8;
        stream_copy(dst, src, K * std::mem::size_of::<T>());
        *self.inner.fill.get_unchecked_mut(part) = fill + K as u32;
    }

    pub fn finish(mut self, arena: &Arena) -> Vec<Vec<Blk<T>>> {
        for part in 0..self.stage_n.len() {
            let n = self.stage_n[part] as usize;
            for i in 0..n {
                let r = self.stage[part][i];
                self.inner.push(arena, part, r);
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            core::arch::x86_64::_mm_sfence();
        }
        self.inner.finish(arena)
    }
}

/// Copies `len` bytes (multiple of 64; `dst` 32-byte aligned) with streaming
/// stores when AVX is available.
#[inline(always)]
unsafe fn stream_copy(dst: *mut u8, src: *const u8, len: usize) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
    {
        use std::arch::x86_64::*;
        let mut o = 0;
        while o < len {
            let v = _mm256_loadu_si256(src.add(o) as *const __m256i);
            _mm256_stream_si256(dst.add(o) as *mut __m256i, v);
            o += 32;
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx")))]
    {
        std::ptr::copy_nonoverlapping(src, dst, len);
    }
}
