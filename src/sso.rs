use std::cmp::Ordering;
use std::fmt;

#[repr(C, align(8))]
pub struct SmallBytes {
    raw: [u8; 16],
}

const HEAP_TAG: u8 = 0xFF;

#[inline]
pub fn hash64_bytes(bytes: &[u8]) -> u64 {
    let mut h: u64;
    if bytes.len() <= 15 {
        let mut buf = [0u8; 16];
        buf[..bytes.len()].copy_from_slice(bytes);
        buf[15] = bytes.len() as u8;
        let w0 = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let w1 = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        h = w0.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ w1.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    } else {
        h = (bytes.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            let w = u64::from_le_bytes(chunk.try_into().unwrap());
            h = (h ^ w).wrapping_mul(0x94D0_49BB_1331_11EB);
        }
        let rem = chunks.remainder();
        if !rem.is_empty() {
            let mut tail = [0u8; 8];
            tail[..rem.len()].copy_from_slice(rem);
            h = (h ^ u64::from_le_bytes(tail)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        }
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^ (h >> 31)
}

impl SmallBytes {
    #[inline]
    pub fn from_slice(s: &[u8]) -> Self {
        if s.len() <= 15 {
            let mut raw = [0u8; 16];
            raw[..s.len()].copy_from_slice(s);
            raw[15] = s.len() as u8;
            Self { raw }
        } else {
            Self::from_slice_heap(s)
        }
    }

    #[cold]
    fn from_slice_heap(s: &[u8]) -> Self {
        assert!(s.len() <= u32::MAX as usize);
        let boxed: Box<[u8]> = s.into();
        let len = boxed.len() as u32;
        let ptr = Box::into_raw(boxed) as *mut u8 as usize;
        let mut raw = [0u8; 16];
        raw[0..8].copy_from_slice(&ptr.to_ne_bytes());
        raw[8..12].copy_from_slice(&len.to_ne_bytes());
        raw[15] = HEAP_TAG;
        Self { raw }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        let tag = self.raw[15];
        if tag != HEAP_TAG {
            &self.raw[..tag as usize]
        } else {
            unsafe { self.heap_slice() }
        }
    }

    #[cold]
    unsafe fn heap_slice(&self) -> &[u8] {
        let ptr = usize::from_ne_bytes(self.raw[0..8].try_into().unwrap()) as *const u8;
        let len = u32::from_ne_bytes(self.raw[8..12].try_into().unwrap()) as usize;
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }

    #[inline]
    pub fn eq_slice(&self, other: &[u8]) -> bool {
        let tag = self.raw[15];
        if tag != HEAP_TAG {
            tag as usize == other.len() && &self.raw[..tag as usize] == other
        } else {
            self.as_slice() == other
        }
    }

    #[inline]
    pub fn hash64(&self) -> u64 {
        if self.raw[15] != HEAP_TAG {
            let w0 = u64::from_le_bytes(self.raw[0..8].try_into().unwrap());
            let w1 = u64::from_le_bytes(self.raw[8..16].try_into().unwrap());
            let mut h =
                w0.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ w1.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            h ^= h >> 30;
            h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            h ^= h >> 27;
            h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
            h ^ (h >> 31)
        } else {
            hash64_bytes(self.as_slice())
        }
    }
}

impl Clone for SmallBytes {
    #[inline]
    fn clone(&self) -> Self {
        if self.raw[15] != HEAP_TAG {
            Self { raw: self.raw }
        } else {
            Self::from_slice_heap(self.as_slice())
        }
    }
}

impl Drop for SmallBytes {
    #[inline]
    fn drop(&mut self) {
        if self.raw[15] == HEAP_TAG {
            unsafe { self.drop_heap() }
        }
    }
}

impl SmallBytes {
    #[cold]
    unsafe fn drop_heap(&mut self) {
        let ptr = usize::from_ne_bytes(self.raw[0..8].try_into().unwrap()) as *mut u8;
        let len = u32::from_ne_bytes(self.raw[8..12].try_into().unwrap()) as usize;
        unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)));
        }
    }
}

impl PartialEq for SmallBytes {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        if (self.raw[15] | other.raw[15]) < 16 {
            u128::from_ne_bytes(self.raw) == u128::from_ne_bytes(other.raw)
        } else {
            self.as_slice() == other.as_slice()
        }
    }
}

impl Eq for SmallBytes {}

impl PartialOrd for SmallBytes {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SmallBytes {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl fmt::Debug for SmallBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", String::from_utf8_lossy(self.as_slice()))
    }
}

unsafe impl Send for SmallBytes {}
unsafe impl Sync for SmallBytes {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_and_heap_roundtrip_hash_and_ord() {
        for len in [0usize, 1, 2, 8, 9, 14, 15, 16, 17, 64, 250] {
            let data: Vec<u8> = (0..len).map(|i| b'a' + (i % 26) as u8).collect();
            let sb = SmallBytes::from_slice(&data);
            let cloned = sb.clone();
            assert_eq!(sb.as_slice(), data.as_slice());
            assert_eq!(sb, cloned);
            assert!(sb.eq_slice(&data));
            assert_eq!(sb.hash64(), hash64_bytes(&data));
        }
        let a = SmallBytes::from_slice(b"U1");
        let b = SmallBytes::from_slice(b"u1");
        assert!(a < b);
    }
}
