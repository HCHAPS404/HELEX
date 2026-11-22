//! Exact u64 keys for short ids, and hashed keys (with a byte registry) for
//! the rest.
//!
//! Key spaces (never overlap within one id domain):
//! * tag form   `prefix + payload` with 1..=8 payload bytes, payload[0] < 0x7F,
//!   no NUL: `be(payload) | 1<<63` -> top byte in 0x80..=0xFE.
//! * plain form 1..=8 bytes, b0 < 0x80, no NUL: `be(bytes)` -> bit 63 clear,
//!   never 0.
//! * slow form  anything else: `0xFF<<56 | hash56(bytes)`; bytes live in a
//!   registry so they can be recovered for output.
//!
//! Key 0 is never produced, so it is free as an "empty / invalid" marker.

use crate::sso::hash64_bytes;

pub const TAG: u64 = 1 << 63;
pub const SLOW_TOP: u64 = 0xFF << 56;
const LOW56: u64 = (1 << 56) - 1;

#[inline(always)]
fn load_be_padded(f: &[u8]) -> u64 {
    debug_assert!(f.len() <= 8);
    let mut b = [0u8; 8];
    b[..f.len()].copy_from_slice(f);
    u64::from_be_bytes(b)
}

#[inline(always)]
fn has_zero_byte(f: &[u8]) -> bool {
    // f.len() <= 8: SWAR zero-byte test on the padded word (padding bytes
    // are excluded by masking them to 0xFF).
    let n = f.len();
    let mut b = [0xFFu8; 8];
    b[..n].copy_from_slice(f);
    let v = u64::from_le_bytes(b);
    (v.wrapping_sub(0x0101_0101_0101_0101) & !v & 0x8080_8080_8080_8080) != 0
}

/// Returns (key, is_slow).
#[inline(always)]
pub fn id_key(f: &[u8], prefix: u8) -> (u64, bool) {
    let n = f.len();
    if n >= 2 && n <= 9 && f[0] == prefix && f[1] < 0x7F && !has_zero_byte(&f[1..]) {
        (load_be_padded(&f[1..]) | TAG, false)
    } else if n >= 1 && n <= 8 && f[0] < 0x80 && !has_zero_byte(f) {
        (load_be_padded(f), false)
    } else {
        (slow_key(f), true)
    }
}

/// Little-endian load of the first `n` (<= 8) bytes at `p`, upper bytes zero.
///
/// # Safety
/// `p..p+8` must be readable.
#[inline(always)]
unsafe fn load_le_n(p: *const u8, n: usize) -> u64 {
    let v = std::ptr::read_unaligned(p as *const u64);
    if n >= 8 {
        v
    } else {
        v & ((1u64 << (n * 8)) - 1)
    }
}

#[inline(always)]
fn zero_in_low_n(v: u64, n: usize) -> bool {
    // fill bytes >= n with 0xFF so only the first n bytes can be zero
    let fill = if n >= 8 { 0 } else { !0u64 << (n * 8) };
    let w = v | fill;
    (w.wrapping_sub(0x0101_0101_0101_0101) & !w & 0x8080_8080_8080_8080) != 0
}

/// Same result as [`id_key`], using wide unaligned loads.
///
/// # Safety
/// At least 8 bytes past `f.as_ptr() + 1` must be readable (padded buffer).
#[inline(always)]
pub unsafe fn id_key_padded(f: &[u8], prefix: u8) -> (u64, bool) {
    let n = f.len();
    let p = f.as_ptr();
    if n >= 2 && n <= 9 && *p == prefix && *p.add(1) < 0x7F {
        let v = load_le_n(p.add(1), n - 1);
        if !zero_in_low_n(v, n - 1) {
            return (v.swap_bytes() | TAG, false);
        }
    }
    if n >= 1 && n <= 8 && *p < 0x80 {
        let v = load_le_n(p, n);
        if !zero_in_low_n(v, n) {
            return (v.swap_bytes(), false);
        }
    }
    (slow_key(f), true)
}

#[inline]
pub fn slow_key(f: &[u8]) -> u64 {
    SLOW_TOP | (hash64_bytes(f) & LOW56)
}

#[inline(always)]
pub fn is_slow(k: u64) -> bool {
    k >> 56 == 0xFF
}

/// Decodes a fast key back to its bytes. Slow keys must be looked up in a
/// registry by the caller.
#[inline]
pub fn fast_key_bytes(key: u64, prefix: u8, out: &mut [u8; 9]) -> usize {
    debug_assert!(!is_slow(key));
    let b = (key & !TAG).to_be_bytes();
    let n = 8 - (((key & !TAG).trailing_zeros() / 8) as usize).min(8);
    if key & TAG != 0 {
        out[0] = prefix;
        out[1..1 + n].copy_from_slice(&b[..n]);
        1 + n
    } else {
        out[..n].copy_from_slice(&b[..n]);
        n
    }
}

#[inline(always)]
pub fn mix64(x: u64) -> u64 {
    let h = (x ^ (x >> 31)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^ (h >> 29)
}

/// Shard (top `bits` of mix64). Table slots inside a shard use low bits.
#[inline(always)]
pub fn shard_of(k: u64, bits: u32) -> usize {
    (mix64(k) >> (64 - bits)) as usize
}

/// Open-addressing u64 -> u32 map. Key 0 marks an empty slot.
pub struct KeyMap {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
    len: usize,
}

impl KeyMap {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        Self {
            keys: vec![0; cap],
            vals: vec![0; cap],
            mask: cap - 1,
            len: 0,
        }
    }

    pub fn clear_for(&mut self, n: usize) {
        let cap = (n * 2).next_power_of_two().max(16);
        if cap > self.keys.len() || cap * 8 < self.keys.len() {
            self.keys = vec![0; cap];
            self.vals = vec![0; cap];
        } else {
            self.keys.fill(0);
        }
        self.mask = self.keys.len() - 1;
        self.len = 0;
    }

    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (mix64(k) as usize) & self.mask
    }

    #[inline]
    pub fn get(&self, k: u64) -> Option<u32> {
        let mut i = self.slot(k);
        loop {
            let kk = unsafe { *self.keys.get_unchecked(i) };
            if kk == k {
                return Some(unsafe { *self.vals.get_unchecked(i) });
            }
            if kk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Returns (value, inserted).
    #[inline]
    pub fn get_or_insert(&mut self, k: u64, v: u32) -> (u32, bool) {
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut i = self.slot(k);
        loop {
            let kk = unsafe { *self.keys.get_unchecked(i) };
            if kk == k {
                return (unsafe { *self.vals.get_unchecked(i) }, false);
            }
            if kk == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                self.len += 1;
                return (v, true);
            }
            i = (i + 1) & self.mask;
        }
    }

    pub fn insert(&mut self, k: u64, v: u32) {
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut i = self.slot(k);
        loop {
            let kk = self.keys[i];
            if kk == k {
                self.vals[i] = v;
                return;
            }
            if kk == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                self.len += 1;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let old_k = std::mem::take(&mut self.keys);
        let old_v = std::mem::take(&mut self.vals);
        let cap = (old_k.len() * 2).max(16);
        self.keys = vec![0; cap];
        self.vals = vec![0; cap];
        self.mask = cap - 1;
        for (k, v) in old_k.into_iter().zip(old_v) {
            if k != 0 {
                let mut i = self.slot(k);
                while self.keys[i] != 0 {
                    i = (i + 1) & self.mask;
                }
                self.keys[i] = k;
                self.vals[i] = v;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(s: &[u8], prefix: u8) {
        let (k, slow) = id_key(s, prefix);
        assert_ne!(k, 0);
        if slow {
            assert!(is_slow(k));
        } else {
            assert!(!is_slow(k), "{:?}", String::from_utf8_lossy(s));
            let mut b = [0u8; 9];
            let n = fast_key_bytes(k, prefix, &mut b);
            assert_eq!(&b[..n], s);
        }
    }

    #[test]
    fn key_forms_roundtrip_and_are_disjoint() {
        let cases: Vec<&[u8]> = vec![
            b"u1",
            b"u",
            b"U1",
            b" u1",
            b"u1 ",
            b"u12345678",
            b"u123456789",
            b"abc",
            b"a",
            b"user_id_a",
            b"\x7f",
            b"u\x7f",
            b"u\x7fabc",
            b"u\x00",
            b"\x00",
            b"e1\r",
            "ü".as_bytes(),
            b"12345678",
            b"123456789",
        ];
        let mut seen = std::collections::HashMap::new();
        for c in &cases {
            roundtrip(c, b'u');
            let mut padded = c.to_vec();
            padded.extend_from_slice(&[0xAB; 16]);
            assert_eq!(
                unsafe { id_key_padded(&padded[..c.len()], b'u') },
                id_key(c, b'u')
            );
            let mut padded0 = c.to_vec();
            padded0.extend_from_slice(&[0; 16]);
            assert_eq!(
                unsafe { id_key_padded(&padded0[..c.len()], b'u') },
                id_key(c, b'u')
            );
            let (k, _) = id_key(c, b'u');
            if let Some(prev) = seen.insert(k, c.to_vec()) {
                assert_eq!(prev, c.to_vec());
            }
        }
    }
}
