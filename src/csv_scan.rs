//! Chunk-level structural scan: one SIMD pass builds newline / comma / quote
//! bitmaps for a whole chunk, then lines and their field boundaries are read
//! from the bitmaps. Lines containing a quote fall back to the exact generic
//! splitter (`split_fields_csv`), so results are identical to it.

use crate::csv_dialect::split_fields_csv;

#[derive(Default)]
pub struct Masks {
    nl: Vec<u64>,
    comma: Vec<u64>,
    quote: Vec<u64>,
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn build_avx2(b: &[u8], nl: &mut [u64], comma: &mut [u64], quote: &mut [u64]) {
    use std::arch::x86_64::*;
    let vn = _mm256_set1_epi8(b'\n' as i8);
    let vc = _mm256_set1_epi8(b',' as i8);
    let vq = _mm256_set1_epi8(b'"' as i8);
    let full = b.len() / 64;
    let p = b.as_ptr();
    for w in 0..full {
        let a = _mm256_loadu_si256(p.add(w * 64) as *const __m256i);
        let c = _mm256_loadu_si256(p.add(w * 64 + 32) as *const __m256i);
        let n0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) as u32 as u64;
        let n1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(c, vn)) as u32 as u64;
        let c0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) as u32 as u64;
        let c1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(c, vc)) as u32 as u64;
        let q0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) as u32 as u64;
        let q1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(c, vq)) as u32 as u64;
        *nl.get_unchecked_mut(w) = n0 | (n1 << 32);
        *comma.get_unchecked_mut(w) = c0 | (c1 << 32);
        *quote.get_unchecked_mut(w) = q0 | (q1 << 32);
    }
    build_scalar_from(b, full, nl, comma, quote);
}

fn build_scalar_from(b: &[u8], from_word: usize, nl: &mut [u64], comma: &mut [u64], quote: &mut [u64]) {
    let words = b.len().div_ceil(64);
    for w in from_word..words {
        let (mut n, mut c, mut q) = (0u64, 0u64, 0u64);
        let base = w * 64;
        let end = (base + 64).min(b.len());
        for (i, &x) in b[base..end].iter().enumerate() {
            n |= u64::from(x == b'\n') << i;
            c |= u64::from(x == b',') << i;
            q |= u64::from(x == b'"') << i;
        }
        nl[w] = n;
        comma[w] = c;
        quote[w] = q;
    }
}

#[cfg(target_arch = "x86_64")]
fn has_avx2() -> bool {
    static FLAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FLAG.get_or_init(|| std::arch::is_x86_feature_detected!("avx2"))
}

impl Masks {
    fn build(&mut self, b: &[u8]) {
        let words = b.len().div_ceil(64) + 2;
        if self.nl.len() < words {
            self.nl.resize(words, 0);
            self.comma.resize(words, 0);
            self.quote.resize(words, 0);
        }
        let last = b.len().div_ceil(64);
        for k in last..last + 2 {
            self.nl[k] = 0;
            self.comma[k] = 0;
            self.quote[k] = 0;
        }
        #[cfg(target_arch = "x86_64")]
        if has_avx2() {
            unsafe { build_avx2(b, &mut self.nl, &mut self.comma, &mut self.quote) };
            return;
        }
        build_scalar_from(b, 0, &mut self.nl, &mut self.comma, &mut self.quote);
    }
}

#[inline(always)]
fn bits_in(mask: &[u64], s: usize, e: usize) -> impl Iterator<Item = usize> + '_ {
    // set-bit positions of `mask` within [s, e)
    let (ws, we) = if s >= e { (1, 0) } else { (s >> 6, (e - 1) >> 6) };
    (ws..=we).flat_map(move |w| {
        let mut m = unsafe { *mask.get_unchecked(w) };
        if w == ws {
            m &= !0u64 << (s & 63);
        }
        if w == we && (e & 63) != 0 {
            m &= (1u64 << (e & 63)) - 1;
        }
        std::iter::from_fn(move || {
            if m == 0 {
                None
            } else {
                let t = m.trailing_zeros() as usize;
                m &= m - 1;
                Some((w << 6) + t)
            }
        })
    })
}

#[inline(always)]
fn any_in(mask: &[u64], s: usize, e: usize) -> bool {
    if s >= e {
        return false;
    }
    let ws = s >> 6;
    let we = (e - 1) >> 6;
    let mut w = ws;
    while w <= we {
        let mut m = unsafe { *mask.get_unchecked(w) };
        if w == ws {
            m &= !0u64 << (s & 63);
        }
        if w == we && (e & 63) != 0 {
            m &= (1u64 << (e & 63)) - 1;
        }
        if m != 0 {
            return true;
        }
        w += 1;
    }
    false
}

/// 128 bits of `mask` starting at bit `pos` (bits past the data are zero:
/// `build` leaves a zero word after the last one, and callers mask to the
/// line length).
#[inline(always)]
fn window128(mask: &[u64], pos: usize) -> u128 {
    let w = pos >> 6;
    let sh = (pos & 63) as u32;
    let a = unsafe { *mask.get_unchecked(w) } as u128;
    let b = unsafe { *mask.get_unchecked(w + 1) } as u128;
    let c = if w + 2 < mask.len() {
        unsafe { *mask.get_unchecked(w + 2) as u128 }
    } else {
        0
    };
    let lo = (a | (b << 64)) >> sh;
    if sh == 0 {
        lo
    } else {
        lo | (c << (128 - sh))
    }
}

/// Calls `f(fields)` for every line that starts in `b[..limit]` (after the
/// partial first line unless `first`) and splits into exactly N fields under
/// the `split_fields_csv` dialect. Lines that do not split are skipped, as the
/// generic splitter would return `None` for them.
#[inline(always)]
pub fn for_each_record<const N: usize>(
    b: &[u8],
    first: bool,
    limit: usize,
    masks: &mut Masks,
    mut f: impl FnMut(&[&[u8]; N]),
) {
    masks.build(b);
    let nl = &masks.nl;
    let comma = &masks.comma;
    let quote = &masks.quote;
    let nwords = b.len().div_ceil(64);
    // newline cursor
    let mut w = 0usize;
    let mut cur = if nwords > 0 { nl[0] } else { 0 };
    let mut next_nl = || -> usize {
        loop {
            if cur != 0 {
                let t = cur.trailing_zeros() as usize;
                cur &= cur - 1;
                return (w << 6) + t;
            }
            w += 1;
            if w >= nwords {
                return usize::MAX;
            }
            cur = unsafe { *nl.get_unchecked(w) };
        }
    };
    let mut pos = if first {
        0
    } else {
        match next_nl() {
            usize::MAX => return,
            p => p + 1,
        }
    };
    while pos < limit && pos < b.len() {
        let end = match next_nl() {
            usize::MAX => b.len(),
            p => p,
        };
        let len = end - pos;
        let line = unsafe { b.get_unchecked(pos..end) };
        if len < 128 {
            let lmask: u128 = (1u128 << len) - 1;
            let q = window128(quote, pos) & lmask;
            if q == 0 {
                let mut c = window128(comma, pos) & lmask;
                if c.count_ones() as usize == N - 1 {
                    let mut fields: [&[u8]; N] = [&[]; N];
                    let mut s = 0usize;
                    for fi in fields.iter_mut().take(N - 1) {
                        let p = c.trailing_zeros() as usize;
                        c &= c - 1;
                        *fi = unsafe { line.get_unchecked(s..p) };
                        s = p + 1;
                    }
                    fields[N - 1] = unsafe { line.get_unchecked(s..) };
                    f(&fields);
                }
            } else {
                // Quoted line: quotes can only wrap whole fields (no value
                // contains commas), so the comma mask still gives the field
                // spans. Per field the quote bits must be exactly 0, or just
                // the two boundary bytes which get stripped.
                let mut c = window128(comma, pos) & lmask;
                if c.count_ones() as usize == N - 1 {
                    let mut fields: [&[u8]; N] = [&[]; N];
                    let mut s = 0usize;
                    let mut ok = true;
                    for fi in 0..N {
                        let e = if fi < N - 1 {
                            let p = c.trailing_zeros() as usize;
                            c &= c - 1;
                            p
                        } else {
                            len
                        };
                        let fl = e - s;
                        let qm = (q >> s) & ((1u128 << fl) - 1);
                        if qm == 0 {
                            fields[fi] = unsafe { line.get_unchecked(s..e) };
                        } else if fl >= 2 && qm == (1u128 | (1u128 << (fl - 1))) {
                            fields[fi] = unsafe { line.get_unchecked(s + 1..e - 1) };
                        } else {
                            ok = false;
                            break;
                        }
                        s = e + 1;
                    }
                    if ok {
                        f(&fields);
                    }
                }
            }
        } else if any_in(quote, pos, end) {
            if let Some(fields) = split_fields_csv::<N>(line) {
                f(&fields);
            }
        } else {
            let mut cpos = [0usize; N];
            let mut cnt = 0usize;
            for c in bits_in(comma, pos, end) {
                if cnt >= N - 1 {
                    cnt = N;
                    break;
                }
                cpos[cnt] = c;
                cnt += 1;
            }
            if cnt == N - 1 {
                let mut fields: [&[u8]; N] = [&[]; N];
                let mut s = pos;
                for i in 0..N - 1 {
                    fields[i] = unsafe { b.get_unchecked(s..cpos[i]) };
                    s = cpos[i] + 1;
                }
                fields[N - 1] = unsafe { b.get_unchecked(s..end) };
                f(&fields);
            }
        }
        pos = end + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generic<const N: usize>(b: &[u8], first: bool, limit: usize) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut pos = if first {
            0
        } else {
            match memchr::memchr(b'\n', b) {
                Some(p) => p + 1,
                None => return out,
            }
        };
        while pos < limit && pos < b.len() {
            let end = memchr::memchr(b'\n', &b[pos..]).map_or(b.len(), |r| pos + r);
            if let Some(f) = split_fields_csv::<N>(&b[pos..end]) {
                out.push(f.iter().map(|x| x.to_vec()).collect());
            }
            pos = end + 1;
        }
        out
    }

    #[test]
    fn matches_generic_splitter() {
        let mut text = Vec::new();
        let lines: Vec<&[u8]> = vec![
            b"e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
            b"\"e1\",u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
            b"e1,\"u\"1,ts,ing,view,p1,",
            b"e1,u1,ts,ing,view,p1",
            b"e1,u1,ts,ing,view,p1,,extra",
            b"",
            b"a,b,c,d,e,f,g\r",
            b"\"\",\"\",\"\",\"\",\"\",\"\",\"\"",
            b"x,y,z,w,v,u,t,s,r,q,p,o,n,m,l,k,j,i,h,g,f,e,d,c,b,a,aa,bb,cc,dd,ee,ff,gg,hh",
            b"short,1,2,3,4,5,6",
        ];
        for (i, l) in lines.iter().cycle().take(400).enumerate() {
            text.extend_from_slice(l);
            if i % 7 == 3 {
                text.extend_from_slice(b"padpadpadpadpadpadpadpadpadpadpadpadpadpadpad");
            }
            if i % 11 == 5 {
                // push the line past 128 bytes
                text.extend_from_slice(&[b'z'; 100]);
            }
            text.push(b'\n');
        }
        text.extend_from_slice(b"e9,u9,ts,ing,view,p1,5");
        let mut m = Masks::default();
        for first in [true, false] {
            for limit in [1usize, 50, 63, 64, 65, 127, 128, 1000, text.len()] {
                let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
                for_each_record::<7>(&text, first, limit, &mut m, |f| {
                    got.push(f.iter().map(|x| x.to_vec()).collect())
                });
                assert_eq!(got, generic::<7>(&text, first, limit), "first={first} limit={limit}");
            }
        }
    }
}
