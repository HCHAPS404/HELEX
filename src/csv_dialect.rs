#[inline]
pub fn is_valid_utf8(bytes: &[u8]) -> bool {
    if bytes.is_ascii() {
        return true;
    }
    std::str::from_utf8(bytes).is_ok()
}

#[inline]
pub fn unquote_field(raw: &[u8]) -> Option<&[u8]> {
    if raw.is_empty() {
        return Some(&[]);
    }
    if raw[0] == b'"' {
        if raw.len() < 2 || raw[raw.len() - 1] != b'"' {
            return None;
        }
        let inner = &raw[1..raw.len() - 1];
        if memchr::memchr(b'"', inner).is_some() || !is_valid_utf8(inner) {
            return None;
        }
        Some(inner)
    } else {
        if memchr::memchr(b'"', raw).is_some() || !is_valid_utf8(raw) {
            return None;
        }
        Some(raw)
    }
}

#[inline]
pub fn parse_fixed_fields<'a, const N: usize>(mut line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    if line.last() == Some(&b'\n') {
        line = &line[..line.len() - 1];
    }
    if line.is_empty() {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0usize;
    for slot in out.iter_mut().take(N - 1) {
        let rel = memchr::memchr(b',', &line[start..])?;
        *slot = unquote_field(&line[start..start + rel])?;
        start += rel + 1;
    }
    let tail = &line[start..];
    if memchr::memchr(b',', tail).is_some() {
        return None;
    }
    out[N - 1] = unquote_field(tail)?;
    Some(out)
}

/// Fast path for lines in an ASCII-verified chunk with no `"` anywhere:
/// fields are returned raw, with only comma-count validation.
#[inline]
pub fn split_fields_unquoted<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0usize;
    for slot in out.iter_mut().take(N - 1) {
        let rel = memchr::memchr(b',', &line[start..])?;
        *slot = &line[start..start + rel];
        start += rel + 1;
    }
    let tail = &line[start..];
    if memchr::memchr(b',', tail).is_some() {
        return None;
    }
    out[N - 1] = tail;
    Some(out)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn comma_quote_masks_avx2(line: &[u8]) -> (u128, u128) {
    use std::arch::x86_64::*;
    let mut commas: u128 = 0;
    let mut quotes: u128 = 0;
    let c_comma = _mm256_set1_epi8(b',' as i8);
    let c_quote = _mm256_set1_epi8(b'"' as i8);
    let mut i = 0usize;
    while i + 32 <= line.len() {
        let v = _mm256_loadu_si256(line.as_ptr().add(i) as *const __m256i);
        let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c_comma)) as u32;
        let qm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c_quote)) as u32;
        commas |= (cm as u128) << i;
        quotes |= (qm as u128) << i;
        i += 32;
    }
    if i < line.len() {
        let base = line.len() - 32;
        let v = _mm256_loadu_si256(line.as_ptr().add(base) as *const __m256i);
        let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c_comma)) as u32;
        let qm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c_quote)) as u32;
        commas |= (cm as u128) << base;
        quotes |= (qm as u128) << base;
    }
    (commas, quotes)
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn has_avx2() -> bool {
    static FLAG: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(u8::MAX);
    match FLAG.load(std::sync::atomic::Ordering::Relaxed) {
        u8::MAX => {
            let v = std::arch::is_x86_feature_detected!("avx2") as u8;
            FLAG.store(v, std::sync::atomic::Ordering::Relaxed);
            v == 1
        }
        v => v == 1,
    }
}

/// AVX2 fast path for lines up to 127 bytes: build comma/quote bitmasks in
/// 32-byte SIMD blocks, then validate structure with pure integer ops.
#[cfg(target_arch = "x86_64")]
#[inline]
fn split_fields_avx2<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    debug_assert!(line.len() <= 127);
    let (mut commas, quotes) = unsafe { comma_quote_masks_avx2(line) };
    if commas.count_ones() as usize != N - 1 {
        return None;
    }
    if quotes != 0 {
        // Every quote must sit at a field boundary: adjacent to a comma on at
        // least one side, or at the line start/end.
        let left_ok = (commas << 1) | 1u128;
        let right_ok = (commas >> 1) | (1u128 << (line.len() - 1));
        if quotes & !(left_ok | right_ok) != 0 {
            return None;
        }
    }
    let mut out = [&[][..]; N];
    let mut start = 0usize;
    for slot in out.iter_mut().take(N - 1) {
        let p = commas.trailing_zeros() as usize;
        *slot = &line[start..p];
        start = p + 1;
        commas &= commas - 1;
    }
    out[N - 1] = &line[start..];
    for f in out.iter_mut() {
        let lead = f.first() == Some(&b'"');
        let tail = f.last() == Some(&b'"');
        if lead != tail || (lead && f.len() < 2) {
            return None;
        }
        if lead {
            *f = &f[1..f.len() - 1];
        }
    }
    Some(out)
}

/// Unified scanner for ASCII-verified chunks: values never contain commas, so
/// fields split on `,`; every `"` must sit exactly at a field boundary, which
/// we verify by scanning quote *positions* once (usually 0-4 per line) instead
/// of re-scanning every field. Strips whole-field quotes; rejects misplaced or
/// unbalanced quotes exactly like the strict dialect path.
#[inline]
pub fn split_fields_csv<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(target_arch = "x86_64")]
    if line.len() >= 32 && line.len() <= 127 && has_avx2() {
        return split_fields_avx2::<N>(line);
    }
    let mut out = split_fields_unquoted::<N>(line)?;
    if memchr::memchr(b'"', line).is_none() {
        return Some(out);
    }
    let mut q = 0usize;
    while let Some(rel) = memchr::memchr(b'"', &line[q..]) {
        let p = q + rel;
        let open = p == 0 || line[p - 1] == b',';
        let close = p + 1 == line.len() || line[p + 1] == b',';
        if !open && !close {
            return None;
        }
        q = p + 1;
    }
    for f in out.iter_mut() {
        let lead = f.first() == Some(&b'"');
        let tail = f.last() == Some(&b'"');
        if lead != tail || (lead && f.len() < 2) {
            return None;
        }
        if lead {
            *f = &f[1..f.len() - 1];
        }
    }
    Some(out)
}

#[inline]
pub fn parse_price_cents(raw: &[u8]) -> Option<u32> {
    if raw.is_empty() {
        return None;
    }
    let mut acc: u64 = 0;
    for &b in raw {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        acc = acc * 10 + u64::from(d);
        if acc > 10_000_000 {
            return None;
        }
    }
    if acc == 0 {
        return None;
    }
    Some(acc as u32)
}

pub fn iter_data_lines(bytes: &[u8], mut f: impl FnMut(&[u8])) {
    let Some(first_nl) = memchr::memchr(b'\n', bytes) else {
        return;
    };
    let mut pos = first_nl + 1;
    while pos < bytes.len() {
        match memchr::memchr(b'\n', &bytes[pos..]) {
            Some(rel) => {
                f(&bytes[pos..pos + rel]);
                pos += rel + 1;
            }
            None => {
                f(&bytes[pos..]);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_dialect_traps() {
        assert_eq!(parse_fixed_fields::<2>(b"\"u1\",\"u2\"\n"), Some([&b"u1"[..], &b"u2"[..]]));
        assert_eq!(parse_fixed_fields::<2>(b"\"\",u2\n"), Some([&b""[..], &b"u2"[..]]));
        assert_eq!(parse_fixed_fields::<2>(b" u1,\" u1\"\n"), Some([&b" u1"[..], &b" u1"[..]]));
        assert_eq!(parse_fixed_fields::<2>(b"u1\r,u2\r\n"), Some([&b"u1\r"[..], &b"u2\r"[..]]));
        assert_eq!(parse_fixed_fields::<2>(b"\n"), None);
        assert_eq!(parse_fixed_fields::<2>(b""), None);
        assert_eq!(parse_fixed_fields::<2>(b"u1,u2,u3\n"), None);
        assert_eq!(parse_fixed_fields::<2>(b"u1\n"), None);
        assert_eq!(parse_fixed_fields::<2>(b"\"u1,u2\n"), None);
        assert_eq!(parse_fixed_fields::<2>(b"u\"1,u2\n"), None);
        assert_eq!(parse_fixed_fields::<2>(b"\"u\"\"1\",u2\n"), None);
    }

    #[test]
    fn price_cents_traps() {
        assert_eq!(parse_price_cents(b"1"), Some(1));
        assert_eq!(parse_price_cents(b"0250"), Some(250));
        assert_eq!(parse_price_cents(b"10000000"), Some(10_000_000));
        assert_eq!(parse_price_cents(b"0"), None);
        assert_eq!(parse_price_cents(b"00"), None);
        assert_eq!(parse_price_cents(b"10000001"), None);
        assert_eq!(parse_price_cents(b"1000\r"), None);
        assert_eq!(parse_price_cents(b"+500"), None);
        assert_eq!(parse_price_cents(b"-5"), None);
        assert_eq!(parse_price_cents(b"19.99"), None);
        assert_eq!(parse_price_cents("٢٥٠".as_bytes()), None);
        assert_eq!(parse_price_cents("２５０".as_bytes()), None);
    }
}
